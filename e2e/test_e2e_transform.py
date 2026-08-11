"""End-to-end proof of the transform hook against real Materialize + turbopuffer.

Uses a deterministic hash as the "embedding" so the test needs no external
model and can assert exact values. One scenario pins all three rules at once:
the attribute is produced, it is recomputed when its source changes, and it is
*not* recomputed when an unrelated column changes.
"""

from __future__ import annotations

import subprocess
import time

import pytest
from harness import (
    API_KEY,
    REGION,
    REPO,
    STRONG,
    create_topic,
    mz_exec,
    sink_environment,
    wait_for,
)
from embedding_sink import fake_embed
from turbopuffer import Turbopuffer

TOPIC = "articles"
SINK_NAME = "articles_sink"
QUALIFIED_SINK = f"materialize.public.{SINK_NAME}"

pytestmark = pytest.mark.skipif(
    not API_KEY, reason="no turbopuffer API key in e2e/.env"
)


@pytest.fixture(scope="module")
def namespace_name():
    return f"mz-tpuf-e2e-transform-{int(time.time())}"


@pytest.fixture(scope="module")
def tpuf(namespace_name):
    ns = Turbopuffer(api_key=API_KEY, region=REGION).namespace(namespace_name)
    yield ns
    try:
        ns.delete_all()
        print(f"\n[cleanup] deleted turbopuffer namespace {namespace_name}")
    except Exception as exc:
        print(f"\n[cleanup] could not delete namespace {namespace_name}: {exc}")


@pytest.fixture(scope="module")
def materialize(infra):
    create_topic(TOPIC)
    mz_exec(
        "CREATE TABLE articles (id bigint, name text, price numeric)",
        "INSERT INTO articles VALUES (1, 'alpha', 1.00), (2, 'beta', 2.00)",
        f"CREATE SINK {SINK_NAME} FROM articles "
        f"INTO KAFKA CONNECTION kafka_conn (TOPIC '{TOPIC}') "
        "KEY (id) NOT ENFORCED "
        "FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn "
        "ENVELOPE DEBEZIUM",
    )


@pytest.fixture(scope="module")
def sink(materialize, namespace_name, tmp_path_factory):
    log = tmp_path_factory.mktemp("transform-sink") / "sink.log"
    env = sink_environment(
        topic=TOPIC,
        group_id=f"e2e-{namespace_name}",
        namespace=namespace_name,
        sink=QUALIFIED_SINK,
    )
    with open(log, "w") as handle:
        process = subprocess.Popen(
            ["uv", "run", "python", "e2e/embedding_sink.py"],
            cwd=REPO,
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
        )
        try:
            yield process
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
            print(f"\n===== transform sink log =====\n{log.read_text()}")


@pytest.fixture
def docs(tpuf, sink):
    def fetch():
        response = tpuf.query(
            rank_by=("id", "asc"),
            top_k=100,
            include_attributes=True,
            consistency=STRONG,
        )
        return {row.id: row for row in response.rows or []}

    return fetch


def approx(vector):
    return [pytest.approx(value, abs=1e-6) for value in vector]


SENTINEL = [0.5, 0.25]


def test_transform_recomputes_only_when_its_source_changes(docs, sink, tpuf):
    # -- the derived attribute is produced for the snapshot ----------------
    state = wait_for(
        lambda: (lambda d: d if len(d) == 2 else None)(docs()),
        "seeded documents with embeddings",
    )
    assert state[1].name_embedding == approx(fake_embed("alpha"))
    assert state[2].name_embedding == approx(fake_embed("beta"))
    print("\n[transform 1] embeddings computed for the snapshot")

    # -- an unrelated column changing must NOT recompute it ----------------
    # Overwrite the vector out of band first: recomputing would restore the
    # hash of "alpha", so the sentinel surviving is proof compute() never ran,
    # not merely that the value happened to match. turbopuffer cannot patch a
    # vector, so the sentinel goes in as a full-row upsert.
    tpuf.write(
        upsert_rows=[
            {"id": 1, "name": "alpha", "price": 1.0, "name_embedding": SENTINEL}
        ],
        distance_metric="cosine_distance",
    )
    assert docs()[1].name_embedding == approx(SENTINEL)

    mz_exec("UPDATE articles SET price = 5.00 WHERE id = 1")
    updated = wait_for(
        lambda: (lambda d: d[1] if d[1].price == 5.0 else None)(docs()),
        "price update to reach turbopuffer",
    )
    assert updated.name_embedding == approx(SENTINEL), (
        "the embedding was recomputed even though `name` did not change"
    )
    print("[transform 2] unrelated update left the embedding untouched")

    # -- changing the source column DOES recompute it ----------------------
    mz_exec("UPDATE articles SET name = 'gamma' WHERE id = 1")
    recomputed = wait_for(
        lambda: (lambda d: d[1] if d[1].name == "gamma" else None)(docs()),
        "name update to reach turbopuffer",
    )
    assert recomputed.name_embedding == approx(fake_embed("gamma"))
    assert recomputed.price == 5.0  # the earlier patch is still intact
    print("[transform 3] source update recomputed the embedding")

    # -- the untouched document was never recomputed -----------------------
    assert docs()[2].name_embedding == approx(fake_embed("beta"))
    assert sink.poll() is None, "sink process died during the test"
