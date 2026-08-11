"""End-to-end test: Materialize → Redpanda → sink → real turbopuffer.

Requires Docker and a real turbopuffer API key in e2e/.env. Run with:

    uv run pytest e2e -v -s

Skipped unless MZ_TPUF_TURBOPUFFER_API_KEY is available.
"""

from __future__ import annotations

import re
import subprocess
import time

import pytest
from harness import (
    API_KEY,
    BOOTSTRAP,
    REGION,
    REPO,
    STRONG,
    create_topic,
    mz_exec,
    sink_environment,
    wait_for,
)
from confluent_kafka import TopicPartition
from turbopuffer import Turbopuffer

TOPIC = "products"
PARTITIONS = 3
SINK_NAME = "products_sink"
QUALIFIED_SINK = f"materialize.public.{SINK_NAME}"

pytestmark = pytest.mark.skipif(
    not API_KEY, reason="no turbopuffer API key in e2e/.env"
)


@pytest.fixture(scope="module")
def namespace_name():
    # unique per run so a rerun never inherits state
    return f"mz-tpuf-e2e-{int(time.time())}"


@pytest.fixture(scope="module")
def group_id(namespace_name):
    return f"e2e-{namespace_name}"


@pytest.fixture(scope="module")
def tpuf(namespace_name):
    client = Turbopuffer(api_key=API_KEY, region=REGION)
    ns = client.namespace(namespace_name)
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
        # covers every temporal shape Materialize emits: `time` is an untagged
        # long, `date`/`timestamp` carry logical types, `interval` is a fixed
        "CREATE TABLE products ("
        "  id bigint, name text, price numeric, in_stock boolean,"
        "  updated_at timestamp, opens_at time, launch_day date, warranty interval"
        ")",
    )
    # seed BEFORE the sink exists so these rows arrive as the snapshot
    mz_exec(
        "INSERT INTO products VALUES "
        "(1, 'widget', 9.99, true, '2026-01-01 00:00:00', '09:30:00', "
        "  '2026-03-01', INTERVAL '1 year'), "
        "(2, 'gadget', 24.50, true, '2026-01-02 00:00:00', '10:15:00', "
        "  '2026-03-02', INTERVAL '6 months'), "
        "(3, 'doohickey', 5.00, false, '2026-01-03 00:00:00', '11:45:00', "
        "  '2026-03-03', INTERVAL '30 days')"
    )
    mz_exec(
        f"CREATE SINK {SINK_NAME} FROM products "
        f"INTO KAFKA CONNECTION kafka_conn (TOPIC '{TOPIC}') "
        "KEY (id) NOT ENFORCED "
        "FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn "
        "ENVELOPE DEBEZIUM"
    )


@pytest.fixture(scope="module")
def sink_log(tmp_path_factory):
    return tmp_path_factory.mktemp("sink") / "sink.log"


@pytest.fixture(scope="module")
def sink(materialize, namespace_name, group_id, sink_log):
    env = sink_environment(
        topic=TOPIC,
        group_id=group_id,
        namespace=namespace_name,
        sink=QUALIFIED_SINK,
    )
    with open(sink_log, "w") as log:
        process = subprocess.Popen(
            ["uv", "run", "mz-tpuf-sink", "--log-level", "INFO"],
            cwd=REPO,
            env=env,
            stdout=log,
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
            print(f"\n===== sink log =====\n{sink_log.read_text()}")


@pytest.fixture
def docs(tpuf, sink):
    """Fetch all documents in the namespace, keyed by id."""

    def fetch():
        response = tpuf.query(
            rank_by=("id", "asc"),
            top_k=100,
            include_attributes=True,
            consistency=STRONG,
        )
        return {row.id: row for row in response.rows or []}

    return fetch


# ---------------------------------------------------------------- the scenario


def test_end_to_end(docs, sink, sink_log, tpuf, group_id):
    # -- stage 1: snapshot / multi-row insert lands ------------------------
    state = wait_for(
        lambda: (lambda d: d if len(d) == 3 else None)(docs()),
        "3 seeded documents to appear in turbopuffer",
    )
    assert set(state) == {1, 2, 3}
    widget = state[1]
    assert widget.name == "widget"
    assert widget.in_stock is True
    assert widget.price == 9.99  # numeric → float, scale-39 padding dropped
    assert widget.updated_at.startswith("2026-01-01T00:00:00")  # timestamp → ISO
    # Materialize emits `time` as an untagged long (microseconds since
    # midnight), so it stays an integer rather than becoming a datetime
    assert widget.opens_at == 9 * 3600_000_000 + 30 * 60_000_000
    # `date` does carry a logical type and becomes a real datetime
    assert widget.launch_day.startswith("2026-03-01T00:00:00")
    # `interval` is an Avro fixed(16); base64 of its 16 bytes
    assert isinstance(widget.warranty, str) and len(widget.warranty) == 24
    assert state[1].id == 1 and isinstance(state[1].id, int)  # uint id declared
    print("\n[stage 1] snapshot of 3 rows landed with correct type mapping")

    # -- stage 2: an update is a COLUMN-LEVEL PATCH ------------------------
    # Write an attribute turbopuffer holds but Materialize knows nothing about.
    # A full-row upsert would destroy it; a patch must preserve it.
    tpuf.write(patch_rows=[{"id": 1, "sidecar": "survives-a-patch"}])
    assert docs()[1].sidecar == "survives-a-patch"

    mz_exec("UPDATE products SET price = 11.25 WHERE id = 1")
    updated = wait_for(
        lambda: (lambda d: d[1] if d[1].price == 11.25 else None)(docs()),
        "price update to reach turbopuffer",
    )
    assert updated.sidecar == "survives-a-patch", (
        "sidecar attribute was destroyed: the sink issued a full-row upsert "
        "instead of a column-level patch"
    )
    assert updated.name == "widget"
    assert updated.updated_at.startswith("2026-01-01T00:00:00")
    print("[stage 2] update was a column-level patch (sidecar attribute survived)")

    # -- stage 3: delete removes only its own document ---------------------
    mz_exec("DELETE FROM products WHERE id = 3")
    remaining = wait_for(
        lambda: (lambda d: d if 3 not in d else None)(docs()),
        "deleted document to disappear",
    )
    assert set(remaining) == {1, 2}
    print("[stage 3] delete removed exactly one document")

    # -- stage 4: one Materialize transaction = one atomic write -----------
    # a single statement touching two rows commits at one timestamp, so both
    # documents must be written in one turbopuffer request
    mz_exec("UPDATE products SET in_stock = false WHERE id IN (1, 2)")
    wait_for(
        lambda: (lambda d: d if d[1].in_stock is False and d[2].in_stock is False else None)(docs()),
        "two-row transaction to reach turbopuffer",
    )
    log_text = sink_log.read_text()
    multi_op = re.findall(r"writing transaction ts=\d+ \((\d+) ops\)", log_text)
    assert "2" in multi_op, (
        "expected a single transaction carrying 2 operations, saw op counts: "
        f"{multi_op}"
    )
    print("[stage 4] both rows of one transaction were written in one request")

    # -- stage 5: liveness after idling (frontier settlement) --------------
    # With 3 partitions and 2 live keys, at least one partition is empty; if
    # empty/idle partitions did not settle, this insert would never flush.
    time.sleep(10)
    mz_exec("INSERT INTO products VALUES (4, 'sprocket', 1.00, true, '2026-02-01 00:00:00')")
    wait_for(
        lambda: (lambda d: d if 4 in d else None)(docs()),
        "post-idle insert to land (proves idle-partition settlement)",
        timeout=60,
    )
    print("[stage 5] insert after a quiet period landed: idle partitions settle")

    # -- stage 6: offsets really were committed to Kafka -------------------
    # Consumer.commit's first positional parameter is `message`, not an offset
    # list, so a wrong call silently never commits and every restart replays
    # the whole topic. Ask the group coordinator directly.
    from confluent_kafka import OFFSET_INVALID, Consumer, TopicPartition

    probe = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
    )
    try:
        committed = probe.committed(
            [TopicPartition(TOPIC, p) for p in range(PARTITIONS)], timeout=10
        )
    finally:
        probe.close()
    offsets = {tp.partition: tp.offset for tp in committed}
    assert any(o > 0 for o in offsets.values()), (
        f"no partition has a committed offset ({offsets}); the sink is not "
        "committing progress and would reprocess the topic on every restart"
    )
    assert all(o >= 0 or o == OFFSET_INVALID for o in offsets.values())
    print(f"[stage 6] offsets committed to Kafka: {offsets}")

    assert sink.poll() is None, "sink process died during the test"
