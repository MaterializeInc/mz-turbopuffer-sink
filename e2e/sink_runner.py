"""Run the sink as a subprocess for the end-to-end tests.

This is the library's own entry point wired up the way an application would do
it: build a SinkConfig, install signal handlers, call run_sink. Setting
MZ_TPUF_E2E_EMBEDDING=1 adds a derived vector attribute.

The embedding is a hash rather than a model, so the tests need no external
service and can assert exact values.
"""

from __future__ import annotations

import hashlib
import logging
import os
import signal
import threading

from mz_tpuf_sink import FunctionTransform, SinkConfig, run_sink

DIMS = 2


def fake_embed(text: str | None) -> list[float]:
    digest = hashlib.sha256((text or "").encode("utf-8")).digest()
    return [digest[i] / 255.0 for i in range(DIMS)]


def compute(rows):
    # one call per batch, mirroring how a real embedding API would be used
    return [{"name_embedding": fake_embed(row["name"])} for row in rows]


EMBEDDING = FunctionTransform(
    name="name_embedding",
    sources=("name",),
    schema={"name_embedding": {"type": f"[{DIMS}]f32", "ann": True}},
    compute=compute,
    batch_size=50,
    distance_metric="cosine_distance",
)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = SinkConfig(
        kafka_bootstrap_servers=os.environ["MZ_TPUF_KAFKA_BOOTSTRAP_SERVERS"],
        kafka_topic=os.environ["MZ_TPUF_KAFKA_TOPIC"],
        kafka_group_id=os.environ["MZ_TPUF_KAFKA_GROUP_ID"],
        schema_registry_url=os.environ["MZ_TPUF_SCHEMA_REGISTRY_URL"],
        materialize_dsn=os.environ["MZ_TPUF_MATERIALIZE_DSN"],
        materialize_sink=os.environ["MZ_TPUF_MATERIALIZE_SINK"],
        turbopuffer_api_key=os.environ["MZ_TPUF_TURBOPUFFER_API_KEY"],
        turbopuffer_region=os.environ["MZ_TPUF_TURBOPUFFER_REGION"],
        namespace=os.environ["MZ_TPUF_NAMESPACE"],
    )
    transforms = [EMBEDDING] if os.environ.get("MZ_TPUF_E2E_EMBEDDING") else []

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    run_sink(config, stop=stop, transforms=transforms)


if __name__ == "__main__":
    main()
