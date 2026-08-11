"""Atomically sink a Materialize Kafka topic into turbopuffer.

Each Materialize timestamp is applied as one atomic turbopuffer write:

    from mz_tpuf_sink import SinkConfig, run_sink

    run_sink(SinkConfig(
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="events",
        schema_registry_url="http://localhost:8081",
        materialize_dsn="postgres://materialize@localhost:6875/materialize",
        materialize_sink="materialize.public.events_sink",
        turbopuffer_api_key="tpuf_...",
        turbopuffer_region="aws-us-east-1",
        namespace="events",
    ))
"""

from .config import SinkConfig
from .runner import run_sink
from .transform import FunctionTransform, Transform, TransformError

__all__ = [
    "SinkConfig",
    "run_sink",
    "Transform",
    "FunctionTransform",
    "TransformError",
]
