"""Build and run a sink from a configuration.

`run_sink` is the library's entry point: it wires the Kafka consumer, Schema
Registry, Materialize frontier watcher, and turbopuffer client together and
runs until stopped. Process-level concerns — logging setup, signal handlers,
reading the environment — belong to the caller.
"""

from __future__ import annotations

import json
import logging
import threading

import psycopg
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from turbopuffer import Turbopuffer

from .buffer import TransactionBuffer
from .config import SinkConfig
from .decoder import Decoder
from .frontier import FrontierWatcher
from .ids import IdCodec
from .schema import row_schema_from_envelope, turbopuffer_schema
from .sink import Sink
from .translate import Translator
from .writer import Writer

logger = logging.getLogger(__name__)


def _build_sink(config: SinkConfig) -> Sink:
    sr_config = {"url": config.schema_registry_url}
    if config.schema_registry_auth:
        sr_config["basic.auth.user.info"] = config.schema_registry_auth
    schema_registry = SchemaRegistryClient(sr_config)

    key_schema = schema_registry.get_latest_version(
        f"{config.kafka_topic}-key"
    ).schema.schema_str
    codec = IdCodec.from_key_schema(json.loads(key_schema))
    logger.info("document ID mode: %s", codec.mode.value)

    # Declare attribute types explicitly rather than letting turbopuffer infer
    # them from the first value (an integral float would be read as int and
    # reject every later fractional value).
    value_schema = schema_registry.get_latest_version(
        f"{config.kafka_topic}-value"
    ).schema.schema_str
    tpuf_schema = turbopuffer_schema(
        row_schema_from_envelope(json.loads(value_schema)),
        id_type=codec.tpuf_id_type,
    )
    logger.info(
        "turbopuffer attribute schema: %s",
        {name: spec["type"] for name, spec in tpuf_schema.items()},
    )

    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": config.kafka_group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            # Materialize writes its sink topic with Kafka transactions.
            # librdkafka already defaults to read_committed (unlike the Java
            # client), but state would be corrupted by aborted records if that
            # ever changed, so pin it.
            "isolation.level": "read_committed",
        }
    )

    # the Writer owns retry policy; disable the SDK's built-in retries so the
    # two layers don't multiply
    tpuf_kwargs = {"api_key": config.turbopuffer_api_key, "max_retries": 0}
    if config.turbopuffer_region:
        tpuf_kwargs["region"] = config.turbopuffer_region
    if config.turbopuffer_base_url:
        tpuf_kwargs["base_url"] = config.turbopuffer_base_url

    database_name, schema_name, sink_name = config.sink_parts
    frontier = FrontierWatcher(
        connect=lambda: psycopg.connect(config.materialize_dsn, autocommit=True),
        database_name=database_name,
        schema_name=schema_name,
        sink_name=sink_name,
        topic=config.kafka_topic,
    )

    return Sink(
        consumer=consumer,
        topic=config.kafka_topic,
        decoder=Decoder(
            key_deserializer=AvroDeserializer(schema_registry),
            value_deserializer=AvroDeserializer(schema_registry),
        ),
        translator=Translator(codec),
        buffer=TransactionBuffer(warn_bytes=config.buffer_warn_bytes),
        writer=Writer(
            Turbopuffer(**tpuf_kwargs),
            namespace=config.namespace,
            schema=tpuf_schema,
            max_rows_per_request=config.max_rows_per_request,
            max_bytes_per_request=config.max_bytes_per_request,
        ),
        frontier=frontier,
        poll_timeout=config.poll_timeout,
    )


def run_sink(config: SinkConfig, stop: threading.Event | None = None) -> None:
    """Sink `config.kafka_topic` into `config.namespace` until stopped.

    Blocks the calling thread. Pass a `threading.Event` and set it from another
    thread — or from a signal handler the caller installs — to shut down after
    the current poll. Startup failures (an unreachable broker, a sink name that
    matches nothing) raise rather than retrying forever.
    """
    sink = _build_sink(config)

    if stop is not None:
        watcher = threading.Thread(
            target=lambda: (stop.wait(), sink.stop()),
            name="sink-stopper",
            daemon=True,
        )
        watcher.start()

    sink.frontier.start()
    try:
        sink.frontier.wait_ready(config.frontier_ready_timeout)
        logger.info(
            "sinking topic %r into turbopuffer namespace %r",
            config.kafka_topic,
            config.namespace,
        )
        sink.run()
    except BaseException:
        sink.frontier.stop()
        raise
