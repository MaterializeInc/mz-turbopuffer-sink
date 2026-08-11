"""Entrypoint: wire config, Kafka, Schema Registry, Materialize, turbopuffer."""

from __future__ import annotations

import json
import logging
import signal

import click
import psycopg
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from turbopuffer import Turbopuffer

from .buffer import TransactionBuffer
from .config import Settings
from .decoder import Decoder
from .frontier import FrontierWatcher
from .ids import IdCodec
from .sink import Sink
from .translate import Translator
from .writer import Writer

logger = logging.getLogger(__name__)


def build_sink(settings: Settings) -> Sink:
    sr_config = {"url": settings.schema_registry_url}
    if settings.schema_registry_auth:
        sr_config["basic.auth.user.info"] = settings.schema_registry_auth
    schema_registry = SchemaRegistryClient(sr_config)

    key_schema = schema_registry.get_latest_version(
        f"{settings.kafka_topic}-key"
    ).schema.schema_str
    codec = IdCodec.from_key_schema(json.loads(key_schema))
    logger.info("document ID mode: %s", codec.mode.value)

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )

    tpuf_kwargs = {"api_key": settings.turbopuffer_api_key}
    if settings.turbopuffer_region:
        tpuf_kwargs["region"] = settings.turbopuffer_region
    if settings.turbopuffer_base_url:
        tpuf_kwargs["base_url"] = settings.turbopuffer_base_url

    frontier = FrontierWatcher(
        connect=lambda: psycopg.connect(settings.materialize_dsn, autocommit=True),
        sink_name=settings.materialize_sink,
    )

    return Sink(
        consumer=consumer,
        topic=settings.kafka_topic,
        decoder=Decoder(
            key_deserializer=AvroDeserializer(schema_registry),
            value_deserializer=AvroDeserializer(schema_registry),
        ),
        translator=Translator(codec),
        buffer=TransactionBuffer(),
        writer=Writer(
            Turbopuffer(**tpuf_kwargs),
            namespace=settings.namespace,
            max_rows_per_request=settings.max_rows_per_request,
            max_bytes_per_request=settings.max_bytes_per_request,
        ),
        frontier=frontier,
        poll_timeout=settings.poll_timeout,
    )


@click.command()
@click.option("--log-level", default="INFO", show_default=True)
def main(log_level: str) -> None:
    """Atomically sink a Materialize Kafka topic into turbopuffer.

    Configuration comes from MZ_TPUF_* environment variables or a .env file;
    see the README for the full reference.
    """
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = Settings()
    sink = build_sink(settings)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: sink.stop())

    sink.frontier.start()
    logger.info(
        "sinking topic %r into turbopuffer namespace %r",
        settings.kafka_topic,
        settings.namespace,
    )
    sink.run()


if __name__ == "__main__":
    main()
