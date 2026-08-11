"""Decode Kafka messages from a Materialize Debezium/Avro sink topic.

Deserializers are injected (in production: confluent-kafka AvroDeserializer
backed by Schema Registry) so envelope handling stays testable.
"""

from __future__ import annotations

from typing import Any, Callable

from confluent_kafka.serialization import MessageField, SerializationContext

from .models import ChangeEvent

TIMESTAMP_HEADER = "materialize-timestamp"

Deserializer = Callable[[bytes, SerializationContext], Any]


def _timestamp_from_headers(msg: Any) -> int:
    for name, value in msg.headers() or []:
        if name == TIMESTAMP_HEADER:
            return int(value.decode("ascii"))
    raise ValueError(
        f"message {msg.topic()}[{msg.partition()}]@{msg.offset()} has no "
        f"{TIMESTAMP_HEADER!r} header; is this topic produced by a Materialize sink?"
    )


class Decoder:
    def __init__(self, key_deserializer: Deserializer, value_deserializer: Deserializer):
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer

    def decode(self, msg: Any) -> ChangeEvent | None:
        """Decode one message; returns None for tombstones."""
        value = self._value_deserializer(
            msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
        )
        if value is None:
            return None
        if "before" not in value or "after" not in value:
            raise ValueError(
                f"message value is not a Debezium envelope (fields: {sorted(value)}); "
                "the sink must use ENVELOPE DEBEZIUM"
            )
        key = self._key_deserializer(
            msg.key(), SerializationContext(msg.topic(), MessageField.KEY)
        )
        return ChangeEvent(
            key=key,
            before=value["before"],
            after=value["after"],
            ts=_timestamp_from_headers(msg),
            partition=msg.partition(),
            offset=msg.offset(),
        )
