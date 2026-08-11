"""Derive turbopuffer document IDs from Avro-decoded Kafka keys.

The ID mode is fixed once, from the key schema, so every document in a
namespace uses a single turbopuffer ID type (u64, UUID, or string).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any

# Namespace for deterministic UUIDv5 hashing of oversized keys.
ID_NAMESPACE_UUID = uuid.uuid5(uuid.NAMESPACE_URL, "mz-tpuf-sink")

_MAX_STRING_ID_BYTES = 64

# Direct and hashed encodings must never overlap. This namespace constant is
# public, so without disjoint prefixes anyone who controls key values could
# craft a >64-byte key whose hash equals another row's short key and silently
# overwrite that document.
_DIRECT_PREFIX = "k:"
_HASH_PREFIX = "h:"


class IdMode(Enum):
    U64 = "u64"
    UUID = "uuid"
    STRING = "string"
    JSON = "json"


def _unwrap_nullable(avro_type: Any) -> Any:
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        if len(non_null) == 1:
            return non_null[0]
    return avro_type


def _hashed(value: str) -> str:
    return _HASH_PREFIX + str(uuid.uuid5(ID_NAMESPACE_UUID, value))


def _string_or_hash(value: str, prefix: str = "") -> str:
    """Encode a string key, hashing it if it would exceed turbopuffer's limit.

    `prefix` separates the direct encoding from the hashed one. Composite keys
    pass no prefix: their canonical JSON always starts with "{", which already
    cannot collide with the "h:" hash form.
    """
    direct = prefix + value
    if len(direct.encode("utf-8")) <= _MAX_STRING_ID_BYTES:
        return direct
    return _hashed(value)


@dataclass(frozen=True)
class IdCodec:
    mode: IdMode
    field: str | None  # None for JSON mode (uses all key fields)

    @property
    def tpuf_id_type(self) -> str:
        """The turbopuffer `id` type to declare for this key encoding."""
        if self.mode is IdMode.U64:
            return "uint"
        if self.mode is IdMode.UUID:
            return "uuid"
        return "string"

    @classmethod
    def from_key_schema(cls, schema: dict) -> "IdCodec":
        if not isinstance(schema, dict) or schema.get("type") != "record":
            raise ValueError(f"key schema must be a record, got: {schema!r}")
        fields = schema.get("fields", [])
        if not fields:
            raise ValueError("key schema has no fields")

        if len(fields) == 1:
            name = fields[0]["name"]
            avro_type = _unwrap_nullable(fields[0]["type"])
            if isinstance(avro_type, dict) and avro_type.get("logicalType") == "uuid":
                return cls(IdMode.UUID, name)
            if avro_type in ("int", "long"):
                return cls(IdMode.U64, name)
            if avro_type == "string":
                return cls(IdMode.STRING, name)

        return cls(IdMode.JSON, None)

    def encode(self, key: dict[str, Any]) -> int | str:
        if self.mode is IdMode.U64:
            value = key[self.field]
            if value < 0:
                raise ValueError(
                    f"key field {self.field!r} is negative ({value}); "
                    "turbopuffer u64 IDs must be non-negative"
                )
            return value
        if self.mode is IdMode.UUID:
            return key[self.field]
        if self.mode is IdMode.STRING:
            return _string_or_hash(key[self.field], _DIRECT_PREFIX)
        canonical = json.dumps(key, sort_keys=True, separators=(",", ":"), default=str)
        return _string_or_hash(canonical)
