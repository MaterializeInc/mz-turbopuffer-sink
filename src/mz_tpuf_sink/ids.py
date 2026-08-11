"""Derive turbopuffer document IDs from Avro-decoded Kafka keys.

The sink's KEY must be a single column, and its value becomes the document id
verbatim. Nothing is combined, truncated, or hashed: every transformation would
map two distinct keys onto one document, so anything that cannot be represented
is an error instead.

The ID mode is fixed once, from the key schema, so every document in a
namespace uses a single turbopuffer ID type (uint, uuid, or string).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

MAX_STRING_ID_BYTES = 64


class IdMode(Enum):
    U64 = "u64"
    UUID = "uuid"
    STRING = "string"


def _unwrap_nullable(avro_type: Any) -> Any:
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        if len(non_null) == 1:
            return non_null[0]
    return avro_type


@dataclass(frozen=True)
class IdCodec:
    mode: IdMode
    field: str

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
        if len(fields) != 1:
            names = ", ".join(f["name"] for f in fields) or "none"
            raise ValueError(
                f"the sink's KEY must be exactly one column, got {len(fields)} "
                f"({names}); a turbopuffer document id is a single value, so "
                "declare KEY (<one column>) on the sink, adding a single "
                "derived key column to the upstream view if needed"
            )

        name = fields[0]["name"]
        avro_type = _unwrap_nullable(fields[0]["type"])

        if isinstance(avro_type, dict) and avro_type.get("logicalType") == "uuid":
            return cls(IdMode.UUID, name)
        if avro_type in ("int", "long"):
            return cls(IdMode.U64, name)
        if avro_type == "string":
            return cls(IdMode.STRING, name)

        raise ValueError(
            f"key column {name!r} has Avro type {avro_type!r}, which cannot be "
            "a turbopuffer document id; use an integer, string, or uuid column"
        )

    def encode(self, key: dict[str, Any]) -> int | str:
        value = key[self.field]

        if self.mode is IdMode.U64:
            if value < 0:
                raise ValueError(
                    f"key column {self.field!r} is negative ({value}); "
                    "turbopuffer uint IDs must be non-negative"
                )
            return value

        if self.mode is IdMode.UUID:
            return value

        size = len(value.encode("utf-8"))
        if size > MAX_STRING_ID_BYTES:
            raise ValueError(
                f"key column {self.field!r} is {size} bytes, over turbopuffer's "
                f"{MAX_STRING_ID_BYTES} bytes limit for string IDs; shorten the "
                "key upstream (for example, sink a hashed key column from the "
                "Materialize view) so each document id stays distinct"
            )
        return value
