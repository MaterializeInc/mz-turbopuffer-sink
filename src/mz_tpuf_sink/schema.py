"""Derive an explicit turbopuffer attribute schema from the Avro row schema.

turbopuffer otherwise infers each attribute's type from the first value it
sees, which misfires in ways that break a sink mid-stream: an integral float
(5.00) is read as `int` and rejects the next fractional value, and a column
whose first row is NULL has nothing to infer from. Declaring the schema up
front — derived from Avro, so still generic over the user's table — removes
that class of failure.

The mapping mirrors `translate.to_attr`: whatever that function produces for a
value must satisfy the type declared here.
"""

from __future__ import annotations

from typing import Any

# turbopuffer attribute types: string, int, uint, float, uuid, datetime, bool,
# and the []T array variants.
_PRIMITIVE_TYPES = {
    "boolean": "bool",
    "int": "int",
    "long": "int",
    "float": "float",
    "double": "float",
    "string": "string",
    "bytes": "string",  # base64-encoded by to_attr
    "enum": "string",
    "fixed": "string",
}

_DATE_TIME_LOGICAL_TYPES = {
    "date",
    "time-millis",
    "time-micros",
    "timestamp-millis",
    "timestamp-micros",
    "local-timestamp-millis",
    "local-timestamp-micros",
}


def _unwrap_nullable(avro_type: Any) -> Any:
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        if len(non_null) == 1:
            return non_null[0]
    return avro_type


def row_schema_from_envelope(envelope: dict) -> dict:
    """Pull the row record out of a Debezium envelope schema.

    Materialize defines the record inline under `before` and refers to it by
    name under `after`, so accept an inline definition from either field.
    """
    fields = {f["name"]: f for f in envelope.get("fields", [])}
    if "before" not in fields or "after" not in fields:
        raise ValueError(
            "value schema is not a Debezium envelope (fields: "
            f"{sorted(fields)}); the sink must use ENVELOPE DEBEZIUM"
        )
    for name in ("before", "after"):
        candidate = _unwrap_nullable(fields[name]["type"])
        if isinstance(candidate, dict) and candidate.get("type") == "record":
            return candidate
    raise ValueError("Debezium envelope has no inline row record definition")


def _attribute_type(avro_type: Any) -> str:
    avro_type = _unwrap_nullable(avro_type)

    if isinstance(avro_type, str):
        return _PRIMITIVE_TYPES.get(avro_type, "string")

    if isinstance(avro_type, dict):
        logical = avro_type.get("logicalType")
        if logical in _DATE_TIME_LOGICAL_TYPES:
            return "datetime"
        if logical == "decimal":
            return "float"  # to_attr converts Decimal to float
        if logical == "uuid":
            return "string"

        kind = avro_type.get("type")
        if kind == "array":
            item = _unwrap_nullable(avro_type.get("items"))
            item_type = _attribute_type(item)
            # arrays of records/maps are JSON-encoded into one string
            if isinstance(item, dict) and item.get("type") in ("record", "map", "array"):
                return "string"
            return f"[]{item_type}"
        if kind in ("record", "map"):
            return "string"  # JSON-encoded by to_attr
        if kind in _PRIMITIVE_TYPES:
            return _PRIMITIVE_TYPES[kind]

    return "string"


def turbopuffer_schema(row_schema: dict) -> dict[str, dict[str, str]]:
    """Map an Avro row record to turbopuffer attribute type declarations.

    `id` is omitted: it is turbopuffer's document key, not an attribute.
    """
    return {
        field["name"]: {"type": _attribute_type(field["type"])}
        for field in row_schema.get("fields", [])
        if field["name"] != "id"
    }
