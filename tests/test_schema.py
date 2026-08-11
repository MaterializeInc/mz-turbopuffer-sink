import pytest

from mz_tpuf_sink.schema import row_schema_from_envelope, turbopuffer_schema

# The exact envelope Materialize registers for
#   CREATE TABLE products (id bigint, name text, price numeric,
#                          in_stock boolean, updated_at timestamp)
# Note: `after` is a *named reference* to the record defined inline in `before`.
REAL_ENVELOPE = {
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "row",
                    "fields": [
                        {"name": "id", "type": ["null", "long"]},
                        {"name": "name", "type": ["null", "string"]},
                        {
                            "name": "price",
                            "type": [
                                "null",
                                {
                                    "type": "bytes",
                                    "precision": 81,
                                    "scale": 39,
                                    "logicalType": "decimal",
                                },
                            ],
                        },
                        {"name": "in_stock", "type": ["null", "boolean"]},
                        {
                            "name": "updated_at",
                            "type": [
                                "null",
                                {"type": "long", "logicalType": "timestamp-micros"},
                            ],
                        },
                    ],
                },
            ],
        },
        {"name": "after", "type": ["null", "row"]},
    ],
}


class TestRowSchemaFromEnvelope:
    def test_extracts_inline_record_from_before(self):
        row = row_schema_from_envelope(REAL_ENVELOPE)
        assert row["name"] == "row"
        assert [f["name"] for f in row["fields"]] == [
            "id",
            "name",
            "price",
            "in_stock",
            "updated_at",
        ]

    def test_extracts_when_only_after_is_inline(self):
        envelope = {
            "type": "record",
            "name": "envelope",
            "fields": [
                {"name": "before", "type": ["null", "row"]},
                {
                    "name": "after",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "row",
                            "fields": [{"name": "id", "type": "long"}],
                        },
                    ],
                },
            ],
        }
        assert row_schema_from_envelope(envelope)["fields"][0]["name"] == "id"

    def test_rejects_non_debezium_schema(self):
        with pytest.raises(ValueError, match="envelope"):
            row_schema_from_envelope(
                {"type": "record", "name": "r", "fields": [{"name": "x", "type": "int"}]}
            )


class TestTurbopufferSchema:
    def test_maps_real_materialize_schema(self):
        schema = turbopuffer_schema(row_schema_from_envelope(REAL_ENVELOPE))
        assert schema == {
            "name": {"type": "string"},
            "price": {"type": "float"},  # decimal → float, matches to_attr
            "in_stock": {"type": "bool"},
            "updated_at": {"type": "datetime"},
        }

    def test_id_is_excluded(self):
        # turbopuffer's id is the document key, never an attribute
        schema = turbopuffer_schema(row_schema_from_envelope(REAL_ENVELOPE))
        assert "id" not in schema

    def test_integer_types(self):
        row = {
            "type": "record",
            "name": "row",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "long"},
            ],
        }
        assert turbopuffer_schema(row) == {
            "a": {"type": "int"},
            "b": {"type": "int"},
        }

    def test_floating_point_types(self):
        row = {
            "type": "record",
            "name": "row",
            "fields": [
                {"name": "a", "type": "float"},
                {"name": "b", "type": "double"},
            ],
        }
        assert turbopuffer_schema(row) == {
            "a": {"type": "float"},
            "b": {"type": "float"},
        }

    def test_date_and_time_logical_types_are_datetime(self):
        row = {
            "type": "record",
            "name": "row",
            "fields": [
                {"name": "d", "type": {"type": "int", "logicalType": "date"}},
                {
                    "name": "ts",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
            ],
        }
        assert turbopuffer_schema(row) == {
            "d": {"type": "datetime"},
            "ts": {"type": "datetime"},
        }

    def test_plain_bytes_is_string(self):
        # to_attr base64-encodes raw bytes
        row = {
            "type": "record",
            "name": "row",
            "fields": [{"name": "blob", "type": "bytes"}],
        }
        assert turbopuffer_schema(row) == {"blob": {"type": "string"}}

    def test_nested_record_is_json_string(self):
        row = {
            "type": "record",
            "name": "row",
            "fields": [
                {
                    "name": "meta",
                    "type": {
                        "type": "record",
                        "name": "inner",
                        "fields": [{"name": "x", "type": "int"}],
                    },
                }
            ],
        }
        assert turbopuffer_schema(row) == {"meta": {"type": "string"}}

    def test_array_of_primitives(self):
        row = {
            "type": "record",
            "name": "row",
            "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "nums", "type": {"type": "array", "items": "long"}},
            ],
        }
        assert turbopuffer_schema(row) == {
            "tags": {"type": "[]string"},
            "nums": {"type": "[]int"},
        }

    def test_array_of_records_is_json_string(self):
        row = {
            "type": "record",
            "name": "row",
            "fields": [
                {
                    "name": "items",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "i",
                            "fields": [{"name": "x", "type": "int"}],
                        },
                    },
                }
            ],
        }
        assert turbopuffer_schema(row) == {"items": {"type": "string"}}

    def test_enum_is_string(self):
        row = {
            "type": "record",
            "name": "row",
            "fields": [
                {"name": "e", "type": {"type": "enum", "name": "e", "symbols": ["A"]}}
            ],
        }
        assert turbopuffer_schema(row) == {"e": {"type": "string"}}

    def test_uuid_logical_type_is_string(self):
        row = {
            "type": "record",
            "name": "row",
            "fields": [
                {"name": "u", "type": {"type": "string", "logicalType": "uuid"}}
            ],
        }
        assert turbopuffer_schema(row) == {"u": {"type": "string"}}

    def test_unknown_type_falls_back_to_string(self):
        row = {
            "type": "record",
            "name": "row",
            "fields": [{"name": "weird", "type": {"type": "map", "values": "int"}}],
        }
        assert turbopuffer_schema(row) == {"weird": {"type": "string"}}
