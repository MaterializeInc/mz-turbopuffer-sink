import uuid

import pytest

from mz_tpuf_sink.ids import ID_NAMESPACE_UUID, IdCodec


def record_schema(fields):
    return {"type": "record", "name": "row", "fields": fields}


class TestSingleIntegerKey:
    def test_long_key_maps_to_u64(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "long"}]))
        assert codec.encode({"id": 42}) == 42

    def test_int_key_maps_to_u64(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "int"}]))
        assert codec.encode({"id": 7}) == 7

    def test_negative_value_is_an_error(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "long"}]))
        with pytest.raises(ValueError, match="negative"):
            codec.encode({"id": -1})


class TestSingleStringKey:
    def test_short_string_used_directly(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        assert codec.encode({"id": "user-1"}) == "user-1"

    def test_string_over_64_bytes_hashed_to_uuid5(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        long_key = "x" * 65
        expected = str(uuid.uuid5(ID_NAMESPACE_UUID, long_key))
        assert codec.encode({"id": long_key}) == expected

    def test_64_byte_boundary_measured_in_bytes_not_chars(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        # 33 two-byte chars = 66 bytes but only 33 chars
        multibyte = "é" * 33
        expected = str(uuid.uuid5(ID_NAMESPACE_UUID, multibyte))
        assert codec.encode({"id": multibyte}) == expected


class TestUuidKey:
    def test_uuid_logical_type_passes_through(self):
        schema = record_schema(
            [{"name": "id", "type": {"type": "string", "logicalType": "uuid"}}]
        )
        codec = IdCodec.from_key_schema(schema)
        value = "9f1c1f6e-0b1a-4a5e-8f3d-2f6a1c9b7d10"
        assert codec.encode({"id": value}) == value

    def test_uuid_instance_passes_through(self):
        # fastavro decodes the uuid logicalType to uuid.UUID instances
        schema = record_schema(
            [{"name": "id", "type": {"type": "string", "logicalType": "uuid"}}]
        )
        codec = IdCodec.from_key_schema(schema)
        value = uuid.UUID("9f1c1f6e-0b1a-4a5e-8f3d-2f6a1c9b7d10")
        assert codec.encode({"id": value}) == value


class TestCompositeKey:
    def test_composite_key_becomes_canonical_json(self):
        schema = record_schema(
            [{"name": "b", "type": "string"}, {"name": "a", "type": "long"}]
        )
        codec = IdCodec.from_key_schema(schema)
        # keys sorted, compact separators -> deterministic regardless of dict order
        assert codec.encode({"b": "x", "a": 1}) == '{"a":1,"b":"x"}'

    def test_composite_json_over_64_bytes_hashed_to_uuid5(self):
        schema = record_schema(
            [{"name": "b", "type": "string"}, {"name": "a", "type": "long"}]
        )
        codec = IdCodec.from_key_schema(schema)
        payload = {"b": "y" * 100, "a": 2}
        expected = str(uuid.uuid5(ID_NAMESPACE_UUID, '{"a":2,"b":"' + "y" * 100 + '"}'))
        assert codec.encode(payload) == expected


class TestUnionUnwrapping:
    def test_nullable_union_is_unwrapped(self):
        schema = record_schema([{"name": "id", "type": ["null", "string"]}])
        codec = IdCodec.from_key_schema(schema)
        assert codec.encode({"id": "k"}) == "k"


class TestSchemaValidation:
    def test_non_record_key_schema_rejected(self):
        with pytest.raises(ValueError, match="record"):
            IdCodec.from_key_schema({"type": "string"})

    def test_empty_field_list_rejected(self):
        with pytest.raises(ValueError, match="field"):
            IdCodec.from_key_schema(record_schema([]))
