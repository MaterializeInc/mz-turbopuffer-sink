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
    """Direct and hashed encodings must occupy disjoint spaces.

    Both are prefixed so no hashed value can ever equal a raw key: the hash
    namespace is a published constant, so with user-controlled keys an
    unprefixed scheme lets someone craft a long key whose hash equals another
    row's short key and overwrite that document.
    """

    def test_short_string_is_prefixed(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        assert codec.encode({"id": "user-1"}) == "k:user-1"

    def test_long_string_is_hashed_with_distinct_prefix(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        long_key = "x" * 65
        assert codec.encode({"id": long_key}) == "h:" + str(
            uuid.uuid5(ID_NAMESPACE_UUID, long_key)
        )

    def test_hash_of_long_key_cannot_collide_with_a_short_key(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        long_key = "x" * 65
        colliding_short_key = str(uuid.uuid5(ID_NAMESPACE_UUID, long_key))
        assert codec.encode({"id": long_key}) != codec.encode(
            {"id": colliding_short_key}
        )

    def test_boundary_measured_in_bytes_after_prefixing(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        # 62 raw bytes + "k:" == exactly the 64-byte limit
        at_limit = "x" * 62
        assert codec.encode({"id": at_limit}) == "k:" + at_limit
        over = "x" * 63
        assert codec.encode({"id": over}).startswith("h:")

    def test_multibyte_boundary_counts_bytes_not_chars(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        multibyte = "é" * 33  # 66 bytes, 33 chars
        assert codec.encode({"id": multibyte}) == "h:" + str(
            uuid.uuid5(ID_NAMESPACE_UUID, multibyte)
        )

    def test_every_encoded_id_fits_turbopuffer_limit(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        for key in ("a", "x" * 62, "x" * 63, "x" * 500, "é" * 40):
            assert len(codec.encode({"id": key}).encode("utf-8")) <= 64


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
        expected = "h:" + str(
            uuid.uuid5(ID_NAMESPACE_UUID, '{"a":2,"b":"' + "y" * 100 + '"}')
        )
        assert codec.encode(payload) == expected

    def test_composite_direct_form_cannot_collide_with_a_hash(self):
        # canonical JSON always starts with "{", the hash form with "h:"
        schema = record_schema(
            [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]
        )
        codec = IdCodec.from_key_schema(schema)
        assert codec.encode({"a": 1, "b": "x"}).startswith("{")
        assert codec.encode({"a": 1, "b": "y" * 100}).startswith("h:")


class TestUnionUnwrapping:
    def test_nullable_union_is_unwrapped(self):
        # Materialize registers every key column as ["null", <type>]
        schema = record_schema([{"name": "id", "type": ["null", "string"]}])
        codec = IdCodec.from_key_schema(schema)
        assert codec.encode({"id": "k"}) == "k:k"


class TestTurbopufferIdType:
    """The id type is declared explicitly rather than left to inference."""

    def test_integer_key_declares_uint(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "long"}]))
        assert codec.tpuf_id_type == "uint"

    def test_uuid_key_declares_uuid(self):
        schema = record_schema(
            [{"name": "id", "type": {"type": "string", "logicalType": "uuid"}}]
        )
        assert IdCodec.from_key_schema(schema).tpuf_id_type == "uuid"

    def test_string_key_declares_string(self):
        codec = IdCodec.from_key_schema(
            record_schema([{"name": "id", "type": "string"}])
        )
        assert codec.tpuf_id_type == "string"

    def test_composite_key_declares_string(self):
        schema = record_schema(
            [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]
        )
        assert IdCodec.from_key_schema(schema).tpuf_id_type == "string"


class TestSchemaValidation:
    def test_non_record_key_schema_rejected(self):
        with pytest.raises(ValueError, match="record"):
            IdCodec.from_key_schema({"type": "string"})

    def test_empty_field_list_rejected(self):
        with pytest.raises(ValueError, match="field"):
            IdCodec.from_key_schema(record_schema([]))
