import uuid

import pytest

from mz_tpuf_sink.ids import IdCodec


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
    """Keys are stored verbatim. Anything that will not fit is a hard error
    rather than a hash: hashing would put two encodings in one ID space, where
    a short key can equal another key's hash and silently share its document."""

    def test_short_string_used_directly(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        assert codec.encode({"id": "user-1"}) == "user-1"

    def test_key_at_the_64_byte_limit_is_accepted(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        at_limit = "x" * 64
        assert codec.encode({"id": at_limit}) == at_limit

    def test_key_over_64_bytes_is_an_error(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        with pytest.raises(ValueError, match="64 bytes"):
            codec.encode({"id": "x" * 65})

    def test_error_names_the_column_and_actual_size(self):
        codec = IdCodec.from_key_schema(
            record_schema([{"name": "slug", "type": "string"}])
        )
        with pytest.raises(ValueError, match=r"'slug'.*65"):
            codec.encode({"slug": "x" * 65})

    def test_limit_is_measured_in_bytes_not_characters(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        # 33 two-byte characters is only 33 chars but 66 bytes
        with pytest.raises(ValueError, match="66"):
            codec.encode({"id": "é" * 33})

    def test_multibyte_key_within_the_limit_is_accepted(self):
        codec = IdCodec.from_key_schema(record_schema([{"name": "id", "type": "string"}]))
        assert codec.encode({"id": "é" * 32}) == "é" * 32


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


class TestUnionUnwrapping:
    def test_nullable_union_is_unwrapped(self):
        # Materialize registers every key column as ["null", <type>]
        schema = record_schema([{"name": "id", "type": ["null", "string"]}])
        codec = IdCodec.from_key_schema(schema)
        assert codec.encode({"id": "k"}) == "k"


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


class TestSchemaValidation:
    def test_non_record_key_schema_rejected(self):
        with pytest.raises(ValueError, match="record"):
            IdCodec.from_key_schema({"type": "string"})

    def test_empty_field_list_rejected(self):
        with pytest.raises(ValueError, match="exactly one"):
            IdCodec.from_key_schema(record_schema([]))

    def test_composite_key_rejected(self):
        # a turbopuffer document id is one value; combining columns would mean
        # inventing an encoding, so require the sink's KEY to be one column
        schema = record_schema(
            [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]
        )
        with pytest.raises(ValueError, match="exactly one"):
            IdCodec.from_key_schema(schema)

    def test_composite_key_error_names_the_columns(self):
        schema = record_schema(
            [{"name": "tenant", "type": "long"}, {"name": "sku", "type": "string"}]
        )
        with pytest.raises(ValueError, match="tenant, sku"):
            IdCodec.from_key_schema(schema)

    def test_unsupported_single_column_type_rejected(self):
        schema = record_schema([{"name": "id", "type": "boolean"}])
        with pytest.raises(ValueError, match="boolean"):
            IdCodec.from_key_schema(schema)
