import pytest

from mz_tpuf_sink.decoder import Decoder
from mz_tpuf_sink.models import ChangeEvent


class FakeMessage:
    def __init__(self, key=b"k", value=b"v", headers=None, partition=0, offset=7):
        self._key = key
        self._value = value
        self._headers = headers
        self._partition = partition
        self._offset = offset

    def key(self):
        return self._key

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def topic(self):
        return "t"


def make_decoder(key_result=None, value_result=None):
    return Decoder(
        key_deserializer=lambda data, ctx: key_result,
        value_deserializer=lambda data, ctx: value_result,
    )


HEADERS = [("materialize-timestamp", b"1723380000000")]


class TestDecode:
    def test_decodes_debezium_envelope(self):
        decoder = make_decoder(
            key_result={"id": 1},
            value_result={"before": None, "after": {"id": 1, "a": "x"}},
        )
        event = decoder.decode(FakeMessage(headers=HEADERS, partition=3, offset=9))
        assert event == ChangeEvent(
            key={"id": 1},
            before=None,
            after={"id": 1, "a": "x"},
            ts=1723380000000,
            partition=3,
            offset=9,
        )

    def test_missing_timestamp_header_is_an_error(self):
        decoder = make_decoder(
            key_result={"id": 1}, value_result={"before": None, "after": {}}
        )
        with pytest.raises(ValueError, match="materialize-timestamp"):
            decoder.decode(FakeMessage(headers=[("other", b"1")]))

    def test_no_headers_at_all_is_an_error(self):
        decoder = make_decoder(
            key_result={"id": 1}, value_result={"before": None, "after": {}}
        )
        with pytest.raises(ValueError, match="materialize-timestamp"):
            decoder.decode(FakeMessage(headers=None))

    def test_tombstone_value_returns_none(self):
        decoder = make_decoder(key_result={"id": 1}, value_result=None)
        assert decoder.decode(FakeMessage(value=None, headers=HEADERS)) is None

    def test_envelope_without_before_after_is_an_error(self):
        decoder = make_decoder(key_result={"id": 1}, value_result={"payload": 1})
        with pytest.raises(ValueError, match="envelope"):
            decoder.decode(FakeMessage(headers=HEADERS))
