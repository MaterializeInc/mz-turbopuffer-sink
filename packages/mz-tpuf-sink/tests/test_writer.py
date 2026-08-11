import httpx
import pytest
import turbopuffer

from mz_tpuf_sink.models import Delete, Patch, Upsert
from mz_tpuf_sink.writer import Writer


class FakeNamespace:
    """Mimics turbopuffer.lib.namespace.Namespace (client.namespace(name))."""

    def __init__(self, name, sink):
        self._name = name
        self._sink = sink

    def write(self, **kwargs):
        self._sink.requests.append({"namespace": self._name, **kwargs})
        if self._sink.errors:
            raise self._sink.errors.pop(0)


class FakeClient:
    def __init__(self, errors=None):
        self.requests = []
        self.errors = list(errors or [])

    def namespace(self, name):
        return FakeNamespace(name, self)


def status_error(code):
    resp = httpx.Response(code, request=httpx.Request("POST", "http://tpuf"))
    return turbopuffer.APIStatusError(f"http {code}", response=resp, body=None)


def make_writer(client, **kwargs):
    kwargs.setdefault("sleep", lambda _: None)
    return Writer(client, namespace="ns", **kwargs)


class TestRequestShape:
    def test_mixed_ops_become_one_request(self):
        client = FakeClient()
        make_writer(client).write_transaction(
            [
                Upsert(id=1, row={"id": 1, "a": "x"}),
                Patch(id=2, columns={"a": "y"}),
                Delete(id=3),
            ]
        )
        assert client.requests == [
            {
                "namespace": "ns",
                "upsert_rows": [{"id": 1, "a": "x"}],
                "patch_rows": [{"id": 2, "a": "y"}],
                "deletes": [3],
            }
        ]

    def test_empty_op_kinds_omitted(self):
        client = FakeClient()
        make_writer(client).write_transaction([Delete(id=3)])
        assert client.requests == [{"namespace": "ns", "deletes": [3]}]

    def test_empty_transaction_sends_nothing(self):
        client = FakeClient()
        make_writer(client).write_transaction([])
        assert client.requests == []


class TestExplicitSchema:
    """turbopuffer infers attribute types from the first value it sees, which
    misreads an integral float as int; declaring the schema prevents that."""

    SCHEMA = {"price": {"type": "float"}, "name": {"type": "string"}}

    def test_schema_sent_with_every_request(self):
        client = FakeClient()
        writer = make_writer(
            client, schema=self.SCHEMA, max_rows_per_request=1
        )
        writer.write_transaction(
            [Upsert(id=1, row={"id": 1, "price": 5.0}), Upsert(id=2, row={"id": 2, "price": 9.99})]
        )
        assert len(client.requests) == 2
        assert all(r["schema"] == self.SCHEMA for r in client.requests)

    def test_schema_omitted_when_not_configured(self):
        client = FakeClient()
        make_writer(client).write_transaction([Upsert(id=1, row={"id": 1})])
        assert "schema" not in client.requests[0]


class TestChunking:
    def test_splits_by_max_rows(self):
        client = FakeClient()
        ops = [Upsert(id=i, row={"id": i}) for i in range(5)]
        make_writer(client, max_rows_per_request=2).write_transaction(ops)
        sizes = [len(r["upsert_rows"]) for r in client.requests]
        assert sizes == [2, 2, 1]
        written = [row["id"] for r in client.requests for row in r["upsert_rows"]]
        assert written == [0, 1, 2, 3, 4]

    def test_splits_by_max_bytes(self):
        client = FakeClient()
        ops = [Upsert(id=i, row={"id": i, "blob": "x" * 100}) for i in range(4)]
        # each row is ~121 bytes of JSON, so a 150-byte budget fits exactly one
        make_writer(client, max_bytes_per_request=150).write_transaction(ops)
        assert len(client.requests) == 4

    def test_small_transaction_is_single_request(self):
        client = FakeClient()
        ops = [Upsert(id=i, row={"id": i}) for i in range(5)]
        make_writer(client).write_transaction(ops)
        assert len(client.requests) == 1


class TestStreaming:
    """Transformed ops carry embedding vectors, so the writer must never hold a
    whole transaction in memory — it consumes an iterable one chunk at a time."""

    def test_accepts_a_generator(self):
        client = FakeClient()
        ops = (Upsert(id=i, row={"id": i}) for i in range(3))
        make_writer(client).write_transaction(ops)
        assert len(client.requests[0]["upsert_rows"]) == 3

    def test_never_reads_more_than_one_chunk_ahead(self):
        client = FakeClient()
        pulled = []

        def source():
            for i in range(10):
                pulled.append(i)
                yield Upsert(id=i, row={"id": i})

        writer = make_writer(client, max_rows_per_request=2)
        original = writer._write_chunk
        depth = []

        def recording_write_chunk(chunk):
            # how many ops have been pulled at the moment a request is issued
            depth.append(len(pulled))
            return original(chunk)

        writer._write_chunk = recording_write_chunk
        writer.write_transaction(source())

        # with a chunk size of 2, issuing request N must have pulled at most
        # 2*N + 1 ops (the +1 is the lookahead that closes the chunk)
        assert all(d <= 2 * (n + 1) + 1 for n, d in enumerate(depth)), depth
        assert len(client.requests) == 5

    def test_split_warning_fires_once_per_transaction(self, caplog):
        import logging

        client = FakeClient()
        ops = [Upsert(id=i, row={"id": i}) for i in range(5)]
        with caplog.at_level(logging.WARNING):
            make_writer(client, max_rows_per_request=2).write_transaction(ops)
        warnings = [r for r in caplog.records if "split" in r.message]
        assert len(warnings) == 1

    def test_no_split_warning_for_a_single_chunk(self, caplog):
        import logging

        client = FakeClient()
        with caplog.at_level(logging.WARNING):
            make_writer(client).write_transaction([Upsert(id=1, row={"id": 1})])
        assert [r for r in caplog.records if "split" in r.message] == []


class TestRetries:
    def test_retries_429_then_succeeds(self):
        sleeps = []
        client = FakeClient(errors=[status_error(429), status_error(503)])
        writer = Writer(client, namespace="ns", sleep=sleeps.append)
        writer.write_transaction([Delete(id=1)])
        assert len(client.requests) == 3
        assert len(sleeps) == 2
        assert sleeps[1] > sleeps[0]  # exponential backoff

    def test_connection_error_is_retryable(self):
        client = FakeClient(
            errors=[turbopuffer.APIConnectionError(request=httpx.Request("POST", "http://tpuf"))]
        )
        make_writer(client).write_transaction([Delete(id=1)])
        assert len(client.requests) == 2

    def test_client_error_raises_immediately(self):
        client = FakeClient(errors=[status_error(400)])
        with pytest.raises(turbopuffer.APIStatusError):
            make_writer(client).write_transaction([Delete(id=1)])
        assert len(client.requests) == 1

    def test_gives_up_after_max_attempts(self):
        client = FakeClient(errors=[status_error(503)] * 10)
        with pytest.raises(turbopuffer.APIStatusError):
            make_writer(client, max_attempts=3).write_transaction([Delete(id=1)])
        assert len(client.requests) == 3
