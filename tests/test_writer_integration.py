"""Writer through the real Turbopuffer SDK client against a mock transport.

Guards the actual HTTP surface (URL routing, request body shape) that
hand-rolled fakes cannot: a wrong SDK call path fails here immediately.
"""

import json

import httpx
import pytest
from turbopuffer import Turbopuffer

from mz_tpuf_sink.models import Delete, Patch, Upsert
from mz_tpuf_sink.writer import Writer


@pytest.fixture
def recorded():
    return []


@pytest.fixture
def client(recorded):
    def handler(request: httpx.Request) -> httpx.Response:
        recorded.append(request)
        return httpx.Response(200, json={"status": "OK", "rows_affected": 1})

    return Turbopuffer(
        api_key="test-key",
        base_url="http://tpuf.test",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )


class TestRealClientSurface:
    def test_transaction_is_one_post_to_namespace_write(self, client, recorded):
        writer = Writer(client, namespace="my-ns", sleep=lambda _: None)
        writer.write_transaction(
            [
                Upsert(id=1, row={"id": 1, "a": "x"}),
                Patch(id=2, columns={"a": "y"}),
                Delete(id=3),
            ]
        )
        assert len(recorded) == 1
        request = recorded[0]
        assert request.method == "POST"
        assert request.url.path == "/v2/namespaces/my-ns"
        body = json.loads(request.content)
        assert body["upsert_rows"] == [{"id": 1, "a": "x"}]
        assert body["patch_rows"] == [{"id": 2, "a": "y"}]
        assert body["deletes"] == [3]

    def test_api_key_sent(self, client, recorded):
        Writer(client, namespace="ns", sleep=lambda _: None).write_transaction(
            [Delete(id=1)]
        )
        assert recorded[0].headers["authorization"] == "Bearer test-key"
