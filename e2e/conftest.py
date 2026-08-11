"""Session fixtures shared by the end-to-end test modules.

Docker comes up once per session; each test module owns its own Kafka topic,
Materialize objects, and turbopuffer namespace so the scenarios cannot
interfere with one another.
"""

from __future__ import annotations

import psycopg
import pytest
from harness import MZ_DSN, compose, mz_exec, wait_for


@pytest.fixture(scope="session")
def infra():
    compose("up", "-d", "--wait")
    try:
        wait_for(
            lambda: psycopg.connect(MZ_DSN, connect_timeout=3).close() or True,
            "Materialize to accept SQL",
        )
        mz_exec(
            "CREATE CONNECTION kafka_conn TO KAFKA "
            "(BROKER 'redpanda:9092', SECURITY PROTOCOL PLAINTEXT)",
            "CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY "
            "(URL 'http://redpanda:8081')",
        )
        yield
    finally:
        compose("down", "-v", check=False)
