"""Shared helpers for the end-to-end tests (fixtures live in conftest.py)."""

from __future__ import annotations

import os
import pathlib
import subprocess
import time
import uuid

import psycopg
from dotenv import load_dotenv

E2E_DIR = pathlib.Path(__file__).parent
REPO = E2E_DIR.parent

load_dotenv(E2E_DIR / ".env")

MZ_DSN = "postgres://materialize@localhost:6875/materialize"
BOOTSTRAP = "localhost:19092"
SCHEMA_REGISTRY = "http://localhost:8081"
PARTITIONS = 3  # exercises multi-partition + idle/empty-partition settlement
STRONG = {"level": "strong"}

API_KEY = os.environ.get("MZ_TPUF_TURBOPUFFER_API_KEY")
REGION = os.environ.get("MZ_TPUF_TURBOPUFFER_REGION", "aws-us-east-1")


def namespace_for(scenario: str) -> str:
    """A namespace name unique to this run.

    Concurrent CI runs would otherwise collide on a second-resolution
    timestamp and write into each other's namespace. MZ_TPUF_E2E_PREFIX lets
    CI tag every namespace it creates so a cancelled run can be cleaned up.
    """
    prefix = os.environ.get("MZ_TPUF_E2E_PREFIX", "local")
    return f"mz-tpuf-e2e-{prefix}-{scenario}-{uuid.uuid4().hex[:8]}"


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=E2E_DIR,
        check=check,
        capture_output=True,
        text=True,
    )


def wait_for(predicate, description: str, timeout: float = 90.0, interval: float = 1.0):
    """Poll until predicate returns a truthy value; raise with context on timeout."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except Exception as exc:  # not ready yet
            last_error = exc
        time.sleep(interval)
    raise AssertionError(
        f"timed out after {timeout}s waiting for {description}"
        + (f" (last error: {last_error})" if last_error else "")
    )


def mz_exec(*statements: str) -> None:
    """Run each statement on its own.

    Materialize rejects UPDATE/DELETE inside an explicit transaction block, so
    every write here is a single auto-committed statement. That is sufficient
    for the atomicity assertions: one statement commits at one Materialize
    timestamp, and a statement touching N rows therefore produces one
    N-operation transaction on the sink topic.
    """
    with psycopg.connect(MZ_DSN, autocommit=True) as conn:
        for statement in statements:
            conn.execute(statement)


def create_topic(name: str) -> None:
    compose("exec", "-T", "redpanda", "rpk", "topic", "create", name, "-p", str(PARTITIONS))


SINK_COMMAND = ["uv", "run", "--no-sync", "python", "e2e/sink_runner.py"]


def sink_environment(
    *, topic: str, group_id: str, namespace: str, sink: str, extra: dict | None = None
) -> dict:
    return {
        **os.environ,
        "MZ_TPUF_KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP,
        "MZ_TPUF_KAFKA_TOPIC": topic,
        "MZ_TPUF_KAFKA_GROUP_ID": group_id,
        "MZ_TPUF_SCHEMA_REGISTRY_URL": SCHEMA_REGISTRY,
        "MZ_TPUF_MATERIALIZE_DSN": MZ_DSN,
        "MZ_TPUF_MATERIALIZE_SINK": sink,
        "MZ_TPUF_TURBOPUFFER_API_KEY": API_KEY or "",
        "MZ_TPUF_TURBOPUFFER_REGION": REGION,
        "MZ_TPUF_NAMESPACE": namespace,
        **(extra or {}),  # last, so a scenario's override wins
    }
