"""Watch the Materialize sink's write frontier via SUBSCRIBE.

The write frontier F guarantees every future message has ts >= F, which lets
the sink complete timestamps affirmatively even when partitions are idle.
The sink-name lookup is folded into the SUBSCRIBE query itself (one query,
no separate id lookup).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Sink names are unique only within a schema, so the lookup is qualified by
# database and schema; an unqualified name could match sinks in several
# schemas and mix their frontiers together. The topic is joined in as well, so
# a name that points at a different topic than the one being consumed fails at
# startup instead of silently tracking the wrong frontier.
SUBSCRIBE_SQL = """
SUBSCRIBE (
    SELECT f.write_frontier
    FROM mz_internal.mz_frontiers f
    JOIN mz_sinks s ON f.object_id = s.id
    JOIN mz_schemas sc ON s.schema_id = sc.id
    JOIN mz_databases d ON sc.database_id = d.id
    JOIN mz_catalog.mz_kafka_sinks ks ON ks.id = s.id
    WHERE s.name = %(sink_name)s
      AND sc.name = %(schema_name)s
      AND d.name = %(database_name)s
      AND ks.topic = %(topic)s
)
"""


class FrontierState:
    """Applies SUBSCRIBE diffs; the current frontier is the live row's value."""

    def __init__(self) -> None:
        self._counts: dict[int, int] = {}
        self._lock = threading.Lock()

    def apply(self, value: Any, diff: int) -> None:
        frontier = int(value)
        with self._lock:
            count = self._counts.get(frontier, 0) + diff
            if count:
                self._counts[frontier] = count
            else:
                self._counts.pop(frontier, None)

    def current(self) -> int | None:
        with self._lock:
            return max(self._counts) if self._counts else None


class FrontierWatcher:
    def __init__(
        self,
        *,
        connect: Callable[[], Any],
        database_name: str,
        schema_name: str,
        sink_name: str,
        topic: str,
        backoff: Callable[[float], None] = time.sleep,
        backoff_seconds: float = 1.0,
    ):
        self._connect = connect
        self._sink_name = sink_name
        self._topic = topic
        self._schema_name = schema_name
        self._database_name = database_name
        self._backoff = backoff
        self._backoff_seconds = backoff_seconds
        self._state = FrontierState()
        self._stopped = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._conn: Any | None = None

    def current(self) -> int | None:
        return self._state.current()

    def wait_ready(self, timeout: float) -> None:
        """Block until the SUBSCRIBE has produced at least one row.

        A sink that exists always snapshots one row immediately, so a timeout
        here almost always means the configured sink name is wrong.
        """
        if not self._ready.wait(timeout):
            qualified = (
                f"{self._database_name}.{self._schema_name}.{self._sink_name}"
            )
            raise RuntimeError(
                f"no frontier row for sink {qualified} writing to topic "
                f"{self._topic!r} after {timeout}s; check that the sink name, "
                "schema, database, and topic all match an existing sink "
                "(SELECT s.name, ks.topic FROM mz_sinks s "
                "JOIN mz_catalog.mz_kafka_sinks ks ON ks.id = s.id)"
            )

    def run_once(self) -> None:
        """One connect-and-stream cycle; raises on failure (caller retries)."""
        conn = self._connect()
        self._conn = conn
        try:
            params = {
                "sink_name": self._sink_name,
                "schema_name": self._schema_name,
                "database_name": self._database_name,
                "topic": self._topic,
            }
            with conn.cursor() as cur:
                for row in cur.stream(SUBSCRIBE_SQL, params):
                    # row: (mz_timestamp, mz_diff, write_frontier)
                    _, diff, write_frontier = row
                    self._ready.set()
                    if write_frontier is None:
                        # empty frontier: the sink is gone or finished
                        logger.error(
                            "sink %r has an empty write frontier; was it dropped?",
                            self._sink_name,
                        )
                        continue
                    self._state.apply(write_frontier, int(diff))
                    if self._stopped.is_set():
                        return
        finally:
            self._conn = None
            conn.close()

    def _run(self) -> None:
        while not self._stopped.is_set():
            try:
                self.run_once()
            except Exception as exc:
                if self._stopped.is_set():
                    return
                logger.warning(
                    "frontier subscribe failed, reconnecting in %.1fs: %s",
                    self._backoff_seconds,
                    exc,
                )
                self._backoff(self._backoff_seconds)

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="frontier-watcher", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stopped.set()
        conn = self._conn
        if conn is not None:
            try:
                conn.close()  # interrupts a blocked stream()
            except Exception:
                pass
