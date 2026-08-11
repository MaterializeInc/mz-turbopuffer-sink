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

SUBSCRIBE_SQL = """
SUBSCRIBE (
    SELECT f.write_frontier
    FROM mz_internal.mz_frontiers f
    JOIN mz_sinks s ON f.object_id = s.id
    WHERE s.name = %(sink_name)s
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
        sink_name: str,
        backoff: Callable[[float], None] = time.sleep,
        backoff_seconds: float = 1.0,
    ):
        self._connect = connect
        self._sink_name = sink_name
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
            raise RuntimeError(
                f"no frontier row for sink {self._sink_name!r} after {timeout}s; "
                "check that the sink name matches mz_sinks.name"
            )

    def run_once(self) -> None:
        """One connect-and-stream cycle; raises on failure (caller retries)."""
        conn = self._connect()
        self._conn = conn
        try:
            with conn.cursor() as cur:
                for row in cur.stream(SUBSCRIBE_SQL, {"sink_name": self._sink_name}):
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
