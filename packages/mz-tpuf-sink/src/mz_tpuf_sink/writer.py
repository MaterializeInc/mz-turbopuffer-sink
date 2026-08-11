"""Write a transaction's operations to turbopuffer.

One Materialize timestamp normally becomes one atomic write request. A
transaction exceeding the configured size limits is split into sequential
chunks (offsets are only committed after the whole transaction lands, so a
crash mid-transaction replays it idempotently).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Iterable, Iterator

import turbopuffer

from .models import Delete, Op, Patch, Upsert, op_size_bytes

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS = {408, 409, 429}


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, turbopuffer.APIConnectionError):
        return True
    if isinstance(exc, turbopuffer.APIStatusError):
        return exc.status_code in _RETRYABLE_STATUS or exc.status_code >= 500
    return False


class Writer:
    def __init__(
        self,
        client: Any,
        *,
        namespace: str,
        schema: dict[str, Any] | None = None,
        max_rows_per_request: int = 10_000,
        max_bytes_per_request: int = 200 * 1024 * 1024,
        max_attempts: int = 5,
        initial_backoff: float = 0.5,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._client = client
        self._namespace = namespace
        self._schema = schema
        self._max_rows = max_rows_per_request
        self._max_bytes = max_bytes_per_request
        self._max_attempts = max_attempts
        self._initial_backoff = initial_backoff
        self._sleep = sleep

    def write_transaction(self, ops: Iterable[Op]) -> None:
        """Write one transaction, chunking if it exceeds the request limits.

        `ops` may be a generator: chunks are written as they are filled, so a
        transaction carrying embedding vectors never has to be held whole in
        memory.
        """
        written = 0
        for chunk in self._chunk(ops):
            self._write_chunk(chunk)
            written += 1
            if written == 2:
                logger.warning(
                    "transaction split across multiple requests; "
                    "atomicity is per-request"
                )

    def _chunk(self, ops: Iterable[Op]) -> Iterator[list[Op]]:
        current: list[Op] = []
        current_bytes = 0
        for op in ops:
            size = op_size_bytes(op)
            if current and (
                len(current) >= self._max_rows
                or current_bytes + size > self._max_bytes
            ):
                yield current
                current, current_bytes = [], 0
            current.append(op)
            current_bytes += size
        if current:
            yield current

    def _write_chunk(self, ops: list[Op]) -> None:
        request: dict[str, Any] = {}
        upserts = [op.row for op in ops if isinstance(op, Upsert)]
        patches = [{"id": op.id, **op.columns} for op in ops if isinstance(op, Patch)]
        deletes = [op.id for op in ops if isinstance(op, Delete)]
        if upserts:
            request["upsert_rows"] = upserts
        if patches:
            request["patch_rows"] = patches
        if deletes:
            request["deletes"] = deletes
        if self._schema:
            request["schema"] = self._schema

        backoff = self._initial_backoff
        for attempt in range(1, self._max_attempts + 1):
            try:
                self._client.namespace(self._namespace).write(**request)
                return
            except Exception as exc:
                if attempt >= self._max_attempts or not _is_retryable(exc):
                    raise
                logger.warning(
                    "turbopuffer write failed (attempt %d/%d), retrying in %.1fs: %s",
                    attempt,
                    self._max_attempts,
                    backoff,
                    exc,
                )
                self._sleep(backoff)
                backoff *= 2
