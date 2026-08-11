"""Write a transaction's operations to turbopuffer.

One Materialize timestamp normally becomes one atomic write request. A
transaction exceeding the configured size limits is split into sequential
chunks (offsets are only committed after the whole transaction lands, so a
crash mid-transaction replays it idempotently).
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable

import turbopuffer

from .models import Delete, Op, Patch, Upsert

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS = {408, 409, 429}


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, turbopuffer.APIConnectionError):
        return True
    if isinstance(exc, turbopuffer.APIStatusError):
        return exc.status_code in _RETRYABLE_STATUS or exc.status_code >= 500
    return False


def _op_bytes(op: Op) -> int:
    if isinstance(op, Upsert):
        payload: Any = op.row
    elif isinstance(op, Patch):
        payload = op.columns
    else:
        payload = op.id
    return len(json.dumps(payload, default=str))


class Writer:
    def __init__(
        self,
        client: Any,
        *,
        namespace: str,
        max_rows_per_request: int = 10_000,
        max_bytes_per_request: int = 200 * 1024 * 1024,
        max_attempts: int = 5,
        initial_backoff: float = 0.5,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._client = client
        self._namespace = namespace
        self._max_rows = max_rows_per_request
        self._max_bytes = max_bytes_per_request
        self._max_attempts = max_attempts
        self._initial_backoff = initial_backoff
        self._sleep = sleep

    def write_transaction(self, ops: list[Op]) -> None:
        for chunk in self._chunk(ops):
            self._write_chunk(chunk)

    def _chunk(self, ops: list[Op]) -> list[list[Op]]:
        chunks: list[list[Op]] = []
        current: list[Op] = []
        current_bytes = 0
        for op in ops:
            size = _op_bytes(op)
            if current and (
                len(current) >= self._max_rows
                or current_bytes + size > self._max_bytes
            ):
                chunks.append(current)
                current, current_bytes = [], 0
            current.append(op)
            current_bytes += size
        if current:
            chunks.append(current)
        if len(chunks) > 1:
            logger.warning(
                "transaction split into %d requests; atomicity is per-request",
                len(chunks),
            )
        return chunks

    def _write_chunk(self, ops: list[Op]) -> None:
        request: dict[str, Any] = {"namespace": self._namespace}
        upserts = [op.row for op in ops if isinstance(op, Upsert)]
        patches = [{"id": op.id, **op.columns} for op in ops if isinstance(op, Patch)]
        deletes = [op.id for op in ops if isinstance(op, Delete)]
        if upserts:
            request["upsert_rows"] = upserts
        if patches:
            request["patch_rows"] = patches
        if deletes:
            request["deletes"] = deletes

        backoff = self._initial_backoff
        for attempt in range(1, self._max_attempts + 1):
            try:
                self._client.namespaces.write(**request)
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
