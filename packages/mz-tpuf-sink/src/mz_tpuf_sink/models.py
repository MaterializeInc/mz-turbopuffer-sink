"""Core data types passed between sink stages."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any, Union

DocId = Union[int, str, uuid.UUID]


@dataclass(frozen=True)
class ChangeEvent:
    """A decoded Debezium message from the Materialize sink topic."""

    key: dict[str, Any]
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    ts: int  # materialize-timestamp header (ms)
    partition: int
    offset: int


@dataclass(frozen=True)
class Upsert:
    id: DocId
    row: dict[str, Any]  # full document, includes "id"


@dataclass(frozen=True)
class Patch:
    id: DocId
    columns: dict[str, Any]  # changed columns only, excludes "id"


@dataclass(frozen=True)
class Delete:
    id: DocId


Op = Union[Upsert, Patch, Delete]


def op_size_bytes(op: Op) -> int:
    """Rough JSON payload size of one operation, for chunking and buffering."""
    if isinstance(op, Upsert):
        payload: Any = op.row
    elif isinstance(op, Patch):
        payload = op.columns
    else:
        payload = op.id
    return len(json.dumps(payload, default=str))
