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


def _element_chars(vector: list) -> int:
    """JSON width of one element, sampled rather than measured.

    Element width varies by type — ~13 characters for an embedding float, 2 for
    a small integer — so sample instead of assuming. The widest sample wins:
    embedding vectors are uniform, and for a ragged list of integers erring high
    keeps the chunk budget conservative.
    """
    samples = {0, len(vector) // 2, len(vector) - 1}
    return max(len(repr(vector[i])) for i in samples) + 1  # + the separator


def _is_numeric_vector(value: Any) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and isinstance(value[0], (int, float))
        and not isinstance(value[0], bool)
    )


def _payload_size(payload: Any) -> int:
    """Size a write payload without serializing embedding vectors.

    Chunking measures every operation, and json.dumps on a 1536-float vector
    allocates ~29 KB of throwaway string each time. Numeric lists are estimated
    instead; the result is documented as rough and only feeds the chunk budget.
    """
    if not isinstance(payload, dict):
        return len(json.dumps(payload, default=str))

    vectors = {k: v for k, v in payload.items() if _is_numeric_vector(v)}
    if not vectors:
        return len(json.dumps(payload, default=str))

    rest = {k: v for k, v in payload.items() if k not in vectors}
    size = len(json.dumps(rest, default=str)) if rest else 2
    for name, vector in vectors.items():
        size += len(name) + 4 + len(vector) * _element_chars(vector)
    return size


def op_size_bytes(op: Op) -> int:
    """Rough JSON payload size of one operation, for chunking and buffering."""
    if isinstance(op, Upsert):
        payload: Any = op.row
    elif isinstance(op, Patch):
        payload = op.columns
    else:
        payload = op.id
    return _payload_size(payload)
