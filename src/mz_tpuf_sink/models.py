"""Core data types passed between sink stages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Union

DocId = Union[int, str]


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
