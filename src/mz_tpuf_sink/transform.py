"""Derive extra turbopuffer attributes from table columns.

A transform is the sink's equivalent of a Kafka Connect SMT, narrowed to the
job that actually needs doing: computing an attribute from one or more columns
— an embedding, say — and attaching it to the document before it is written.

Transforms run *after* the Debezium diff and after the per-key merge, on the
operations for one Materialize timestamp. That placement is what makes the
central promise cheap to keep: an update that did not touch a transform's
source columns never reaches `compute`, so the embedding is not recomputed.

The framework decides what to recompute and batches the calls; a transform only
declares what it reads, what it produces, and how to compute a batch.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Iterator, Mapping, Protocol, Sequence

from .models import Op, Patch, Upsert

logger = logging.getLogger(__name__)

_VECTOR_TYPE = re.compile(r"^\[(\d+)\](f32|f16|i8)$")
_MAX_VECTOR_ATTRIBUTES = 2


class TransformError(RuntimeError):
    """A transform failed or returned something unusable."""


class Transform(Protocol):
    """Derives turbopuffer attributes from a record's columns."""

    name: str
    sources: tuple[str, ...]
    schema: Mapping[str, Mapping[str, Any]]
    batch_size: int
    # required when the transform produces a vector: turbopuffer rejects any
    # write to a namespace holding one unless the request names a metric
    distance_metric: str | None

    def compute(
        self, rows: Sequence[Mapping[str, Any]]
    ) -> Sequence[Mapping[str, Any]]:
        """Return one mapping of produced attributes per input row.

        Each row holds exactly the declared `sources` plus `"id"`, with values
        already converted to turbopuffer attribute form. Return the attributes
        named in `schema` for every row, in the same order.
        """
        ...


@dataclass(frozen=True)
class FunctionTransform:
    """A Transform built from a plain callable."""

    name: str
    sources: tuple[str, ...]
    schema: Mapping[str, Mapping[str, Any]]
    compute: Callable[[Sequence[Mapping[str, Any]]], Sequence[Mapping[str, Any]]]
    batch_size: int = field(default=256)
    distance_metric: str | None = None


# ---------------------------------------------------------------- validation


def _looks_like_a_transform(candidate: Any) -> bool:
    return all(
        hasattr(candidate, attr)
        for attr in ("name", "sources", "schema", "compute", "batch_size")
    )


def validate_transforms(
    transforms: Sequence[Any],
    *,
    table: Mapping[str, Mapping[str, Any]],
    columns: set[str],
) -> dict[str, Mapping[str, Any]]:
    """Check transforms against the table schema and return the merged schema.

    Everything here fails before the sink connects to Kafka, so a misconfigured
    transform is a startup error rather than a crash on the first record.
    """
    merged: dict[str, Mapping[str, Any]] = dict(table)
    seen_names: set[str] = set()
    all_sources = {s for t in transforms if _looks_like_a_transform(t) for s in t.sources}
    vector_attributes = 0

    for candidate in transforms:
        if not _looks_like_a_transform(candidate):
            raise ValueError(
                f"{candidate!r} is not a Transform: it must have name, sources, "
                "schema, batch_size, and compute()"
            )
        transform: Transform = candidate

        if transform.name in seen_names:
            raise ValueError(f"duplicate transform name {transform.name!r}")
        seen_names.add(transform.name)

        if not transform.sources:
            raise ValueError(
                f"transform {transform.name!r} declares no source columns"
            )
        for source in transform.sources:
            if source == "id":
                raise ValueError(
                    f"transform {transform.name!r} cannot use 'id' as a source: "
                    "the document id comes from the Kafka key, not a value column"
                )
            if source not in columns:
                available = ", ".join(sorted(columns))
                raise ValueError(
                    f"transform {transform.name!r} reads column {source!r}, which "
                    f"the sink topic does not have; available columns: {available}"
                )

        if not transform.schema:
            raise ValueError(
                f"transform {transform.name!r} produces no attributes"
            )
        for attribute, spec in transform.schema.items():
            if attribute == "id":
                raise ValueError(
                    f"transform {transform.name!r} cannot produce 'id': it is the "
                    "document key, derived from the Kafka key"
                )
            if attribute in merged:
                raise ValueError(
                    f"transform {transform.name!r} produces {attribute!r}, which "
                    "already exists as a table column or another transform's output"
                )
            if attribute in all_sources:
                raise ValueError(
                    f"transform {transform.name!r} produces {attribute!r}, which "
                    "another transform reads as a source; transforms run on the "
                    "translated record and cannot be chained"
                )
            spec_type = spec.get("type")
            if not isinstance(spec_type, str):
                raise ValueError(
                    f"transform {transform.name!r} attribute {attribute!r} has no "
                    "string 'type' in its turbopuffer schema"
                )
            is_vector = _VECTOR_TYPE.match(spec_type) is not None
            if spec.get("ann") and not is_vector:
                raise ValueError(
                    f"transform {transform.name!r} attribute {attribute!r} sets "
                    f"ann on non-vector type {spec_type!r}"
                )
            if is_vector:
                vector_attributes += 1
                if not spec.get("ann"):
                    raise ValueError(
                        f"transform {transform.name!r} attribute {attribute!r} is a "
                        f"vector without ann=True, which turbopuffer rejects"
                    )
                if not getattr(transform, "distance_metric", None):
                    raise ValueError(
                        f"transform {transform.name!r} produces vector "
                        f"{attribute!r} but sets no distance_metric; turbopuffer "
                        "requires one on every write to a namespace with a vector"
                    )
            merged[attribute] = spec

    if vector_attributes > _MAX_VECTOR_ATTRIBUTES:
        raise ValueError(
            f"{vector_attributes} vector attributes declared; turbopuffer "
            f"namespaces support at most two vector columns"
        )
    write_distance_metric(transforms)  # rejects conflicting metrics
    return merged


def write_distance_metric(transforms: Sequence[Transform]) -> str | None:
    """The single metric sent with every write, or None without vectors.

    turbopuffer takes one metric per write request, so two vector transforms
    cannot disagree.
    """
    metrics = {
        t.distance_metric
        for t in transforms
        if getattr(t, "distance_metric", None)
        and any(_VECTOR_TYPE.match(str(s.get("type", ""))) for s in t.schema.values())
    }
    if len(metrics) > 1:
        raise ValueError(
            "transforms declare conflicting distance_metric values "
            f"({', '.join(sorted(metrics))}); a turbopuffer write carries one "
            "metric for the whole namespace"
        )
    return metrics.pop() if metrics else None


def produces_a_vector(transform: Transform) -> bool:
    return any(
        _VECTOR_TYPE.match(str(spec.get("type", "")))
        for spec in transform.schema.values()
    )


def vector_source_columns(transforms: Sequence[Transform]) -> frozenset[str]:
    """Columns whose change forces a whole-document upsert.

    turbopuffer cannot patch a vector attribute, so once one of these changes
    the recomputed vector can only be written by replacing the document.
    """
    return frozenset(
        source
        for transform in transforms
        if produces_a_vector(transform)
        for source in transform.sources
    )


def retain_groups(transforms: Sequence[Transform]) -> list[frozenset[str]]:
    """Column groups the translator must keep together on an update.

    Only multi-source transforms need one: a single source is already in the
    patch whenever it changed.
    """
    return [frozenset(t.sources) for t in transforms if len(set(t.sources)) > 1]


# ------------------------------------------------------------------- applying


def _attributes_of(op: Op) -> Mapping[str, Any]:
    return op.row if isinstance(op, Upsert) else op.columns


def _needs(transform: Transform, op: Op) -> bool:
    """Whether this operation must be (re)computed.

    An upsert carries the whole row, so it always computes. A patch computes
    only when *all* of the transform's sources are present — with overlapping
    source sets another transform's retention can pull in one shared column,
    and computing from that partial input would be silently wrong.
    """
    if isinstance(op, Upsert):
        return True
    if not isinstance(op, Patch):
        return False
    return all(source in op.columns for source in transform.sources)


def _as_attribute(value: Any) -> Any:
    if hasattr(value, "tolist"):  # numpy and friends
        return value.tolist()
    return value


def _validate_result(
    transform: Transform,
    result: Mapping[str, Any],
    doc_id: Any,
    dims: Mapping[str, int],
) -> dict[str, Any]:
    expected = set(transform.schema)
    got = set(result)
    if got != expected:
        missing = ", ".join(sorted(expected - got)) or "none"
        unexpected = ", ".join(sorted(got - expected)) or "none"
        raise TransformError(
            f"transform {transform.name!r} returned the wrong attributes for "
            f"document {doc_id!r}: missing {missing}; unexpected {unexpected}"
        )

    validated: dict[str, Any] = {}
    for attribute, value in result.items():
        value = _as_attribute(value)
        expected_dims = dims.get(attribute)
        if expected_dims is not None and value is not None:
            if not isinstance(value, (list, tuple)):
                raise TransformError(
                    f"transform {transform.name!r} returned {type(value).__name__} "
                    f"for vector attribute {attribute!r} on document {doc_id!r}; "
                    "expected a list of numbers"
                )
            if len(value) != expected_dims:
                raise TransformError(
                    f"transform {transform.name!r} returned {len(value)} dimensions "
                    f"for {attribute!r} on document {doc_id!r}; schema declares "
                    f"{expected_dims}"
                )
            value = list(value)
        validated[attribute] = value
    return validated


def _with_attributes(op: Op, attributes: Mapping[str, Any]) -> Op:
    if isinstance(op, Upsert):
        return Upsert(id=op.id, row={**op.row, **attributes})
    return Patch(id=op.id, columns={**op.columns, **attributes})


def _vector_dims(transform: Transform) -> dict[str, int]:
    dims = {}
    for attribute, spec in transform.schema.items():
        match = _VECTOR_TYPE.match(str(spec.get("type", "")))
        if match:
            dims[attribute] = int(match.group(1))
    return dims


def _apply_one(
    transform: Transform, ops: list[Op], dims: Mapping[str, int]
) -> list[Op]:
    """Compute one transform over a window of operations, in place-ish."""
    targets = [i for i, op in enumerate(ops) if _needs(transform, op)]
    if not targets:
        return ops

    rows = [
        {"id": ops[i].id, **{s: _attributes_of(ops[i]).get(s) for s in transform.sources}}
        for i in targets
    ]
    doc_ids = [ops[i].id for i in targets]
    try:
        results = transform.compute(rows)
    except Exception as exc:
        shown = ", ".join(repr(d) for d in doc_ids[:5])
        raise TransformError(
            f"transform {transform.name!r} failed on {len(doc_ids)} document(s) "
            f"({shown}{', ...' if len(doc_ids) > 5 else ''}): {exc}"
        ) from exc

    results = list(results)
    if len(results) != len(rows):
        raise TransformError(
            f"transform {transform.name!r} returned {len(results)} results for "
            f"{len(rows)} rows"
        )

    for index, result in zip(targets, results):
        attributes = _validate_result(transform, result, ops[index].id, dims)
        ops[index] = _with_attributes(ops[index], attributes)
    return ops


def apply_transforms(
    ops: Iterable[Op], transforms: Sequence[Transform]
) -> Iterator[Op]:
    """Stream operations through the transforms, batching `compute` calls.

    Yields window by window rather than materializing the transaction, so the
    vectors held at once are bounded by the largest transform's batch size.
    """
    if not transforms:
        yield from ops
        return

    dims = {t.name: _vector_dims(t) for t in transforms}
    window_size = min(t.batch_size for t in transforms)
    window: list[Op] = []

    for op in ops:
        window.append(op)
        if len(window) >= window_size:
            for transform in transforms:
                _apply_one(transform, window, dims[transform.name])
            yield from window
            window = []

    if window:
        for transform in transforms:
            _apply_one(transform, window, dims[transform.name])
        yield from window
