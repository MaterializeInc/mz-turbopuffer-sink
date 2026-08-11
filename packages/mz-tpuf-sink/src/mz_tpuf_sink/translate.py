"""Turn Debezium change events into turbopuffer operations.

Updates are diffed against `before` so only changed columns are patched.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import logging
from decimal import Decimal
from typing import Any, Sequence

from .ids import IdCodec
from .models import ChangeEvent, Delete, Op, Patch, Upsert

logger = logging.getLogger(__name__)

# A float64 carries ~15-17 significant decimal digits; beyond that the
# conversion below is genuinely lossy and worth telling the operator about.
_FLOAT_SIGNIFICANT_DIGITS = 17
_warned_decimal_precision = False


def _decimal_to_float(value: Decimal) -> float:
    """Materialize numerics arrive as Avro decimals padded to scale 39.

    Storing them as numbers (rather than text) is what makes turbopuffer range
    filters and sorting work on them.
    """
    global _warned_decimal_precision
    digits = len(value.normalize().as_tuple().digits)
    if digits > _FLOAT_SIGNIFICANT_DIGITS and not _warned_decimal_precision:
        _warned_decimal_precision = True
        logger.warning(
            "decimal %s has %d significant digits and loses precision as a "
            "float; turbopuffer attributes have no exact decimal type",
            value,
            digits,
        )
    return float(value)


def to_attr(value: Any) -> Any:
    """Map a decoded Avro value to a turbopuffer attribute value."""
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return _decimal_to_float(value)
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    if isinstance(value, list):
        if any(isinstance(v, (dict, list)) for v in value):
            return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
        return [to_attr(v) for v in value]
    return value


class Translator:
    def __init__(
        self,
        codec: IdCodec,
        retain_groups: Sequence[frozenset[str]] = (),
    ):
        """`retain_groups` are sets of columns that must travel together.

        A transform deriving one attribute from several columns cannot work
        from a patch that carries only what changed, so when any column of a
        group changes the rest of that group is retained from `after`. Groups
        of one are dropped: that column is already in the patch whenever it
        matters, so the common case costs nothing.
        """
        self._codec = codec
        self._retain_groups = tuple(g for g in retain_groups if len(g) > 1)
        self._warned_id_column = False

    def _warn_dropped_id(self, row: dict[str, Any]) -> None:
        # "id" is turbopuffer's reserved document-ID field; a value column
        # by that name is representable only when it IS the key column
        if self._warned_id_column or "id" not in row or self._codec.field == "id":
            return
        self._warned_id_column = True
        logger.warning(
            'value column "id" is dropped: the document ID comes from the '
            "Kafka key (%s), and turbopuffer reserves the \"id\" field",
            self._codec.field,
        )

    def translate(self, event: ChangeEvent) -> Op | None:
        self._warn_dropped_id(event.after or event.before or {})
        doc_id = self._codec.encode(event.key)

        if event.after is not None and event.before is None:
            row = {"id": doc_id}
            row.update(
                (f, to_attr(v)) for f, v in event.after.items() if f != "id"
            )
            return Upsert(id=doc_id, row=row)

        if event.after is not None and event.before is not None:
            changed = {
                f: to_attr(v)
                for f, v in event.after.items()
                if f != "id" and event.before.get(f) != v
            }
            if not changed:
                return None
            # snapshot before retaining: otherwise retaining a column for one
            # group would trigger any other group containing it, and each
            # spurious trigger is a paid transform call downstream
            triggers = set(changed)
            for group in self._retain_groups:
                if group & triggers:
                    for field in group - triggers:
                        if field in event.after:
                            changed[field] = to_attr(event.after[field])
            return Patch(id=doc_id, columns=changed)

        if event.before is not None:
            return Delete(id=doc_id)

        raise ValueError("change event has neither before nor after")
