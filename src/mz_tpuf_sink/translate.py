"""Turn Debezium change events into turbopuffer operations.

Updates are diffed against `before` so only changed columns are patched.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import logging
from decimal import Decimal
from typing import Any

from .ids import IdCodec
from .models import ChangeEvent, Delete, Op, Patch, Upsert

logger = logging.getLogger(__name__)


def to_attr(value: Any) -> Any:
    """Map a decoded Avro value to a turbopuffer attribute value."""
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
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
    def __init__(self, codec: IdCodec):
        self._codec = codec
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
            self._codec.field or "composite key",
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
            return Patch(id=doc_id, columns=changed)

        if event.before is not None:
            return Delete(id=doc_id)

        raise ValueError("change event has neither before nor after")
