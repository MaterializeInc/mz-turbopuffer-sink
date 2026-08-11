"""Buffer change operations by materialize-timestamp until complete.

Completeness: within a partition Materialize emits messages in non-decreasing
timestamp order, so a partition is *settled past F* when either
  (a) it has yielded a message with ts >= F, or
  (b) the consumer caught up to the partition's high watermark after the
      Materialize sink's write frontier reached F (`settle_watermark`).
Every buffered timestamp below the minimum settle point across assigned
partitions is complete and safe to write atomically.

Backpressure: a partition may read the oldest unflushed timestamp plus one
timestamp ahead; on yielding a second distinct timestamp beyond the oldest,
it belongs in `pause_set()` until flushing catches up.
"""

from __future__ import annotations

from .models import Delete, DocId, Op, Patch, Upsert

_NEG_INF = float("-inf")


class TransactionBuffer:
    def __init__(self) -> None:
        self._assigned: set[int] = set()
        self._ops: dict[int, dict[DocId, Op]] = {}
        self._max_seen: dict[int, int] = {}
        self._watermark_bound: dict[int, int] = {}
        self._partition_ts: dict[int, set[int]] = {}

    def clear(self) -> None:
        """Drop all buffered data and settlement state (e.g. on rebalance)."""
        self._ops.clear()
        self._max_seen.clear()
        self._watermark_bound.clear()
        self._partition_ts.clear()

    @property
    def assigned(self) -> set[int]:
        return set(self._assigned)

    def has_pending(self) -> bool:
        return bool(self._ops)

    def set_assignment(self, partitions: list[int]) -> None:
        assigned = set(partitions)
        for state in (self._max_seen, self._watermark_bound, self._partition_ts):
            for p in list(state):
                if p not in assigned:
                    del state[p]
        self._assigned = assigned

    def observe(self, partition: int, ts: int) -> None:
        """Record that a message with this timestamp was read (op or not)."""
        current = self._max_seen.get(partition)
        if current is None or ts > current:
            self._max_seen[partition] = ts
        self._partition_ts.setdefault(partition, set()).add(ts)

    def settle_watermark(self, partition: int, frontier_ts: int) -> None:
        """Partition was caught up to its high watermark after frontier_ts."""
        current = self._watermark_bound.get(partition)
        if current is None or frontier_ts > current:
            self._watermark_bound[partition] = frontier_ts

    def add(self, ts: int, op: Op) -> None:
        by_id = self._ops.setdefault(ts, {})
        prior = by_id.get(op.id)
        if isinstance(op, Patch) and isinstance(prior, Upsert):
            by_id[op.id] = Upsert(id=op.id, row={**prior.row, **op.columns})
        elif isinstance(op, Patch) and isinstance(prior, Patch):
            by_id[op.id] = Patch(id=op.id, columns={**prior.columns, **op.columns})
        else:
            by_id[op.id] = op

    def completeness_bound(self) -> float:
        """Every buffered ts strictly below this bound is complete."""
        if not self._assigned:
            return _NEG_INF
        return min(
            max(
                self._max_seen.get(p, _NEG_INF),
                self._watermark_bound.get(p, _NEG_INF),
            )
            for p in self._assigned
        )

    def take_flushable(self) -> list[tuple[int, list[Op]]]:
        """Remove and return complete timestamps in ascending order."""
        bound = self.completeness_bound()
        flush_ts = sorted(ts for ts in self._ops if ts < bound)
        result = [(ts, list(self._ops.pop(ts).values())) for ts in flush_ts]
        for observed in self._partition_ts.values():
            observed -= {ts for ts in observed if ts < bound}
        return result

    def pause_set(self) -> set[int]:
        """Partitions that have read more than one timestamp past the oldest
        unflushed transaction and should be paused."""
        if not self._ops:
            return set()
        oldest = min(self._ops)
        return {
            p
            for p in self._assigned
            if len({ts for ts in self._partition_ts.get(p, ()) if ts > oldest}) >= 2
        }
