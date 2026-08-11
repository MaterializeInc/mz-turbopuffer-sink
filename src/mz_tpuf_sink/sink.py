"""Main sink loop: poll → decode → buffer → flush complete transactions.

Offsets are committed only after a transaction is written to turbopuffer, so
delivery is at-least-once; replayed writes are idempotent.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from confluent_kafka import TopicPartition

from .buffer import TransactionBuffer
from .decoder import Decoder
from .translate import Translator

logger = logging.getLogger(__name__)


class Sink:
    def __init__(
        self,
        *,
        consumer: Any,
        topic: str,
        decoder: Decoder,
        translator: Translator,
        buffer: TransactionBuffer,
        writer: Any,
        frontier: Any,
        poll_timeout: float = 1.0,
    ):
        self.consumer = consumer
        self.topic = topic
        self.decoder = decoder
        self.translator = translator
        self.buffer = buffer
        self.writer = writer
        self.frontier = frontier
        self.poll_timeout = poll_timeout
        self._offsets: dict[int, dict[int, int]] = {}  # partition -> ts -> last offset
        self._paused: set[int] = set()
        self._stopped = threading.Event()

    # -- rebalance callbacks -------------------------------------------------

    def _on_assign(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        assigned = [tp.partition for tp in partitions]
        logger.info("assigned partitions %s", assigned)
        self.buffer.set_assignment(assigned)

    def _on_revoke(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        # Buffered-but-unwritten data may be redelivered to another consumer;
        # drop everything so a stale flush can never overwrite newer writes.
        logger.info("partitions revoked; clearing buffered state")
        self.buffer.clear()
        self._offsets.clear()
        self._paused.clear()

    # -- one loop iteration ---------------------------------------------------

    def run_iteration(self) -> None:
        self._consume_one()
        self._settle_idle_partitions()
        self._flush_complete()
        self._apply_backpressure()

    def _consume_one(self) -> None:
        msg = self.consumer.poll(self.poll_timeout)
        if msg is None:
            return
        if msg.error():
            raise RuntimeError(f"kafka consumer error: {msg.error()}")
        event = self.decoder.decode(msg)
        if event is None:  # tombstone
            return
        self.buffer.observe(event.partition, event.ts)
        self._offsets.setdefault(event.partition, {})[event.ts] = event.offset
        op = self.translator.translate(event)
        if op is not None:
            self.buffer.add(event.ts, op)

    def _settle_idle_partitions(self) -> None:
        """Apply completeness rule (b): frontier + caught-up-to-watermark."""
        frontier = self.frontier.current()
        if frontier is None or not self.buffer.has_pending():
            return
        for partition in self.buffer.assigned:
            tp = TopicPartition(self.topic, partition)
            _, high = self.consumer.get_watermark_offsets(tp)
            position = self.consumer.position([tp])[0].offset
            if position >= high:
                self.buffer.settle_watermark(partition, frontier)

    def _flush_complete(self) -> None:
        bound = self.buffer.completeness_bound()
        flushable = self.buffer.take_flushable()
        if not flushable:
            return
        for ts, ops in flushable:
            logger.info("writing transaction ts=%d (%d ops)", ts, len(ops))
            self.writer.write_transaction(ops)
        self._commit_offsets(bound)

    def _commit_offsets(self, bound: float) -> None:
        commits = []
        for partition, by_ts in self._offsets.items():
            done = [ts for ts in by_ts if ts < bound]
            if not done:
                continue
            last_offset = max(by_ts.pop(ts) for ts in done)
            commits.append(TopicPartition(self.topic, partition, last_offset + 1))
        if commits:
            self.consumer.commit(commits, asynchronous=False)

    def _apply_backpressure(self) -> None:
        desired = self.buffer.pause_set()
        to_pause = desired - self._paused
        to_resume = self._paused - desired
        if to_pause:
            logger.info("pausing partitions %s (read-ahead limit)", sorted(to_pause))
            self.consumer.pause([TopicPartition(self.topic, p) for p in to_pause])
        if to_resume:
            logger.info("resuming partitions %s", sorted(to_resume))
            self.consumer.resume([TopicPartition(self.topic, p) for p in to_resume])
        self._paused = desired

    # -- lifecycle -------------------------------------------------------------

    def run(self) -> None:
        self.consumer.subscribe(
            [self.topic], on_assign=self._on_assign, on_revoke=self._on_revoke
        )
        try:
            while not self._stopped.is_set():
                self.run_iteration()
        finally:
            self.frontier.stop()
            self.consumer.close()

    def stop(self) -> None:
        self._stopped.set()
