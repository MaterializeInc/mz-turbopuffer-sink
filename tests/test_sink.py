import pytest
from confluent_kafka import TopicPartition

from mz_tpuf_sink.buffer import TransactionBuffer
from mz_tpuf_sink.ids import IdCodec
from mz_tpuf_sink.models import ChangeEvent
from mz_tpuf_sink.sink import Sink
from mz_tpuf_sink.translate import Translator

TOPIC = "t"


class FakeMessage:
    def __init__(self, event: ChangeEvent):
        self.event = event

    def error(self):
        return None

    def partition(self):
        return self.event.partition

    def offset(self):
        return self.event.offset

    def topic(self):
        return TOPIC


class PassthroughDecoder:
    def decode(self, msg):
        return msg.event


OFFSET_INVALID = -1001


class FakeConsumer:
    def __init__(self, messages, high_watermarks=None, committed_offsets=None):
        self.queue = list(messages)
        self.paused = set()
        self.commits = []
        self.closed = False
        self.high_watermarks = high_watermarks or {}
        self.low_watermarks = {}
        self.committed_offsets = committed_offsets or {}
        self.watermark_calls = 0
        self._positions = {}

    def poll(self, timeout):
        if self.queue:
            msg = self.queue.pop(0)
            if msg.partition() not in self.paused:
                self._positions[msg.partition()] = msg.offset() + 1
                return msg
            self.queue.insert(0, msg)
        return None

    def pause(self, tps):
        self.paused |= {tp.partition for tp in tps}

    def resume(self, tps):
        self.paused -= {tp.partition for tp in tps}

    def commit(self, message=None, offsets=None, asynchronous=True):
        # mirrors confluent_kafka.Consumer.commit(message=None, offsets=None,
        # asynchronous=True): a positionally-passed list lands in `message`
        # and the real client raises TypeError
        if message is not None:
            raise TypeError("expected confluent_kafka.cimpl.Message")
        assert asynchronous is False, "offset commits must be synchronous"
        self.commits.append(list(offsets))

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        self.watermark_calls += 1
        return (
            self.low_watermarks.get(tp.partition, 0),
            self.high_watermarks.get(tp.partition, 0),
        )

    def position(self, tps):
        # honest: OFFSET_INVALID until a message is consumed in this session
        return [
            TopicPartition(
                TOPIC, tp.partition, self._positions.get(tp.partition, OFFSET_INVALID)
            )
            for tp in tps
        ]

    def committed(self, tps, timeout=None):
        return [
            TopicPartition(
                TOPIC,
                tp.partition,
                self.committed_offsets.get(tp.partition, OFFSET_INVALID),
            )
            for tp in tps
        ]

    def close(self):
        self.closed = True


class FakeFrontier:
    def __init__(self, value=None):
        self.value = value

    def current(self):
        return self.value


class RecordingWriter:
    def __init__(self, fail_times=0):
        self.transactions = []
        self._fail_times = fail_times

    def write_transaction(self, ops):
        if self._fail_times:
            self._fail_times -= 1
            raise RuntimeError("tpuf down")
        self.transactions.append(list(ops))


def make_event(ts, offset, partition=0, doc=1, after=True):
    return ChangeEvent(
        key={"id": doc},
        before=None if after else {"id": doc, "v": 1},
        after={"id": doc, "v": ts} if after else None,
        ts=ts,
        partition=partition,
        offset=offset,
    )


def make_sink(consumer, writer=None, frontier=None, partitions=(0,), transforms=()):
    codec = IdCodec.from_key_schema(
        {"type": "record", "name": "k", "fields": [{"name": "id", "type": "long"}]}
    )
    buffer = TransactionBuffer()
    buffer.set_assignment(list(partitions))
    sink = Sink(
        consumer=consumer,
        topic=TOPIC,
        decoder=PassthroughDecoder(),
        translator=Translator(codec),
        buffer=buffer,
        writer=writer or RecordingWriter(),
        frontier=frontier or FakeFrontier(),
        transforms=transforms,
    )
    return sink


class TestFlushOnReadAhead:
    def test_transaction_flushed_when_next_ts_seen(self):
        consumer = FakeConsumer(
            [
                FakeMessage(make_event(100, 0, doc=1)),
                FakeMessage(make_event(100, 1, doc=2)),
                FakeMessage(make_event(200, 2, doc=3)),
            ]
        )
        writer = RecordingWriter()
        sink = make_sink(consumer, writer)
        for _ in range(4):
            sink.run_iteration()
        assert len(writer.transactions) == 1
        assert {op.id for op in writer.transactions[0]} == {1, 2}

    def test_offsets_committed_after_write(self):
        consumer = FakeConsumer(
            [
                FakeMessage(make_event(100, 0)),
                FakeMessage(make_event(200, 1)),
            ]
        )
        sink = make_sink(consumer)
        for _ in range(3):
            sink.run_iteration()
        # ts=100 flushed; its last offset is 0, so committed offset is 1
        assert consumer.commits == [[TopicPartition(TOPIC, 0, 1)]]


class TestFlushViaFrontier:
    def test_idle_topic_flushes_after_frontier_passes(self):
        frontier = FakeFrontier(None)
        consumer = FakeConsumer(
            [FakeMessage(make_event(100, 0))], high_watermarks={0: 1}
        )
        writer = RecordingWriter()
        sink = make_sink(consumer, writer, frontier)
        sink.run_iteration()
        assert writer.transactions == []  # frontier unknown, nothing settles
        frontier.value = 150
        sink.run_iteration()
        assert len(writer.transactions) == 1
        assert consumer.commits == [[TopicPartition(TOPIC, 0, 1)]]

    def test_empty_partition_settles_without_ever_consuming(self):
        # partition 1 has never had a message (low == high == 0) and nothing
        # was ever consumed from it; it must not block flushing forever
        frontier = FakeFrontier(150)
        consumer = FakeConsumer(
            [FakeMessage(make_event(100, 0))], high_watermarks={0: 1, 1: 0}
        )
        writer = RecordingWriter()
        sink = make_sink(consumer, writer, frontier, partitions=(0, 1))
        sink.run_iteration()
        sink.run_iteration()
        assert len(writer.transactions) == 1

    def test_restart_idle_partition_settles_via_committed_offset(self):
        # after a restart, position is OFFSET_INVALID until the first message;
        # the committed offset (== high watermark) proves we are caught up
        frontier = FakeFrontier(150)
        consumer = FakeConsumer(
            [FakeMessage(make_event(100, 5))],
            high_watermarks={0: 6, 1: 4},
            committed_offsets={1: 4},
        )
        writer = RecordingWriter()
        sink = make_sink(consumer, writer, frontier, partitions=(0, 1))
        sink.run_iteration()
        sink.run_iteration()
        assert len(writer.transactions) == 1

    def test_never_consumed_partition_with_data_does_not_settle(self):
        # partition 1 has published data (high=4), no committed offset, and
        # nothing consumed yet: it must NOT settle
        frontier = FakeFrontier(150)
        consumer = FakeConsumer(
            [FakeMessage(make_event(100, 0))], high_watermarks={0: 1, 1: 4}
        )
        writer = RecordingWriter()
        sink = make_sink(consumer, writer, frontier, partitions=(0, 1))
        sink.run_iteration()
        sink.run_iteration()
        assert writer.transactions == []

    def test_frontier_does_not_settle_partition_with_unread_data(self):
        # consumer position (0) is behind the high watermark (5): not caught up,
        # so the frontier alone must not settle the partition
        frontier = FakeFrontier(150)
        consumer = FakeConsumer([], high_watermarks={0: 5})
        writer = RecordingWriter()
        sink = make_sink(consumer, writer, frontier)
        sink.buffer.add(100, __import__("mz_tpuf_sink.models", fromlist=["Upsert"]).Upsert(id=1, row={"id": 1}))
        sink.run_iteration()
        assert writer.transactions == []


class TestSettlementThrottling:
    def test_watermarks_not_probed_per_message_when_frontier_unchanged(self):
        # frontier below everything: probing repeatedly can never settle more
        frontier = FakeFrontier(50)
        consumer = FakeConsumer(
            [FakeMessage(make_event(100 + i, i, doc=i)) for i in range(5)],
            high_watermarks={0: 5, 1: 0},
        )
        sink = make_sink(consumer, RecordingWriter(), frontier, partitions=(0, 1))
        sink.run_iteration()
        probes_after_first = consumer.watermark_calls
        for _ in range(4):  # busy iterations, frontier unchanged
            sink.run_iteration()
        assert consumer.watermark_calls == probes_after_first

    def test_probes_again_when_frontier_advances(self):
        frontier = FakeFrontier(50)
        consumer = FakeConsumer(
            [FakeMessage(make_event(100, 0)), FakeMessage(make_event(200, 1))],
            high_watermarks={0: 2, 1: 0},
        )
        sink = make_sink(consumer, RecordingWriter(), frontier, partitions=(0, 1))
        sink.run_iteration()
        sink.run_iteration()
        before = consumer.watermark_calls
        frontier.value = 300
        sink.run_iteration()
        assert consumer.watermark_calls > before


class TestNoOpTransactions:
    def test_offsets_committed_for_ts_with_no_effective_ops(self):
        # a timestamp whose every event is a no-change update produces no
        # write, but its offsets must still be committed once complete
        no_change = ChangeEvent(
            key={"id": 1},
            before={"id": 1, "v": 9},
            after={"id": 1, "v": 9},
            ts=100,
            partition=0,
            offset=0,
        )
        frontier = FakeFrontier(150)
        consumer = FakeConsumer([FakeMessage(no_change)], high_watermarks={0: 1})
        writer = RecordingWriter()
        sink = make_sink(consumer, writer, frontier)
        sink.run_iteration()
        sink.run_iteration()
        assert writer.transactions == []
        assert consumer.commits == [[TopicPartition(TOPIC, 0, 1)]]


class TestBackpressure:
    def test_partition_paused_then_resumed(self):
        # partition 1 is idle with no frontier info, so nothing can flush;
        # partition 0 reads two timestamps past the oldest and must be paused
        consumer = FakeConsumer(
            [
                FakeMessage(make_event(100, 0, doc=1)),
                FakeMessage(make_event(200, 1, doc=2)),
                FakeMessage(make_event(300, 2, doc=3)),
            ]
        )
        frontier = FakeFrontier(None)
        writer = RecordingWriter()
        sink = make_sink(consumer, writer, frontier, partitions=(0, 1))
        for _ in range(3):
            sink.run_iteration()
        assert writer.transactions == []
        assert consumer.paused == {0}
        # frontier passes everything; idle partition 1 settles via watermark
        frontier.value = 400
        consumer.high_watermarks[0] = 3
        sink.run_iteration()
        assert consumer.paused == set()
        assert len(writer.transactions) == 3


class TestTransforms:
    def _transform(self, compute, sources=("v",), schema=None):
        from mz_tpuf_sink.transform import FunctionTransform

        return FunctionTransform(
            name="embed",
            sources=tuple(sources),
            schema=schema or {"embedding": {"type": "[2]f32", "ann": True}},
            compute=compute,
            batch_size=100,
        )

    def test_derived_attribute_reaches_the_writer(self):
        consumer = FakeConsumer(
            [FakeMessage(make_event(100, 0)), FakeMessage(make_event(200, 1))]
        )
        writer = RecordingWriter()
        transform = self._transform(
            lambda rows: [{"embedding": [1.0, 2.0]} for _ in rows]
        )
        sink = make_sink(consumer, writer, transforms=[transform])
        for _ in range(3):
            sink.run_iteration()
        assert writer.transactions[0][0].row["embedding"] == [1.0, 2.0]

    def test_compute_runs_once_for_a_document_merged_within_one_timestamp(self):
        # insert then update of the same document in one timestamp merges to a
        # single op before transforms run, so the embedding is computed once
        insert = make_event(100, 0, doc=1)
        update = ChangeEvent(
            key={"id": 1},
            before={"id": 1, "v": 100},
            after={"id": 1, "v": 999},
            ts=100,
            partition=0,
            offset=1,
        )
        consumer = FakeConsumer(
            [
                FakeMessage(insert),
                FakeMessage(update),
                FakeMessage(make_event(200, 2, doc=2)),
            ]
        )
        calls = []

        def compute(rows):
            calls.append(len(rows))
            return [{"embedding": [0.0, 0.0]} for _ in rows]

        sink = make_sink(consumer, RecordingWriter(), transforms=[self._transform(compute)])
        for _ in range(4):
            sink.run_iteration()
        assert calls == [1]

    def test_transform_failure_leaves_offsets_uncommitted(self):
        from mz_tpuf_sink.transform import TransformError

        consumer = FakeConsumer(
            [FakeMessage(make_event(100, 0)), FakeMessage(make_event(200, 1))]
        )

        def boom(rows):
            raise RuntimeError("model down")

        sink = make_sink(consumer, RecordingWriter(), transforms=[self._transform(boom)])
        with pytest.raises(TransformError):
            for _ in range(3):
                sink.run_iteration()
        assert consumer.commits == []


class TestPerTimestampCommits:
    def test_each_timestamp_commits_before_the_next_is_written(self):
        # transforms make redoing work expensive, so a failure at timestamp N
        # must not force recomputing timestamps before it
        frontier = FakeFrontier(None)
        consumer = FakeConsumer(
            [
                FakeMessage(make_event(100, 0, doc=1)),
                FakeMessage(make_event(200, 1, doc=2)),
                FakeMessage(make_event(300, 2, doc=3)),
            ]
        )

        class FailOnSecond:
            def __init__(self):
                self.transactions = []

            def write_transaction(self, ops):
                ops = list(ops)
                if len(self.transactions) == 1:
                    raise RuntimeError("tpuf down")
                self.transactions.append(ops)

        writer = FailOnSecond()
        sink = make_sink(consumer, writer, frontier)
        with pytest.raises(RuntimeError):
            for _ in range(4):
                sink.run_iteration()
        # ts=100 was written and its offset committed before ts=200 failed
        assert len(writer.transactions) == 1
        assert consumer.commits == [[TopicPartition(TOPIC, 0, 1)]]


class TestWriteFailure:
    def test_writer_error_propagates_and_commits_nothing(self):
        consumer = FakeConsumer(
            [
                FakeMessage(make_event(100, 0)),
                FakeMessage(make_event(200, 1)),
            ]
        )
        writer = RecordingWriter(fail_times=1)
        sink = make_sink(consumer, writer)
        try:
            for _ in range(3):
                sink.run_iteration()
            raised = False
        except RuntimeError:
            raised = True
        assert raised
        assert consumer.commits == []
