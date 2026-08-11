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


class FakeConsumer:
    def __init__(self, messages, high_watermarks=None):
        self.queue = list(messages)
        self.paused = set()
        self.commits = []
        self.closed = False
        self.high_watermarks = high_watermarks or {}
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

    def commit(self, offsets, asynchronous):
        assert asynchronous is False
        self.commits.append(list(offsets))

    def get_watermark_offsets(self, tp, timeout=None):
        return (0, self.high_watermarks.get(tp.partition, 0))

    def position(self, tps):
        return [
            TopicPartition(TOPIC, tp.partition, self._positions.get(tp.partition, 0))
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


def make_sink(consumer, writer=None, frontier=None, partitions=(0,)):
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
