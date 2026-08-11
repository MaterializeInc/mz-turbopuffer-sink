from mz_tpuf_sink.buffer import TransactionBuffer
from mz_tpuf_sink.models import Delete, Patch, Upsert


def make_buffer(partitions=(0,)):
    buf = TransactionBuffer()
    buf.set_assignment(list(partitions))
    return buf


class TestCompletenessViaReadAhead:
    """Rule (a): a partition is settled past F once it yields a message with ts >= F."""

    def test_nothing_flushable_before_any_settlement(self):
        buf = make_buffer([0, 1])
        buf.observe(0, 100)
        buf.add(100, Upsert(id=1, row={"id": 1}))
        assert buf.take_flushable() == []

    def test_flushes_when_all_partitions_read_past_ts(self):
        buf = make_buffer([0, 1])
        buf.observe(0, 100)
        buf.add(100, Upsert(id=1, row={"id": 1}))
        buf.observe(0, 200)  # partition 0 settled past 200
        buf.observe(1, 150)  # partition 1 settled past 150
        flushed = buf.take_flushable()
        assert flushed == [(100, [Upsert(id=1, row={"id": 1})])]

    def test_ts_equal_to_partition_max_is_not_flushable(self):
        # more messages may still arrive at the same timestamp
        buf = make_buffer([0])
        buf.observe(0, 100)
        buf.add(100, Upsert(id=1, row={"id": 1}))
        assert buf.take_flushable() == []

    def test_flushes_ascending_and_removes(self):
        buf = make_buffer([0])
        for ts, doc in [(100, 1), (200, 2)]:
            buf.observe(0, ts)
            buf.add(ts, Upsert(id=doc, row={"id": doc}))
        buf.observe(0, 300)
        flushed = buf.take_flushable()
        assert [ts for ts, _ in flushed] == [100, 200]
        assert buf.take_flushable() == []

    def test_unassigned_partition_does_not_block(self):
        buf = make_buffer([0])
        buf.observe(0, 100)
        buf.add(100, Upsert(id=1, row={"id": 1}))
        buf.observe(0, 200)
        assert len(buf.take_flushable()) == 1


class TestCompletenessViaWatermark:
    """Rule (b): an idle partition settles past F once caught up to its high watermark."""

    def test_idle_partition_blocks_flush(self):
        buf = make_buffer([0, 1])
        buf.observe(0, 100)
        buf.add(100, Upsert(id=1, row={"id": 1}))
        buf.observe(0, 200)
        assert buf.take_flushable() == []  # partition 1 silent

    def test_watermark_settlement_unblocks_idle_partition(self):
        buf = make_buffer([0, 1])
        buf.observe(0, 100)
        buf.add(100, Upsert(id=1, row={"id": 1}))
        buf.observe(0, 200)
        buf.settle_watermark(1, 150)  # caught up to watermark after frontier=150
        assert buf.take_flushable() == [(100, [Upsert(id=1, row={"id": 1})])]

    def test_watermark_settlement_is_monotonic(self):
        buf = make_buffer([0])
        buf.settle_watermark(0, 200)
        buf.settle_watermark(0, 100)  # stale, must not regress
        buf.add(150, Upsert(id=1, row={"id": 1}))
        assert buf.take_flushable() == [(150, [Upsert(id=1, row={"id": 1})])]


class TestPerKeyMerge:
    def test_patch_onto_upsert_merges_into_row(self):
        buf = make_buffer([0])
        buf.add(100, Upsert(id=1, row={"id": 1, "a": 1, "b": 2}))
        buf.add(100, Patch(id=1, columns={"b": 3}))
        buf.settle_watermark(0, 500)
        assert buf.take_flushable() == [
            (100, [Upsert(id=1, row={"id": 1, "a": 1, "b": 3})])
        ]

    def test_patch_onto_patch_merges_columns(self):
        buf = make_buffer([0])
        buf.add(100, Patch(id=1, columns={"a": 1}))
        buf.add(100, Patch(id=1, columns={"b": 2}))
        buf.settle_watermark(0, 500)
        assert buf.take_flushable() == [(100, [Patch(id=1, columns={"a": 1, "b": 2})])]

    def test_delete_replaces_prior_ops(self):
        buf = make_buffer([0])
        buf.add(100, Upsert(id=1, row={"id": 1}))
        buf.add(100, Delete(id=1))
        buf.settle_watermark(0, 500)
        assert buf.take_flushable() == [(100, [Delete(id=1)])]

    def test_distinct_keys_kept_separately(self):
        buf = make_buffer([0])
        buf.add(100, Upsert(id=1, row={"id": 1}))
        buf.add(100, Upsert(id=2, row={"id": 2}))
        buf.settle_watermark(0, 500)
        [(_, ops)] = buf.take_flushable()
        assert len(ops) == 2


class TestBackpressure:
    """A partition may read the oldest unflushed ts plus one timestamp ahead."""

    def test_no_pause_with_single_read_ahead(self):
        buf = make_buffer([0])
        buf.observe(0, 100)
        buf.add(100, Upsert(id=1, row={"id": 1}))
        buf.observe(0, 200)
        assert buf.pause_set() == set()

    def test_pause_on_second_read_ahead_timestamp(self):
        buf = make_buffer([0])
        buf.observe(0, 100)
        buf.add(100, Upsert(id=1, row={"id": 1}))
        buf.observe(0, 200)
        buf.add(200, Upsert(id=2, row={"id": 2}))
        buf.observe(0, 300)
        assert buf.pause_set() == {0}

    def test_resume_after_flush(self):
        buf = make_buffer([0])
        buf.observe(0, 100)
        buf.add(100, Upsert(id=1, row={"id": 1}))
        buf.observe(0, 200)
        buf.add(200, Upsert(id=2, row={"id": 2}))
        buf.observe(0, 300)
        assert buf.pause_set() == {0}
        buf.take_flushable()  # partition read to 300, so both 100 and 200 flush
        assert buf.pause_set() == set()

    def test_no_pause_when_nothing_buffered(self):
        buf = make_buffer([0])
        buf.observe(0, 100)
        buf.observe(0, 200)
        buf.observe(0, 300)
        assert buf.pause_set() == set()

    def test_partition_without_backlog_is_not_paused(self):
        buf = make_buffer([0, 1])
        buf.observe(0, 100)
        buf.add(100, Upsert(id=1, row={"id": 1}))
        buf.observe(0, 200)
        buf.add(200, Upsert(id=2, row={"id": 2}))
        buf.observe(0, 300)
        buf.observe(1, 300)
        assert buf.pause_set() == {0}


class TestBufferedBytesWarning:
    def test_warns_once_when_crossing_high_water_mark(self, caplog):
        import logging

        buf = TransactionBuffer(warn_bytes=50)
        buf.set_assignment([0])
        with caplog.at_level(logging.WARNING):
            buf.add(100, Upsert(id=1, row={"id": 1, "blob": "x" * 100}))
            buf.add(100, Upsert(id=2, row={"id": 2, "blob": "x" * 100}))
        warnings = [r for r in caplog.records if "buffered" in r.message]
        assert len(warnings) == 1

    def test_warns_again_after_draining(self, caplog):
        import logging

        buf = TransactionBuffer(warn_bytes=50)
        buf.set_assignment([0])
        with caplog.at_level(logging.WARNING):
            buf.add(100, Upsert(id=1, row={"id": 1, "blob": "x" * 100}))
            buf.settle_watermark(0, 500)
            buf.take_flushable()
            buf.add(600, Upsert(id=2, row={"id": 2, "blob": "x" * 100}))
        warnings = [r for r in caplog.records if "buffered" in r.message]
        assert len(warnings) == 2


class TestRebalance:
    def test_clear_drops_all_buffered_state(self):
        buf = make_buffer([0])
        buf.observe(0, 100)
        buf.add(100, Upsert(id=1, row={"id": 1}))
        buf.settle_watermark(0, 500)
        buf.clear()
        assert buf.take_flushable() == []
        buf.add(100, Upsert(id=1, row={"id": 1}))
        # old settlement must not survive the clear
        assert buf.take_flushable() == []

    def test_removed_partition_state_dropped(self):
        buf = make_buffer([0, 1])
        buf.observe(0, 100)
        buf.add(100, Upsert(id=1, row={"id": 1}))
        buf.observe(0, 200)
        # partition 1 idle and would block; rebalance away from it
        buf.set_assignment([0])
        assert buf.take_flushable() == [(100, [Upsert(id=1, row={"id": 1})])]
