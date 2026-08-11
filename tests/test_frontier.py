import pytest

from mz_tpuf_sink.frontier import FrontierState, FrontierWatcher, SUBSCRIBE_SQL


class TestFrontierState:
    def test_starts_unknown(self):
        assert FrontierState().current() is None

    def test_initial_snapshot_row_sets_frontier(self):
        state = FrontierState()
        state.apply(1000, 1)
        assert state.current() == 1000

    def test_frontier_advance_is_retraction_plus_insertion(self):
        state = FrontierState()
        state.apply(1000, 1)
        state.apply(1000, -1)
        state.apply(2000, 1)
        assert state.current() == 2000

    def test_insertion_before_retraction_within_batch(self):
        state = FrontierState()
        state.apply(1000, 1)
        state.apply(2000, 1)
        state.apply(1000, -1)
        assert state.current() == 2000

    def test_decimal_values_coerced_to_int(self):
        from decimal import Decimal

        state = FrontierState()
        state.apply(Decimal("1000"), 1)
        assert state.current() == 1000


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def stream(self, query, params=None):
        assert "SUBSCRIBE" in query
        yield from self._rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeConnection:
    def __init__(self, rows):
        self._rows = rows
        self.closed = False

    def cursor(self):
        return FakeCursor(self._rows)

    def close(self):
        self.closed = True


class TestFrontierWatcher:
    def test_consumes_stream_and_updates_state(self):
        # rows: (mz_timestamp, mz_diff, write_frontier)
        rows = [(1, 1, 1000), (2, -1, 1000), (2, 1, 2000)]
        watcher = FrontierWatcher(
            connect=lambda: FakeConnection(rows), sink_name="s"
        )
        watcher.run_once()
        assert watcher.current() == 2000

    def test_reconnects_after_connection_failure(self):
        calls = []

        def connect():
            calls.append(1)
            if len(calls) == 1:
                raise ConnectionError("boom")
            return FakeConnection([(1, 1, 3000)])

        watcher = FrontierWatcher(connect=connect, sink_name="s", backoff=lambda _: None)
        with pytest.raises(ConnectionError):
            watcher.run_once()
        watcher.run_once()
        assert watcher.current() == 3000

    def test_subscribe_query_joins_sink_lookup(self):
        assert "JOIN mz_sinks" in SUBSCRIBE_SQL
        assert "mz_internal.mz_frontiers" in SUBSCRIBE_SQL

    def test_null_write_frontier_is_skipped_not_fatal(self):
        # write_frontier is NULL when the sink's frontier is empty (e.g. the
        # sink was dropped); the watcher must not crash into a reconnect loop
        rows = [(1, 1, 1000), (2, 1, None)]
        watcher = FrontierWatcher(connect=lambda: FakeConnection(rows), sink_name="s")
        watcher.run_once()
        assert watcher.current() == 1000

    def test_wait_ready_returns_after_first_row(self):
        watcher = FrontierWatcher(
            connect=lambda: FakeConnection([(1, 1, 1000)]), sink_name="s"
        )
        watcher.run_once()
        watcher.wait_ready(timeout=0.01)  # must not raise

    def test_wait_ready_raises_when_no_rows_arrive(self):
        # e.g. a misspelled sink name yields an empty SUBSCRIBE forever
        watcher = FrontierWatcher(
            connect=lambda: FakeConnection([]), sink_name="typo_sink"
        )
        watcher.run_once()
        with pytest.raises(RuntimeError, match="typo_sink"):
            watcher.wait_ready(timeout=0.01)
