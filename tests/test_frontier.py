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
    def __init__(self, rows, recorder=None):
        self._rows = rows
        self._recorder = recorder

    def stream(self, query, params=None):
        assert "SUBSCRIBE" in query
        if self._recorder is not None:
            self._recorder.append((query, params))
        yield from self._rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeConnection:
    def __init__(self, rows, recorder=None):
        self._rows = rows
        self._recorder = recorder
        self.closed = False

    def cursor(self):
        return FakeCursor(self._rows, self._recorder)

    def close(self):
        self.closed = True


class TestFrontierWatcher:
    def test_consumes_stream_and_updates_state(self):
        # rows: (mz_timestamp, mz_diff, write_frontier)
        rows = [(1, 1, 1000), (2, -1, 1000), (2, 1, 2000)]
        watcher = FrontierWatcher(
            connect=lambda: FakeConnection(rows), database_name="d", schema_name="s", sink_name="snk", topic="t"
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

        watcher = FrontierWatcher(connect=connect, database_name="d", schema_name="s", sink_name="snk", topic="t", backoff=lambda _: None)
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
        watcher = FrontierWatcher(connect=lambda: FakeConnection(rows), database_name="d", schema_name="s", sink_name="snk", topic="t")
        watcher.run_once()
        assert watcher.current() == 1000

    def test_wait_ready_returns_after_first_row(self):
        watcher = FrontierWatcher(
            connect=lambda: FakeConnection([(1, 1, 1000)]), database_name="d", schema_name="s", sink_name="snk", topic="t"
        )
        watcher.run_once()
        watcher.wait_ready(timeout=0.01)  # must not raise

    def test_wait_ready_raises_when_no_rows_arrive(self):
        # e.g. a misspelled sink name yields an empty SUBSCRIBE forever
        watcher = FrontierWatcher(
            connect=lambda: FakeConnection([]), database_name="d", schema_name="s", sink_name="typo_sink", topic="t"
        )
        watcher.run_once()
        with pytest.raises(RuntimeError, match="typo_sink"):
            watcher.wait_ready(timeout=0.01)


class TestSinkIdentification:
    """Sink names are scoped by schema, so a bare name is ambiguous: two
    schemas may each hold a sink of the same name, and both frontiers would
    then feed FrontierState and could prematurely complete a timestamp. The
    sink must therefore be named database.schema.sink."""

    def _run(self, **kwargs):
        recorder = []
        params = dict(
            database_name="materialize",
            schema_name="public",
            sink_name="products_sink",
            topic="products",
        )
        params.update(kwargs)
        watcher = FrontierWatcher(
            connect=lambda: FakeConnection([(1, 1, 1000)], recorder), **params
        )
        watcher.run_once()
        return recorder[0]

    def test_query_qualifies_by_database_and_schema(self):
        query, params = self._run()
        assert "mz_schemas" in query and "mz_databases" in query
        assert params["database_name"] == "materialize"
        assert params["schema_name"] == "public"
        assert params["sink_name"] == "products_sink"

    def test_database_is_never_inferred_from_the_connection(self):
        query, _ = self._run()
        assert "current_database()" not in query

    def test_all_three_parts_are_used(self):
        _, params = self._run(database_name="analytics", schema_name="search")
        assert params["database_name"] == "analytics"
        assert params["schema_name"] == "search"

    def test_topic_is_validated_against_the_sink(self):
        # catches a sink name that points at a different topic than the one
        # being consumed
        query, params = self._run()
        assert "mz_kafka_sinks" in query
        assert params["topic"] == "products"

    def test_error_message_names_the_fully_qualified_sink(self):
        watcher = FrontierWatcher(
            connect=lambda: FakeConnection([]),
            database_name="analytics",
            schema_name="search",
            sink_name="typo_sink",
            topic="products",
        )
        watcher.run_once()
        with pytest.raises(RuntimeError, match="analytics.search.typo_sink"):
            watcher.wait_ready(timeout=0.01)
