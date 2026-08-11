import json

import pytest

from mz_tpuf_sink import FunctionTransform, SinkConfig
from mz_tpuf_sink import runner as runner_module

ENVELOPE = {
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "row",
                    "fields": [
                        {"name": "id", "type": ["null", "long"]},
                        {"name": "title", "type": ["null", "string"]},
                        {"name": "body", "type": ["null", "string"]},
                    ],
                },
            ],
        },
        {"name": "after", "type": ["null", "row"]},
    ],
}
KEY = {
    "type": "record",
    "name": "row",
    "fields": [{"name": "id", "type": ["null", "long"]}],
}


class FakeSchema:
    def __init__(self, schema):
        self.schema_str = json.dumps(schema)


class FakeVersion:
    def __init__(self, schema):
        self.schema = FakeSchema(schema)


class FakeRegistry:
    def __init__(self, *args, **kwargs):
        pass

    def get_latest_version(self, subject):
        return FakeVersion(KEY if subject.endswith("-key") else ENVELOPE)


class Captured:
    """Records what _build_sink hands to each collaborator."""

    def __init__(self):
        self.consumer_config = None
        self.writer_kwargs = None
        self.translator_args = None
        self.sink_kwargs = None


@pytest.fixture
def captured(monkeypatch):
    box = Captured()

    monkeypatch.setattr(runner_module, "SchemaRegistryClient", FakeRegistry)
    monkeypatch.setattr(runner_module, "AvroDeserializer", lambda *a, **k: None)
    monkeypatch.setattr(runner_module, "Turbopuffer", lambda **k: object())
    monkeypatch.setattr(runner_module, "psycopg", None)

    def fake_consumer(config):
        box.consumer_config = config
        return object()

    def fake_writer(client, **kwargs):
        box.writer_kwargs = kwargs
        return object()

    real_translator = runner_module.Translator

    def fake_translator(codec, retain_groups=(), full_row_triggers=frozenset()):
        box.translator_args = (codec, list(retain_groups), full_row_triggers)
        return real_translator(codec, retain_groups, full_row_triggers)

    def fake_sink(**kwargs):
        box.sink_kwargs = kwargs
        return object()

    monkeypatch.setattr(runner_module, "Consumer", fake_consumer)
    monkeypatch.setattr(runner_module, "Writer", fake_writer)
    monkeypatch.setattr(runner_module, "Translator", fake_translator)
    monkeypatch.setattr(runner_module, "Sink", fake_sink)
    monkeypatch.setattr(runner_module, "FrontierWatcher", lambda **k: object())
    return box


def make_config(**overrides):
    values = {
        "kafka_bootstrap_servers": "localhost:9092",
        "kafka_topic": "events",
        "schema_registry_url": "http://localhost:8081",
        "materialize_dsn": "postgres://materialize@localhost:6875/materialize",
        "materialize_sink": "materialize.public.events_sink",
        "turbopuffer_api_key": "key",
        "namespace": "events",
    }
    values.update(overrides)
    return SinkConfig(**values)


def embedding(name="embed", sources=("title",)):
    return FunctionTransform(
        name=name,
        sources=tuple(sources),
        schema={"embedding": {"type": "[2]f32", "ann": True}},
        compute=lambda rows: [{"embedding": [0.0, 0.0]} for _ in rows],
        distance_metric="cosine_distance",
    )


class TestMaxPollInterval:
    def test_absent_by_default(self, captured):
        runner_module._build_sink(make_config())
        assert "max.poll.interval.ms" not in captured.consumer_config

    def test_passed_through_when_set(self, captured):
        runner_module._build_sink(make_config(kafka_max_poll_interval_ms=900_000))
        assert captured.consumer_config["max.poll.interval.ms"] == 900_000

    def test_drives_the_slow_flush_warning_threshold(self, captured):
        runner_module._build_sink(make_config(kafka_max_poll_interval_ms=600_000))
        assert captured.sink_kwargs["slow_flush_seconds"] == pytest.approx(300.0)


class TestTransformWiring:
    def test_produced_attributes_reach_the_writer_schema(self, captured):
        runner_module._build_sink(make_config(), transforms=[embedding()])
        schema = captured.writer_kwargs["schema"]
        assert schema["embedding"] == {"type": "[2]f32", "ann": True}
        assert schema["title"] == {"type": "string"}
        assert schema["id"] == {"type": "uint"}

    def test_multi_source_transform_becomes_a_retain_group(self, captured):
        runner_module._build_sink(
            make_config(), transforms=[embedding(sources=("title", "body"))]
        )
        assert captured.translator_args[1] == [frozenset({"title", "body"})]

    def test_single_source_transform_adds_no_retain_group(self, captured):
        runner_module._build_sink(make_config(), transforms=[embedding()])
        assert captured.translator_args[1] == []

    def test_distance_metric_reaches_the_writer(self, captured):
        runner_module._build_sink(make_config(), transforms=[embedding()])
        assert captured.writer_kwargs["distance_metric"] == "cosine_distance"

    def test_no_distance_metric_without_transforms(self, captured):
        runner_module._build_sink(make_config())
        assert captured.writer_kwargs["distance_metric"] is None

    def test_vector_transform_sources_force_full_row_upserts(self, captured):
        runner_module._build_sink(make_config(), transforms=[embedding()])
        assert captured.translator_args[2] == frozenset({"title"})

    def test_non_vector_transform_does_not_force_full_rows(self, captured):
        slug = FunctionTransform(
            name="slug",
            sources=("title",),
            schema={"slug": {"type": "string"}},
            compute=lambda rows: [{"slug": "x"} for _ in rows],
        )
        runner_module._build_sink(make_config(), transforms=[slug])
        assert captured.translator_args[2] == frozenset()

    def test_transforms_reach_the_sink(self, captured):
        transform = embedding()
        runner_module._build_sink(make_config(), transforms=[transform])
        assert list(captured.sink_kwargs["transforms"]) == [transform]

    def test_no_transforms_leaves_the_schema_untouched(self, captured):
        runner_module._build_sink(make_config())
        assert set(captured.writer_kwargs["schema"]) == {"id", "title", "body"}
        assert captured.sink_kwargs["transforms"] == ()

    def test_invalid_transform_fails_before_any_kafka_connection(self, captured):
        with pytest.raises(ValueError, match="nosuch"):
            runner_module._build_sink(
                make_config(), transforms=[embedding(sources=("nosuch",))]
            )
        assert captured.consumer_config is None
