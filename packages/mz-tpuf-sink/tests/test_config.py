import pytest
from pydantic import ValidationError

from mz_tpuf_sink import SinkConfig

REQUIRED = {
    "kafka_bootstrap_servers": "localhost:9092",
    "kafka_topic": "events",
    "schema_registry_url": "http://localhost:8081",
    "materialize_dsn": "postgres://materialize@localhost:6875/materialize",
    "materialize_sink": "materialize.public.events_sink",
    "turbopuffer_api_key": "tpuf-key",
    "namespace": "events",
}


class TestSinkConfig:
    def test_built_from_explicit_values(self):
        config = SinkConfig(**REQUIRED)
        assert config.kafka_topic == "events"
        assert config.namespace == "events"

    def test_sensible_defaults(self):
        config = SinkConfig(**REQUIRED)
        assert config.kafka_group_id == "mz-tpuf-sink"
        assert config.max_rows_per_request == 10_000
        assert config.poll_timeout == 1.0

    def test_missing_required_field_fails(self):
        missing = {k: v for k, v in REQUIRED.items() if k != "turbopuffer_api_key"}
        with pytest.raises(ValidationError):
            SinkConfig(**missing)

    def test_environment_is_never_consulted(self, monkeypatch):
        # the library must not pick up ambient settings; that is the CLI's job
        monkeypatch.setenv("MZ_TPUF_NAMESPACE", "from-the-environment")
        monkeypatch.setenv("MZ_TPUF_TURBOPUFFER_API_KEY", "from-the-environment")
        missing = {k: v for k, v in REQUIRED.items() if k != "namespace"}
        with pytest.raises(ValidationError):
            SinkConfig(**missing)

    def test_unknown_field_rejected(self):
        with pytest.raises(ValidationError):
            SinkConfig(**REQUIRED, kafka_topics="typo")


class TestSinkNameQualification:
    """A bare sink name is ambiguous across schemas, so the fully qualified
    database.schema.sink form is required."""

    def test_parts_are_split(self):
        config = SinkConfig(**REQUIRED)
        assert config.sink_parts == ("materialize", "public", "events_sink")

    @pytest.mark.parametrize(
        "value", ["events_sink", "public.events_sink", "a.b.c.d", "a..c", ".b.c", ""]
    )
    def test_unqualified_or_malformed_names_rejected(self, value):
        with pytest.raises(ValidationError, match="database.schema.sink"):
            SinkConfig(**{**REQUIRED, "materialize_sink": value})

    def test_whitespace_is_trimmed(self):
        config = SinkConfig(**{**REQUIRED, "materialize_sink": " db . sch . snk "})
        assert config.sink_parts == ("db", "sch", "snk")
