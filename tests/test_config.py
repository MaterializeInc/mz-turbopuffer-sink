import pytest
from pydantic import ValidationError

from mz_tpuf_sink.config import Settings

REQUIRED = {
    "MZ_TPUF_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "MZ_TPUF_KAFKA_TOPIC": "events",
    "MZ_TPUF_SCHEMA_REGISTRY_URL": "http://localhost:8081",
    "MZ_TPUF_MATERIALIZE_DSN": "postgres://materialize@localhost:6875/materialize",
    "MZ_TPUF_MATERIALIZE_SINK": "events_sink",
    "MZ_TPUF_TURBOPUFFER_API_KEY": "tpuf-key",
    "MZ_TPUF_NAMESPACE": "events",
}


def set_required(monkeypatch):
    for key, value in REQUIRED.items():
        monkeypatch.setenv(key, value)


class TestSettings:
    def test_reads_from_environment(self, monkeypatch):
        set_required(monkeypatch)
        settings = Settings()
        assert settings.kafka_topic == "events"
        assert settings.namespace == "events"
        assert settings.materialize_sink == "events_sink"

    def test_sensible_defaults(self, monkeypatch):
        set_required(monkeypatch)
        settings = Settings()
        assert settings.kafka_group_id == "mz-tpuf-sink"
        assert settings.max_rows_per_request == 10_000
        assert settings.poll_timeout == 1.0

    def test_missing_required_setting_fails(self, monkeypatch):
        set_required(monkeypatch)
        monkeypatch.delenv("MZ_TPUF_TURBOPUFFER_API_KEY")
        with pytest.raises(ValidationError):
            Settings(_env_file=None)
