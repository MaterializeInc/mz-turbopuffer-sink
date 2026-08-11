import signal

import pytest
from click.testing import CliRunner
from mz_tpuf_sink import SinkConfig
from pydantic import ValidationError

from mz_tpuf_sink_cli import cli as cli_module
from mz_tpuf_sink_cli.cli import main
from mz_tpuf_sink_cli.settings import Settings

REQUIRED = {
    "MZ_TPUF_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "MZ_TPUF_KAFKA_TOPIC": "events",
    "MZ_TPUF_SCHEMA_REGISTRY_URL": "http://localhost:8081",
    "MZ_TPUF_MATERIALIZE_DSN": "postgres://materialize@localhost:6875/materialize",
    "MZ_TPUF_MATERIALIZE_SINK": "materialize.public.events_sink",
    "MZ_TPUF_TURBOPUFFER_API_KEY": "tpuf-key",
    "MZ_TPUF_NAMESPACE": "events",
}


@pytest.fixture
def env(monkeypatch, tmp_path):
    # run somewhere without a .env so the real one can never leak in
    monkeypatch.chdir(tmp_path)
    for key, value in REQUIRED.items():
        monkeypatch.setenv(key, value)
    return monkeypatch


class TestSettings:
    def test_reads_prefixed_environment_variables(self, env):
        settings = Settings()
        assert settings.kafka_topic == "events"
        assert settings.materialize_sink == "materialize.public.events_sink"

    def test_missing_required_setting_fails(self, env):
        env.delenv("MZ_TPUF_TURBOPUFFER_API_KEY")
        with pytest.raises(ValidationError):
            Settings()

    def test_library_validation_still_applies(self, env):
        env.setenv("MZ_TPUF_MATERIALIZE_SINK", "unqualified")
        with pytest.raises(ValidationError, match="database.schema.sink"):
            Settings()

    def test_to_config_returns_a_plain_sink_config(self, env):
        config = Settings().to_config()
        assert type(config) is SinkConfig
        assert config.sink_parts == ("materialize", "public", "events_sink")

    def test_reads_a_dotenv_file(self, env, tmp_path):
        env.delenv("MZ_TPUF_NAMESPACE")
        (tmp_path / ".env").write_text("MZ_TPUF_NAMESPACE=from-dotenv\n")
        assert Settings().namespace == "from-dotenv"


class TestMain:
    def test_runs_the_sink_with_config_from_the_environment(self, env):
        captured = {}

        def fake_run_sink(config, stop=None):
            captured["config"] = config
            captured["stop"] = stop

        env.setattr(cli_module, "run_sink", fake_run_sink)
        result = CliRunner().invoke(main, [])

        assert result.exit_code == 0, result.output
        assert captured["config"].kafka_topic == "events"
        assert captured["config"].namespace == "events"

    def test_signal_handler_sets_the_stop_event(self, env):
        captured = {}

        def fake_run_sink(config, stop=None):
            captured["stop"] = stop
            # the handler is installed by now; deliver the signal to ourselves
            signal.raise_signal(signal.SIGINT)

        env.setattr(cli_module, "run_sink", fake_run_sink)
        result = CliRunner().invoke(main, [])

        assert result.exit_code == 0, result.output
        assert captured["stop"].is_set(), "SIGINT should request a clean shutdown"

    def test_bad_configuration_is_reported(self, env):
        env.delenv("MZ_TPUF_NAMESPACE")
        result = CliRunner().invoke(main, [])
        assert result.exit_code != 0
