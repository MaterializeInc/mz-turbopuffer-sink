"""Configuration via environment variables (MZ_TPUF_*) or a .env file."""

from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MZ_TPUF_", env_file=".env")

    # Kafka
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str = "mz-tpuf-sink"

    # Confluent Schema Registry
    schema_registry_url: str
    schema_registry_auth: str | None = None  # "user:password"

    # Materialize (for frontier tracking)
    materialize_dsn: str
    # Fully qualified as database.schema.sink. Sink names are unique only
    # within a schema, so a bare name could match sinks in several schemas and
    # mix their frontiers together.
    materialize_sink: str

    # turbopuffer
    turbopuffer_api_key: str
    turbopuffer_region: str | None = None
    turbopuffer_base_url: str | None = None
    namespace: str

    # Tuning
    max_rows_per_request: int = 10_000
    max_bytes_per_request: int = 200 * 1024 * 1024
    buffer_warn_bytes: int = 1024 * 1024 * 1024
    poll_timeout: float = 1.0
    frontier_ready_timeout: float = 15.0

    @field_validator("materialize_sink")
    @classmethod
    def _require_qualified_sink(cls, value: str) -> str:
        parts = [part.strip() for part in value.split(".")]
        if len(parts) != 3 or not all(parts):
            raise ValueError(
                f"materialize_sink must be fully qualified as "
                f"database.schema.sink (got {value!r})"
            )
        return ".".join(parts)

    @property
    def sink_parts(self) -> tuple[str, str, str]:
        """The sink's database, schema, and name."""
        database, schema, name = self.materialize_sink.split(".")
        return database, schema, name
