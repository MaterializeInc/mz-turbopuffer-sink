"""Configuration via environment variables (MZ_TPUF_*) or a .env file."""

from __future__ import annotations

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
    materialize_sink: str  # sink name as it appears in mz_sinks.name

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
