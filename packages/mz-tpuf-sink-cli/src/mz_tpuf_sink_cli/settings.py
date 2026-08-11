"""Load a SinkConfig from MZ_TPUF_* environment variables or a .env file.

Subclassing SinkConfig keeps the field list in one place: the library owns the
shape and validation, this module only adds where the values come from.
"""

from __future__ import annotations

from mz_tpuf_sink import SinkConfig
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(SinkConfig, BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MZ_TPUF_", env_file=".env", extra="ignore"
    )

    def to_config(self) -> SinkConfig:
        return SinkConfig(**self.model_dump())
