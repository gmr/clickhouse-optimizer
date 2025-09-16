"""Configuration management for ClickHouse optimizer."""

from __future__ import annotations

import pydantic
import pydantic_settings


class OptimizerSettings(pydantic_settings.BaseSettings):
    """Application settings with environment variable support."""

    model_config = pydantic_settings.SettingsConfigDict(
        cli_parse_args=True,
        cli_enforce_required=True,
        cli_prog_name='clickhouse-optimizer',
    )
    host: str = pydantic.Field(description='ClickHouse server hostname')
    port: int = pydantic.Field(
        default=9440, description='ClickHouse server port'
    )
    database: str = pydantic.Field(description='Database name')
    secure: bool = pydantic.Field(
        default=False, description='Use secure connection'
    )
    user: str = pydantic.Field(description='Username for authentication')
    password: pydantic.SecretStr = pydantic.Field(
        description='Password for authentication'
    )
    optimize_timeout: int = pydantic.Field(
        default=43200,
        description='Maximum seconds to wait for merges',
        alias='optimize-timeout',
    )
    poll_interval: int = pydantic.Field(
        default=5,
        description='Seconds between merge status checks',
        alias='poll-interval',
    )
    verbose: bool = pydantic.Field(
        default=False, description='Enable verbose logging'
    )
    table_name: pydantic_settings.CliPositionalArg[str] = pydantic.Field(
        description='Table to optimize'
    )
