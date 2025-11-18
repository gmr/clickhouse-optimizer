"""Configuration management for ClickHouse optimizer."""

from __future__ import annotations

import datetime

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
    min_date: datetime.date | None = pydantic.Field(
        default=None,
        description='Minimum partition date to optimize (YYYY-MM-DD)',
        alias='min-date',
    )
    max_date: datetime.date | None = pydantic.Field(
        default=None,
        description='Maximum partition date to optimize (YYYY-MM-DD)',
        alias='max-date',
    )
    table_name: pydantic_settings.CliPositionalArg[str] = pydantic.Field(
        description='Table to optimize'
    )

    @pydantic.field_validator('min_date')
    @classmethod
    def validate_min_date(
        cls, value: datetime.date | None
    ) -> datetime.date | None:
        """Validate that min_date is not in the future."""
        if value is not None:
            today = datetime.datetime.now(tz=datetime.UTC).date()
            if value > today:
                raise ValueError('min_date cannot be in the future')
        return value

    @pydantic.field_validator('max_date')
    @classmethod
    def validate_max_date(
        cls, value: datetime.date | None
    ) -> datetime.date | None:
        """Validate that max_date is not in the future."""
        if value is not None:
            today = datetime.datetime.now(tz=datetime.UTC).date()
            if value > today:
                raise ValueError('max_date cannot be in the future')
        return value

    @pydantic.model_validator(mode='after')
    def validate_date_range(self) -> OptimizerSettings:
        """Validate that min_date <= max_date."""
        if (
            self.min_date is not None
            and self.max_date is not None
            and self.min_date > self.max_date
        ):
            raise ValueError('min_date cannot be greater than max_date')
        return self
