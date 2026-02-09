"""Tests for the settings module."""

from __future__ import annotations

import datetime
import unittest

import pydantic

from clickhouse_optimizer import settings


class TestOptimizerSettings(unittest.TestCase):
    """Tests for OptimizerSettings."""

    REQUIRED_ARGS = [
        '--host',
        'localhost',
        '--database',
        'default',
        '--user',
        'default',
        '--password',
        'secret',
        'test_table',
    ]

    def _create_settings(
        self, extra_args: list[str] | None = None
    ) -> settings.OptimizerSettings:
        args = list(self.REQUIRED_ARGS)
        if extra_args:
            args.extend(extra_args)
        return settings.OptimizerSettings(_cli_parse_args=args)

    def test_required_fields(self) -> None:
        s = self._create_settings()
        self.assertEqual(s.host, 'localhost')
        self.assertEqual(s.database, 'default')
        self.assertEqual(s.user, 'default')
        self.assertEqual(s.password.get_secret_value(), 'secret')
        self.assertEqual(s.table_name, 'test_table')

    def test_default_port(self) -> None:
        s = self._create_settings()
        self.assertEqual(s.port, 9440)

    def test_custom_port(self) -> None:
        s = self._create_settings(['--port', '9000'])
        self.assertEqual(s.port, 9000)

    def test_default_secure(self) -> None:
        s = self._create_settings()
        self.assertFalse(s.secure)

    def test_secure_enabled(self) -> None:
        s = self._create_settings(['--secure', 'true'])
        self.assertTrue(s.secure)

    def test_default_optimize_timeout(self) -> None:
        s = self._create_settings()
        self.assertEqual(s.optimize_timeout, 43200)

    def test_custom_optimize_timeout(self) -> None:
        s = self._create_settings(['--optimize-timeout', '3600'])
        self.assertEqual(s.optimize_timeout, 3600)

    def test_default_poll_interval(self) -> None:
        s = self._create_settings()
        self.assertEqual(s.poll_interval, 5)

    def test_custom_poll_interval(self) -> None:
        s = self._create_settings(['--poll-interval', '10'])
        self.assertEqual(s.poll_interval, 10)

    def test_default_verbose(self) -> None:
        s = self._create_settings()
        self.assertFalse(s.verbose)

    def test_verbose_enabled(self) -> None:
        s = self._create_settings(['--verbose', 'true'])
        self.assertTrue(s.verbose)

    def test_default_cluster(self) -> None:
        s = self._create_settings()
        self.assertIsNone(s.cluster)

    def test_custom_cluster(self) -> None:
        s = self._create_settings(['--cluster', 'my_cluster'])
        self.assertEqual(s.cluster, 'my_cluster')

    def test_default_min_date(self) -> None:
        s = self._create_settings()
        self.assertIsNone(s.min_date)

    def test_custom_min_date(self) -> None:
        s = self._create_settings(['--min-date', '2024-01-01'])
        self.assertEqual(s.min_date, datetime.date(2024, 1, 1))

    def test_default_max_date(self) -> None:
        s = self._create_settings()
        self.assertIsNone(s.max_date)

    def test_custom_max_date(self) -> None:
        s = self._create_settings(['--max-date', '2025-06-15'])
        self.assertEqual(s.max_date, datetime.date(2025, 6, 15))

    def test_min_date_in_future_raises(self) -> None:
        future = datetime.datetime.now(
            tz=datetime.UTC
        ).date() + datetime.timedelta(days=30)
        with self.assertRaises(pydantic.ValidationError) as ctx:
            self._create_settings(['--min-date', future.isoformat()])
        self.assertIn('min_date', str(ctx.exception))

    def test_max_date_in_future_raises(self) -> None:
        future = datetime.datetime.now(
            tz=datetime.UTC
        ).date() + datetime.timedelta(days=30)
        with self.assertRaises(pydantic.ValidationError) as ctx:
            self._create_settings(['--max-date', future.isoformat()])
        self.assertIn('max_date', str(ctx.exception))

    def test_min_date_after_max_date_raises(self) -> None:
        with self.assertRaises(pydantic.ValidationError) as ctx:
            self._create_settings(
                ['--min-date', '2025-06-15', '--max-date', '2024-01-01']
            )
        self.assertIn('min_date', str(ctx.exception))

    def test_valid_date_range(self) -> None:
        s = self._create_settings(
            ['--min-date', '2024-01-01', '--max-date', '2025-01-01']
        )
        self.assertEqual(s.min_date, datetime.date(2024, 1, 1))
        self.assertEqual(s.max_date, datetime.date(2025, 1, 1))
