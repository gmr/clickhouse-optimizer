"""Tests for the CLI module."""

from __future__ import annotations

import logging
import unittest
from unittest import mock

from clickhouse_optimizer import cli


class TestSetupLogging(unittest.TestCase):
    """Tests for setup_logging."""

    def test_default_level_is_info(self) -> None:
        with mock.patch('logging.basicConfig') as mock_config:
            cli.setup_logging()
        mock_config.assert_called_once()
        call_kwargs = mock_config.call_args[1]
        self.assertEqual(call_kwargs['level'], logging.INFO)

    def test_verbose_sets_debug(self) -> None:
        with mock.patch('logging.basicConfig') as mock_config:
            cli.setup_logging(verbose=True)
        mock_config.assert_called_once()
        call_kwargs = mock_config.call_args[1]
        self.assertEqual(call_kwargs['level'], logging.DEBUG)


class TestMain(unittest.TestCase):
    """Tests for main entry point."""

    @mock.patch('clickhouse_optimizer.cli.optimizer')
    @mock.patch('clickhouse_optimizer.cli.settings')
    def test_main_success(
        self, mock_settings: mock.Mock, mock_optimizer: mock.Mock
    ) -> None:
        mock_settings.OptimizerSettings.return_value = mock.Mock(verbose=False)
        cli.main()
        mock_optimizer.ClickHouseOptimizer.assert_called_once()
        mock_optimizer.ClickHouseOptimizer.return_value.run.assert_called_once()

    @mock.patch('clickhouse_optimizer.cli.optimizer')
    @mock.patch('clickhouse_optimizer.cli.settings')
    def test_main_keyboard_interrupt(
        self, mock_settings: mock.Mock, mock_optimizer: mock.Mock
    ) -> None:
        mock_settings.OptimizerSettings.return_value = mock.Mock(verbose=False)
        mock_optimizer.ClickHouseOptimizer.return_value.run.side_effect = (
            KeyboardInterrupt()
        )
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertEqual(ctx.exception.code, 130)

    @mock.patch('clickhouse_optimizer.cli.optimizer')
    @mock.patch('clickhouse_optimizer.cli.settings')
    def test_main_system_exit_preserves_code(
        self, mock_settings: mock.Mock, mock_optimizer: mock.Mock
    ) -> None:
        mock_settings.OptimizerSettings.return_value = mock.Mock(verbose=False)
        mock_optimizer.ClickHouseOptimizer.return_value.run.side_effect = (
            SystemExit(42)
        )
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertEqual(ctx.exception.code, 42)

    @mock.patch('clickhouse_optimizer.cli.optimizer')
    @mock.patch('clickhouse_optimizer.cli.settings')
    def test_main_system_exit_no_code(
        self, mock_settings: mock.Mock, mock_optimizer: mock.Mock
    ) -> None:
        mock_settings.OptimizerSettings.return_value = mock.Mock(verbose=False)
        mock_optimizer.ClickHouseOptimizer.return_value.run.side_effect = (
            SystemExit()
        )
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertIsNone(ctx.exception.code)

    @mock.patch('clickhouse_optimizer.cli.optimizer')
    @mock.patch('clickhouse_optimizer.cli.settings')
    def test_main_value_error(
        self, mock_settings: mock.Mock, mock_optimizer: mock.Mock
    ) -> None:
        mock_settings.OptimizerSettings.return_value = mock.Mock(verbose=False)
        mock_optimizer.ClickHouseOptimizer.return_value.run.side_effect = (
            ValueError('bad value')
        )
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertEqual(ctx.exception.code, 1)

    @mock.patch('clickhouse_optimizer.cli.optimizer')
    @mock.patch('clickhouse_optimizer.cli.settings')
    def test_main_os_error(
        self, mock_settings: mock.Mock, mock_optimizer: mock.Mock
    ) -> None:
        mock_settings.OptimizerSettings.return_value = mock.Mock(verbose=False)
        mock_optimizer.ClickHouseOptimizer.return_value.run.side_effect = (
            OSError('connection failed')
        )
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertEqual(ctx.exception.code, 1)

    @mock.patch('clickhouse_optimizer.cli.optimizer')
    @mock.patch('clickhouse_optimizer.cli.settings')
    def test_main_runtime_error(
        self, mock_settings: mock.Mock, mock_optimizer: mock.Mock
    ) -> None:
        mock_settings.OptimizerSettings.return_value = mock.Mock(verbose=False)
        mock_optimizer.ClickHouseOptimizer.return_value.run.side_effect = (
            RuntimeError('unexpected')
        )
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertEqual(ctx.exception.code, 1)

    @mock.patch('clickhouse_optimizer.cli.optimizer')
    @mock.patch('clickhouse_optimizer.cli.settings')
    def test_main_type_error(
        self, mock_settings: mock.Mock, mock_optimizer: mock.Mock
    ) -> None:
        mock_settings.OptimizerSettings.return_value = mock.Mock(verbose=False)
        mock_optimizer.ClickHouseOptimizer.return_value.run.side_effect = (
            TypeError('type mismatch')
        )
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertEqual(ctx.exception.code, 1)
