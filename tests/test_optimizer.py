"""Tests for the optimizer module."""

from __future__ import annotations

import datetime
import unittest
from unittest import mock

import pydantic

from clickhouse_optimizer import optimizer, settings


def _create_settings(**overrides: object) -> settings.OptimizerSettings:
    """Create test OptimizerSettings bypassing CLI parsing."""
    defaults = {
        'host': 'localhost',
        'port': 9000,
        'database': 'default',
        'secure': False,
        'user': 'default',
        'password': pydantic.SecretStr('secret'),
        'optimize_timeout': 3600,
        'poll_interval': 1,
        'verbose': False,
        'cluster': None,
        'min_date': None,
        'max_date': None,
        'table_name': 'test_table',
    }
    defaults.update(overrides)
    return settings.OptimizerSettings.model_construct(**defaults)


class TestActiveMerge(unittest.TestCase):
    """Tests for the ActiveMerge dataclass."""

    def test_create(self) -> None:
        merge = optimizer.ActiveMerge(progress=0.5, elapsed=10.0)
        self.assertEqual(merge.progress, 0.5)
        self.assertEqual(merge.elapsed, 10.0)


class TestClickHouseOptimizerInit(unittest.TestCase):
    """Tests for ClickHouseOptimizer initialization."""

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_init_creates_client(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        mock_driver.Client.assert_called_once_with(
            client_name='clickhouse-optimizer',
            host='localhost',
            port=9000,
            secure=False,
            user='default',
            password='secret',  # noqa: S106
            database='default',
        )
        self.assertEqual(opt.table_name, 'test_table')
        self.assertEqual(opt.database, 'default')
        self.assertEqual(opt.optimize_timeout, 3600)
        self.assertEqual(opt.poll_interval, 1)
        self.assertIsNone(opt.cluster)
        self.assertIsNone(opt.min_date)
        self.assertIsNone(opt.max_date)

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_init_with_cluster(self, mock_driver: mock.Mock) -> None:
        s = _create_settings(cluster='my_cluster')
        opt = optimizer.ClickHouseOptimizer(s)
        self.assertEqual(opt.cluster, 'my_cluster')

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_init_with_dates(self, mock_driver: mock.Mock) -> None:
        s = _create_settings(
            min_date=datetime.date(2024, 1, 1),
            max_date=datetime.date(2025, 1, 1),
        )
        opt = optimizer.ClickHouseOptimizer(s)
        self.assertEqual(opt.min_date, datetime.date(2024, 1, 1))
        self.assertEqual(opt.max_date, datetime.date(2025, 1, 1))


class TestGetTablePartitions(unittest.TestCase):
    """Tests for _get_table_partitions."""

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_returns_partitions(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [
            [('p1', '2024-01'), ('p2', '2024-02'), ('p3', '2024-03')],
            [('p1',)],  # _get_optimized_partitions result
        ]
        partitions, skipped = opt._get_table_partitions()
        self.assertEqual(len(partitions), 2)
        self.assertEqual(skipped, 1)
        self.assertEqual(partitions[0]['partition_id'], 'p2')
        self.assertEqual(partitions[1]['partition_id'], 'p3')

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_all_optimized(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [[('p1', '2024-01')], [('p1',)]]
        partitions, skipped = opt._get_table_partitions()
        self.assertEqual(len(partitions), 0)
        self.assertEqual(skipped, 1)

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_none_optimized(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [
            [('p1', '2024-01'), ('p2', '2024-02')],
            [],
        ]
        partitions, skipped = opt._get_table_partitions()
        self.assertEqual(len(partitions), 2)
        self.assertEqual(skipped, 0)

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_with_min_date(self, mock_driver: mock.Mock) -> None:
        s = _create_settings(min_date=datetime.date(2024, 6, 1))
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [
            [('p1', '2024-06'), ('p2', '2024-07')],
            [],
        ]
        partitions, skipped = opt._get_table_partitions()
        self.assertEqual(len(partitions), 2)
        call_args = opt.client.execute.call_args_list[0]
        self.assertIn('min_date', call_args[0][1])

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_with_max_date(self, mock_driver: mock.Mock) -> None:
        s = _create_settings(max_date=datetime.date(2024, 12, 31))
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [[('p1', '2024-06')], []]
        partitions, skipped = opt._get_table_partitions()
        self.assertEqual(len(partitions), 1)
        call_args = opt.client.execute.call_args_list[0]
        self.assertIn('max_date', call_args[0][1])


class TestGetOptimizedPartitions(unittest.TestCase):
    """Tests for _get_optimized_partitions."""

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_returns_partition_set(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = [('p1',), ('p2',)]
        result = opt._get_optimized_partitions()
        self.assertEqual(result, {'p1', 'p2'})

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_returns_empty_set(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = []
        result = opt._get_optimized_partitions()
        self.assertEqual(result, set())


class TestGetActiveMerge(unittest.TestCase):
    """Tests for _get_active_merge."""

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_no_active_merge(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = []
        result = opt._get_active_merge('p1')
        self.assertIsNone(result)

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_active_merge_found(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = [
            ('default', 'test_table', 'p1', 0.5, 30.0)
        ]
        result = opt._get_active_merge('p1')
        self.assertIsNotNone(result)
        self.assertEqual(result.progress, 0.5)
        self.assertEqual(result.elapsed, 30.0)

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_active_merge_wrong_table(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = [
            ('default', 'other_table', 'p1', 0.5, 30.0)
        ]
        result = opt._get_active_merge('p1')
        self.assertIsNone(result)

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_active_merge_wrong_database(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = [
            ('other_db', 'test_table', 'p1', 0.5, 30.0)
        ]
        result = opt._get_active_merge('p1')
        self.assertIsNone(result)

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_active_merge_wrong_partition(
        self, mock_driver: mock.Mock
    ) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = [
            ('default', 'test_table', 'p2', 0.5, 30.0)
        ]
        result = opt._get_active_merge('p1')
        self.assertIsNone(result)

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_active_merge_no_partition_filter(
        self, mock_driver: mock.Mock
    ) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = [
            ('default', 'test_table', 'p1', 0.8, 60.0)
        ]
        result = opt._get_active_merge()
        self.assertIsNotNone(result)
        self.assertEqual(result.progress, 0.8)

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_cluster_query(self, mock_driver: mock.Mock) -> None:
        s = _create_settings(cluster='my_cluster')
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = [
            ('default', 'test_table', 'p1', 0.5, 30.0)
        ]
        result = opt._get_active_merge('p1')
        self.assertIsNotNone(result)
        query = opt.client.execute.call_args[0][0]
        self.assertIn('clusterAllReplicas', query)


class TestHasPendingMerge(unittest.TestCase):
    """Tests for _has_pending_merge."""

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_no_pending_merge(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = []
        self.assertFalse(opt._has_pending_merge('p1'))

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_has_pending_merge(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = [(1,)]
        self.assertTrue(opt._has_pending_merge('p1'))

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_no_partition_filter(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = [(1,)]
        self.assertTrue(opt._has_pending_merge())
        query = opt.client.execute.call_args[0][0]
        self.assertNotIn('partition_pattern', query)

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_cluster_query(self, mock_driver: mock.Mock) -> None:
        s = _create_settings(cluster='my_cluster')
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = []
        opt._has_pending_merge('p1')
        query = opt.client.execute.call_args[0][0]
        self.assertIn('clusterAllReplicas', query)


class TestIsTableBusy(unittest.TestCase):
    """Tests for _is_table_busy."""

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_not_busy(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = []
        result = opt._is_table_busy('p1')
        self.assertIsNone(result)

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_active_merge(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.return_value = [
            ('default', 'test_table', 'p1', 0.5, 30.0)
        ]
        result = opt._is_table_busy('p1')
        self.assertIsNotNone(result)
        self.assertEqual(result.progress, 0.5)

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_pending_merge_only(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [
            [],  # _get_active_merge: no active merge
            [(1,)],  # _has_pending_merge: pending merge
        ]
        result = opt._is_table_busy('p1')
        self.assertIsNotNone(result)
        self.assertEqual(result.progress, 0.0)
        self.assertEqual(result.elapsed, 0.0)


class TestRun(unittest.TestCase):
    """Tests for run method."""

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_no_partitions_no_date_filter(
        self, mock_driver: mock.Mock
    ) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [
            [],  # _get_table_partitions
            [],  # _get_optimized_partitions
        ]
        opt.run()  # Should log warning and return

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_no_partitions_with_min_date_raises(
        self, mock_driver: mock.Mock
    ) -> None:
        s = _create_settings(min_date=datetime.date(2024, 1, 1))
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [[], []]
        with self.assertRaises(ValueError) as ctx:
            opt.run()
        self.assertIn('No partitions found', str(ctx.exception))
        self.assertIn('min_date', str(ctx.exception))

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_no_partitions_with_max_date_raises(
        self, mock_driver: mock.Mock
    ) -> None:
        s = _create_settings(max_date=datetime.date(2025, 1, 1))
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [[], []]
        with self.assertRaises(ValueError) as ctx:
            opt.run()
        self.assertIn('No partitions found', str(ctx.exception))
        self.assertIn('max_date', str(ctx.exception))

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_no_partitions_with_both_dates_raises(
        self, mock_driver: mock.Mock
    ) -> None:
        s = _create_settings(
            min_date=datetime.date(2024, 1, 1),
            max_date=datetime.date(2025, 1, 1),
        )
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [[], []]
        with self.assertRaises(ValueError) as ctx:
            opt.run()
        error_msg = str(ctx.exception)
        self.assertIn('min_date', error_msg)
        self.assertIn('max_date', error_msg)

    @mock.patch('clickhouse_optimizer.optimizer.time.sleep')
    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_optimizes_single_partition(
        self, mock_driver: mock.Mock, mock_sleep: mock.Mock
    ) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [
            [('p1', '2024-01')],  # _get_table_partitions
            [],  # _get_optimized_partitions
            # _is_table_busy (before optimize): not busy
            [],  # _get_active_merge
            [],  # _has_pending_merge
            None,  # OPTIMIZE TABLE
            # wait for merge to appear (10 iterations, all empty)
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            # while loop: not busy
            [],  # _get_active_merge
            [],  # _has_pending_merge
        ]
        opt.progress.disable = True
        opt.run()

    @mock.patch('clickhouse_optimizer.optimizer.time.sleep')
    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_skipped_partitions_counted(
        self, mock_driver: mock.Mock, mock_sleep: mock.Mock
    ) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [
            [('p1', '2024-01'), ('p2', '2024-02')],
            [('p1',)],  # p1 already optimized
            # _is_table_busy for p2
            [],
            [],
            None,  # OPTIMIZE TABLE
            # wait for merge
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            # while loop: not busy
            [],
            [],
        ]
        opt.progress.disable = True
        opt.run()

    @mock.patch('clickhouse_optimizer.optimizer.time.sleep')
    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_timeout_breaks_loop(
        self, mock_driver: mock.Mock, mock_sleep: mock.Mock
    ) -> None:
        s = _create_settings(optimize_timeout=0)
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [
            [('p1', '2024-01')],
            [],
            # _is_table_busy (before optimize): not busy
            [],
            [],
            None,  # OPTIMIZE TABLE
            # wait for merge
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
        ]
        opt.progress.disable = True
        # With optimize_timeout=0, the while loop in
        # _optimize_partition exits immediately and raises
        # TimeoutError, which run() catches and breaks
        opt.run()

    @mock.patch('clickhouse_optimizer.optimizer.time.sleep')
    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_active_merge_on_entry(
        self, mock_driver: mock.Mock, mock_sleep: mock.Mock
    ) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [
            [('p1', '2024-01')],
            [],
            # _is_table_busy for p1 (before optimize): active
            [('default', 'test_table', 'p1', 0.5, 30.0)],
            # while loop: not busy anymore
            [],
            [],
        ]
        opt.progress.disable = True
        opt.run()

    @mock.patch('clickhouse_optimizer.optimizer.time.sleep')
    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_merge_busy_with_progress(
        self, mock_driver: mock.Mock, mock_sleep: mock.Mock
    ) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [
            [('p1', '2024-01')],
            [],
            # _is_table_busy (before optimize): not busy
            [],
            [],
            None,  # OPTIMIZE TABLE
            # wait for merge to appear - merge appears on attempt 1
            [('default', 'test_table', 'p1', 0.1, 1.0)],
            # while loop: busy with progress
            [('default', 'test_table', 'p1', 0.5, 5.0)],
            # while loop: not busy
            [],
            [],
        ]
        opt.progress.disable = True
        opt.run()

    @mock.patch('clickhouse_optimizer.optimizer.time.sleep')
    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_optimize_query_timeout(
        self, mock_driver: mock.Mock, mock_sleep: mock.Mock
    ) -> None:
        """Test that query timeout during OPTIMIZE is handled."""
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [
            [('p1', '2024-01')],
            [],
            [],  # _get_active_merge
            [],  # _has_pending_merge
            TimeoutError('query timeout'),  # OPTIMIZE TABLE
            # wait for merge to appear
            [('default', 'test_table', 'p1', 0.1, 1.0)],
            # while loop: not busy
            [],
            [],
        ]
        opt.progress.disable = True
        opt.run()

    @mock.patch('clickhouse_optimizer.optimizer.time.sleep')
    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_active_merge_adjusts_start_time(
        self, mock_driver: mock.Mock, mock_sleep: mock.Mock
    ) -> None:
        """Test that active merge elapsed time adjusts task start."""
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        opt.client.execute.side_effect = [
            [('p1', '2024-01')],
            [],
            # _is_table_busy: active merge with 60s elapsed
            [('default', 'test_table', 'p1', 0.5, 60.0)],
            # while loop: not busy
            [],
            [],
        ]
        opt.progress.disable = True
        opt.run()
        # The start_time adjustment should have been applied
        self.assertIsNotNone(opt.start_time)


class TestProgressTasks(unittest.TestCase):
    """Tests for progress_tasks property."""

    @mock.patch('clickhouse_optimizer.optimizer.clickhouse_driver')
    def test_returns_tasks_dict(self, mock_driver: mock.Mock) -> None:
        s = _create_settings()
        opt = optimizer.ClickHouseOptimizer(s)
        self.assertIsInstance(opt.progress_tasks, dict)
