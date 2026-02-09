"""ClickHouse table optimization functionality."""

from __future__ import annotations

import dataclasses
import logging
import re
import time
import typing

import clickhouse_driver
from rich import console, progress

from clickhouse_optimizer import settings

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class ActiveMerge:
    progress: float
    elapsed: float


class ClickHouseOptimizer:
    """Handles optimization of ClickHouse tables."""

    def __init__(self, optimizer_settings: settings.OptimizerSettings) -> None:
        """Initialize the ClickHouse optimizer."""
        self.client = clickhouse_driver.Client(
            client_name='clickhouse-optimizer',
            host=optimizer_settings.host,
            port=optimizer_settings.port,
            secure=optimizer_settings.secure,
            user=optimizer_settings.user,
            password=optimizer_settings.password.get_secret_value(),
            database=optimizer_settings.database,
        )
        self.console = console.Console()
        self.optimize_timeout = optimizer_settings.optimize_timeout
        self.optimize_task: progress.TaskID | None = None
        self.poll_interval = optimizer_settings.poll_interval
        self.progress: progress.Progress = progress.Progress(
            progress.SpinnerColumn(),
            progress.TextColumn('[progress.description]{task.description}'),
            progress.MofNCompleteColumn(),
            progress.BarColumn(),
            progress.TaskProgressColumn(),
            progress.TimeElapsedColumn(),
            progress.TimeRemainingColumn(),
            get_time=time.time,
        )
        self.start_time: float | None = None
        self.table_name = optimizer_settings.table_name
        self.database = optimizer_settings.database
        self.min_date = optimizer_settings.min_date
        self.max_date = optimizer_settings.max_date
        self.cluster = optimizer_settings.cluster

    def run(self) -> None:
        partitions, skipped_count = self._get_table_partitions()
        if not partitions and not skipped_count:
            if self.min_date or self.max_date:
                date_filter = []
                if self.min_date:
                    date_filter.append(f'min_date >= {self.min_date}')
                if self.max_date:
                    date_filter.append(f'max_date <= {self.max_date}')
                raise ValueError(
                    f'No partitions found for table {self.table_name} '
                    f'with {" and ".join(date_filter)}'
                )
            LOGGER.warning(
                'No active partitions found for table %s', self.table_name
            )
            return

        total_partitions = len(partitions) + skipped_count
        self.optimize_task = self.progress.add_task(
            f'Optimizing {self.database}.{self.table_name}',
            total=total_partitions,
        )
        self.progress.start()
        self.start_time = time.time()
        if skipped_count:
            self.progress.update(self.optimize_task, completed=skipped_count)
        for partition in partitions:
            try:
                self._optimize_partition(
                    partition['partition_id'], partition['partition']
                )
            except TimeoutError:
                break
            self.progress.update(self.optimize_task, advance=1)
        self.progress.stop()

    @property
    def progress_tasks(self) -> dict[progress.TaskID, progress.Task]:
        return self.progress._tasks

    def _get_optimized_partitions(self) -> set[str]:
        """Get partitions with only 1 part, optimized already."""
        query = re.sub(
            r'\s+',
            ' ',
            """\
             SELECT partition_id
               FROM system.parts
              WHERE (active = 1)
                AND (database = %(database)s)
                AND (table = %(table_name)s)
           GROUP BY database, table, partition_id
             HAVING count(*) = 1""",
        )
        result = self.client.execute(
            query, {'database': self.database, 'table_name': self.table_name}
        )
        return {row[0] for row in result}

    def _get_table_partitions(self) -> tuple[list[dict[str, typing.Any]], int]:
        """Get all partitions excluding recent single-part ones.

        Returns:
            A tuple of (partitions_to_optimize, skipped_partition_count)
        """
        query = re.sub(
            r'\s+',
            ' ',
            """\
            SELECT DISTINCT partition_id, partition
              FROM system.parts
             WHERE (active = 1)
               AND (database = %(database)s)
               AND (table = %(table_name)s)
        """,
        )
        params: dict[str, typing.Any] = {
            'database': self.database,
            'table_name': self.table_name,
        }

        if self.min_date:
            query += ' AND (partition >= %(min_date)s)'
            params['min_date'] = self.min_date.isoformat()
            LOGGER.info(
                'Filtering partitions to min_date >= %s', self.min_date
            )

        if self.max_date:
            query += ' AND (partition <= %(max_date)s)'
            params['max_date'] = self.max_date.isoformat()
            LOGGER.info(
                'Filtering partitions to max_date <= %s', self.max_date
            )

        query += ' ORDER BY partition_id'

        result = self.client.execute(query, params)

        optimized = self._get_optimized_partitions()
        partitions_to_optimize = [
            {'partition_id': row[0], 'partition': row[1]}
            for row in result
            if row[0] not in optimized
        ]

        skipped_count = len(result) - len(partitions_to_optimize)
        if skipped_count:
            LOGGER.info(
                'Skipping %d already-optimized single-part partitions',
                skipped_count,
            )
            LOGGER.debug(
                'Skipped partitions: %s', ', '.join(sorted(optimized))
            )

        return partitions_to_optimize, skipped_count

    def _optimize_partition(self, partition_id: str, name: str) -> None:
        """Optimize a specific partition and wait for completion."""
        task = self.progress.add_task(
            f'Processing partition {name}', total=1, start=False
        )
        active_merge = self._is_table_busy(partition_id)
        if active_merge:
            start_time = time.time() - active_merge.elapsed
            self.progress_tasks[task].start_time = start_time
            if self.progress_tasks[self.optimize_task].start_time > start_time:
                self.progress_tasks[self.optimize_task].start_time = start_time

        self.progress.start_task(task)
        if not active_merge:
            query = re.sub(
                r'\s+',
                ' ',
                f"""\
                OPTIMIZE TABLE {self.database}.{self.table_name}
                  PARTITION ID '{partition_id}'
                         FINAL""",
            )
            try:
                self.client.execute(query)
            except TimeoutError:
                LOGGER.debug('Query timeout, polling for merges...')

            # Wait for merge to appear (replicated tables schedule async)
            for attempt in range(10):
                time.sleep(1)
                if self._is_table_busy(partition_id):
                    LOGGER.debug(
                        'Merge appeared after %d seconds for partition %s',
                        attempt + 1,
                        partition_id,
                    )
                    break
            else:
                LOGGER.warning(
                    'Merge did not appear after OPTIMIZE for partition %s',
                    partition_id,
                )

        elapsed = time.time() - self.start_time
        while elapsed < self.optimize_timeout:
            busy = self._is_table_busy(partition_id)
            if not busy:
                break
            if busy.progress > 0:
                self.progress.update(task, completed=busy.progress)
            time.sleep(self.poll_interval)
            elapsed = time.time() - self.start_time

        if elapsed >= self.optimize_timeout:
            raise TimeoutError(
                f'Optimization did not complete within '
                f'{self.optimize_timeout}s'
            )
        self.progress.remove_task(task)

    def _get_active_merge(self, partition_id: str = '') -> ActiveMerge | None:
        """Check for active merges on a table or specific partition."""
        if self.cluster:
            query = re.sub(
                r'\s+',
                ' ',
                """\
                SELECT database, table, partition_id, progress, elapsed
                  FROM clusterAllReplicas(%(cluster)s, system.merges)
                 WHERE database = %(database)s
                   AND table = %(table_name)s""",
            )
            params: dict[str, typing.Any] = {
                'cluster': self.cluster,
                'database': self.database,
                'table_name': self.table_name,
            }
        else:
            query = re.sub(
                r'\s+',
                ' ',
                """\
                SELECT database, table, partition_id, progress, elapsed
                  FROM system.merges
                 WHERE database = %(database)s
                   AND table = %(table_name)s""",
            )
            params = {'database': self.database, 'table_name': self.table_name}

        if partition_id:
            query += ' AND partition_id = %(partition_id)s'
            params['partition_id'] = partition_id

        result = self.client.execute(query, params)
        for row in result:
            if (
                row[0] == self.database
                and row[1] == self.table_name
                and (not partition_id or row[2] == partition_id)
            ):
                return ActiveMerge(progress=row[3], elapsed=row[4])
        return None

    def _has_pending_merge(self, partition_id: str = '') -> bool:
        """Check for pending OPTIMIZE/MERGE entries in replication queue."""
        if self.cluster:
            query = re.sub(
                r'\s+',
                ' ',
                """\
                SELECT 1
                  FROM clusterAllReplicas(
                           %(cluster)s, system.replication_queue)
                 WHERE database = %(database)s
                   AND table = %(table_name)s
                   AND type = 'MERGE_PARTS'""",
            )
            params: dict[str, typing.Any] = {
                'cluster': self.cluster,
                'database': self.database,
                'table_name': self.table_name,
            }
        else:
            query = re.sub(
                r'\s+',
                ' ',
                """\
                SELECT 1
                  FROM system.replication_queue
                 WHERE database = %(database)s
                   AND table = %(table_name)s
                   AND type = 'MERGE_PARTS'""",
            )
            params = {'database': self.database, 'table_name': self.table_name}

        if partition_id:
            query += ' AND new_part_name LIKE %(partition_pattern)s'
            params['partition_pattern'] = f'{partition_id}%'

        query += ' LIMIT 1'

        result = self.client.execute(query, params)
        return len(result) > 0

    def _is_table_busy(self, partition_id: str = '') -> ActiveMerge | None:
        """Check if table has any active or pending merges."""
        active = self._get_active_merge(partition_id)
        if active:
            return active

        if self._has_pending_merge(partition_id):
            return ActiveMerge(progress=0.0, elapsed=0.0)

        return None
