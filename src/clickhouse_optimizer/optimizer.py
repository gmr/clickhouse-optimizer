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

    def run(self) -> None:
        partitions = self._get_table_partitions()
        if not partitions:
            LOGGER.warning(
                'No active partitions found for table %s', self.table_name
            )
            return
        self.optimize_task = self.progress.add_task(
            f'Optimizing {self.database}.{self.table_name}',
            total=len(partitions),
        )
        self.progress.start()
        self.start_time = time.time()
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

    def _get_table_partitions(self) -> list[dict[str, typing.Any]]:
        """Get all partitions excluding recent single-part ones."""
        query = re.sub(
            r'\s+',
            ' ',
            """\
            SELECT DISTINCT partition_id, partition
              FROM system.parts
             WHERE (active = 1)
               AND (database = %(database)s)
               AND (table = %(table_name)s)
          ORDER BY partition_id
        """,
        )
        result = self.client.execute(
            query, {'database': self.database, 'table_name': self.table_name}
        )

        optimized = self._get_optimized_partitions()
        if optimized:
            LOGGER.debug(
                'Excluding %d recent single-part partitions: %s',
                len(optimized),
                ', '.join(sorted(optimized)),
            )

        return [
            {'partition_id': row[0], 'partition': row[1]}
            for row in result
            if row[0] not in optimized
        ]

    def _optimize_partition(self, partition_id: str, name: str) -> None:
        """Optimize a specific partition and wait for completion."""
        task = self.progress.add_task(
            f'Processing partition {name}', total=1, start=False
        )
        active_merge = self._get_active_merge(partition_id)
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

        elapsed = time.time() - self.start_time
        while elapsed < self.optimize_timeout:
            active_merge = self._get_active_merge(partition_id)
            if not active_merge:
                break
            self.progress.update(task, completed=active_merge.progress)
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
                and row[2] == partition_id
            ):
                return ActiveMerge(progress=row[3], elapsed=row[4])
        return None
