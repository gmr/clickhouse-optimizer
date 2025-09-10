"""ClickHouse table optimization functionality."""

from __future__ import annotations

import datetime
import logging
import pathlib
import re
import time
import typing

import clickhouse_driver
from clickhouse_driver import errors
from rich import console, progress

from clickhouse_optimizer import settings

LOGGER = logging.getLogger(__name__)


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
        self.dry_run = optimizer_settings.dry_run
        self.optimize_timeout = optimizer_settings.optimize_timeout
        self.poll_interval = optimizer_settings.poll_interval
        self.table_name = optimizer_settings.table_name
        self.checkpoint_file = (
            pathlib.Path(optimizer_settings.checkpoint_file)
            if optimizer_settings.checkpoint_file
            else None
        )
        self.database = optimizer_settings.database

    def get_recent_single_part_partitions(self) -> set[str]:
        """Get partitions with only 1 part modified within last 30 days."""
        query = re.sub(
            r'\s+',
            ' ',
            """\
        SELECT
            database,
            table,
            partition_id,
            count(*) AS part_count,
            max(modification_time) AS latest_part_time
        FROM system.parts
        WHERE (active = 1)
          AND (table = %(table_name)s)
          AND (database = %(database)s)
        GROUP BY database, table, partition_id
        HAVING part_count = 1
          AND latest_part_time > (now() - INTERVAL 30 DAY)
        ORDER BY latest_part_time DESC
        """,
        )
        result = self.client.execute(
            query, {'table_name': self.table_name, 'database': self.database}
        )
        return {row[2] for row in result}  # partition_id is at index 2

    def get_table_partitions(self) -> list[dict[str, typing.Any]]:
        """Get all partitions excluding recent single-part ones."""
        query = re.sub(
            r'\s+',
            ' ',
            """\
        SELECT DISTINCT partition_id, partition
          FROM system.parts
         WHERE table = %(table_name)s
           AND active = 1
         ORDER BY partition_id
        """,
        )
        result = self.client.execute(query, {'table_name': self.table_name})

        # Get partitions to exclude
        excluded_partitions = self.get_recent_single_part_partitions()
        if excluded_partitions:
            LOGGER.info(
                'Excluding %d recent single-part partitions: %s',
                len(excluded_partitions),
                ', '.join(sorted(excluded_partitions)),
            )

        # Filter out excluded partitions
        return [
            {'partition_id': row[0], 'partition': row[1]}
            for row in result
            if row[0] not in excluded_partitions
        ]

    def check_active_merges(
        self, partition_id: str = ''
    ) -> list[dict[str, typing.Any]]:
        """Check for active merges on a table or specific partition."""
        query = re.sub(
            r'\s+',
            ' ',
            """\
        SELECT partition_id, partition, progress, elapsed
          FROM system.merges
         WHERE table = %(table_name)s
        """,
        )
        params = {'table_name': self.table_name}

        if partition_id:
            query += ' AND partition_id = %(partition_id)s'
            params['partition_id'] = partition_id

        result = self.client.execute(query, params)
        return [
            {
                'partition_id': row[0],
                'partition': row[1],
                'progress': row[2],
                'elapsed': row[3],
            }
            for row in result
        ]

    def wait_for_merges_completion(self, partition_id: str = '') -> None:
        """Wait for merges to complete on a table or partition."""
        start_time = time.time()

        while True:
            active_merges = self.check_active_merges(partition_id)

            if not active_merges:
                LOGGER.debug(
                    'No active merges found for %s:%s',
                    self.table_name,
                    partition_id,
                )
                break

            elapsed = time.time() - start_time
            if elapsed > self.optimize_timeout:
                raise TimeoutError(
                    f'Merges did not complete within {self.optimize_timeout}s '
                    f'for {self.table_name}:{partition_id}'
                )

            # Log merge progress - use console if available to avoid
            # interfering with progress bars
            for merge in active_merges:
                msg = (
                    f'Merge in progress for partition {merge["partition"]} '
                    f'(ID: {merge["partition_id"]}) - '
                    f'{merge["progress"]:.1f}% '
                    f'({merge["elapsed"]:.1f}s elapsed)'
                )
                if (
                    hasattr(self, '_progress_console')
                    and self._progress_console
                ):
                    self._progress_console.log(msg)
                else:
                    LOGGER.info(msg)

            LOGGER.debug(
                'Waiting %ss before next merge check...', self.poll_interval
            )
            time.sleep(self.poll_interval)

    def optimize_partition(self, partition_id: str) -> None:
        """Optimize a specific partition and wait for completion."""
        query = re.sub(
            r'\s+',
            ' ',
            f"""\
        OPTIMIZE TABLE {self.table_name}
          PARTITION ID '{partition_id}'
                 FINAL""",
        )
        if self.dry_run:
            LOGGER.info('Would execute: %s', query)
            LOGGER.info(
                'Would wait for merges to complete on partition %s',
                partition_id,
            )
        else:
            LOGGER.debug('Executing: %s', query)
            try:
                # Execute the optimize command - this may timeout but that's OK
                # We'll poll for merge completion regardless
                self.client.execute(query)
            except (errors.Error, OSError) as exc:
                # Log the error but continue - the merge may still be running
                LOGGER.warning(
                    'OPTIMIZE command may have timed out for partition %s: %s',
                    partition_id,
                    exc,
                )
                LOGGER.info('Continuing to monitor merge progress...')

            # Wait for any merges on this partition to complete
            msg = (
                f'Waiting for merges to complete on partition '
                f'{partition_id}...'
            )
            if hasattr(self, '_progress_console') and self._progress_console:
                self._progress_console.log(msg)
            else:
                LOGGER.info(msg)

            self.wait_for_merges_completion(partition_id)

            msg = f'Partition {partition_id} optimization complete'
            if hasattr(self, '_progress_console') and self._progress_console:
                self._progress_console.log(msg)
            else:
                LOGGER.info(msg)

    def load_checkpoint(self) -> set[str]:
        """Load completed partitions from checkpoint file."""
        if not self.checkpoint_file or not self.checkpoint_file.exists():
            return set()

        try:
            completed = set()
            with self.checkpoint_file.open('r', encoding='ascii') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        completed.add(line)
            LOGGER.info(
                'Loaded %d completed partitions from checkpoint: %s',
                len(completed),
                self.checkpoint_file,
            )
            return completed
        except (OSError, UnicodeDecodeError) as exc:
            LOGGER.warning(
                'Failed to load checkpoint file %s: %s',
                self.checkpoint_file,
                exc,
            )
            return set()

    def save_partition_to_checkpoint(self, partition_id: str) -> None:
        """Save a completed partition to the checkpoint file."""
        if not self.checkpoint_file:
            return

        try:
            # Ensure directory exists
            self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)

            # Append partition ID with timestamp comment
            timestamp = datetime.datetime.now(datetime.UTC).strftime(
                '%Y-%m-%d %H:%M:%S'
            )
            with self.checkpoint_file.open('a', encoding='ascii') as f:
                f.write(f'{partition_id}  # completed at {timestamp}\n')

            LOGGER.debug('Saved partition %s to checkpoint file', partition_id)
        except OSError as exc:
            LOGGER.warning(
                'Failed to save partition %s to checkpoint file: %s',
                partition_id,
                exc,
            )

    def optimize_table(self) -> None:
        LOGGER.info('Starting optimization of table: %s', self.table_name)
        partitions = self.get_table_partitions()

        if not partitions:
            LOGGER.warning(
                'No active partitions found for table %s', self.table_name
            )
            return

        # Load checkpoint to filter out already completed partitions
        completed_partitions = self.load_checkpoint()
        if completed_partitions:
            partitions = [
                p
                for p in partitions
                if p['partition_id'] not in completed_partitions
            ]
            LOGGER.info(
                'Filtered out %d already completed partitions',
                len(self.get_table_partitions()) - len(partitions),
            )

        if not partitions:
            LOGGER.info('All partitions already completed')
            return

        LOGGER.info('Found %s partitions to optimize', len(partitions))

        # Optimize each partition
        start_time = time.time()
        partition_times = []

        with progress.Progress(
            progress.SpinnerColumn(),
            progress.TextColumn('[progress.description]{task.description}'),
            progress.BarColumn(),
            progress.TaskProgressColumn(),
            progress.TimeElapsedColumn(),
            progress.TimeRemainingColumn(),
            console=self.console,
        ) as prog:
            # Store progress console reference for logging during
            # progress display
            self._progress_console = prog.console
            task = prog.add_task(
                'Optimizing partitions...', total=len(partitions)
            )

            for i, partition in enumerate(partitions):
                partition_id = partition['partition_id']
                partition_value = partition['partition']
                partition_start = time.time()

                prog.update(
                    task,
                    description=(
                        f'Optimizing partition {partition_value} '
                        f'(ID: {partition_id})'
                    ),
                )

                try:
                    self.optimize_partition(partition_id)
                    partition_end = time.time()
                    partition_duration = partition_end - partition_start
                    partition_times.append(partition_duration)

                    # Save to checkpoint
                    self.save_partition_to_checkpoint(partition_id)

                    # Log timing info with ETA
                    avg_time = sum(partition_times) / len(partition_times)
                    remaining_partitions = len(partitions) - (i + 1)
                    eta_seconds = remaining_partitions * avg_time

                    msg = (
                        f'Partition {partition_id} done in '
                        f'{partition_duration:.1f}s '
                        f'(avg/ETA: {avg_time:.1f}s/{eta_seconds:.1f}s)'
                    )
                    if (
                        hasattr(self, '_progress_console')
                        and self._progress_console
                    ):
                        self._progress_console.log(msg)
                    else:
                        LOGGER.info(msg)

                    prog.advance(task)
                except Exception as exc:
                    LOGGER.error(
                        'Failed to optimize partition %s: %s',
                        partition_id,
                        exc,
                    )
                    raise

        # Clear progress console reference
        self._progress_console = None

        total_time = time.time() - start_time
        action = 'Would optimize' if self.dry_run else 'Optimized'
        LOGGER.info(
            '%s %s partitions for table %s in %.1fs',
            action,
            len(partitions),
            self.table_name,
            total_time,
        )
