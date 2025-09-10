"""ClickHouse optimizer CLI application."""

from __future__ import annotations

import logging
import sys

from rich import console
from rich import logging as rich_logging

from clickhouse_optimizer import optimizer, settings


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the application."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(message)s',
        handlers=[rich_logging.RichHandler(rich_tracebacks=True)],
    )


def main() -> None:
    """Main entry point."""
    optimizer_settings = settings.OptimizerSettings()
    setup_logging(optimizer_settings.verbose)
    rich_console = console.Console()
    ch_optimizer = optimizer.ClickHouseOptimizer(optimizer_settings)
    try:
        ch_optimizer.optimize_table()
    except (KeyboardInterrupt, SystemExit):
        rich_console.print('[yellow]Operation cancelled[/yellow]')
        sys.exit(130)
    except (OSError, ValueError, TypeError, RuntimeError) as e:
        rich_console.print(f'[red]Error: {e}[/red]')
        sys.exit(1)


if __name__ == '__main__':
    main()
