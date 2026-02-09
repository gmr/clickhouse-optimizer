# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Development
- `uv sync` - Install package with dev dependencies
- `uv run ruff check` - Run linting checks
- `uv run ruff format` - Format code (uses single quotes, 79 char line length)
- `uv run pre-commit run --all-files` - Run pre-commit hooks on all files
- `uv build` - Build package distributions

### Testing & Coverage
- `uv run coverage run -m pytest` - Run tests with coverage tracking
- `uv run coverage report` - Show coverage report in terminal
- `uv run coverage html` - Generate HTML coverage report

### Running the Tool
- `uvx clickhouse-optimizer --help` - Show CLI help and all options
- `uvx clickhouse-optimizer --verbose <table>` - Run with detailed logging
- Environment variables can be used for connection settings (CLICKHOUSE_HOST, CLICKHOUSE_USER, etc.)

## Architecture

This is a single-purpose CLI tool for optimizing ClickHouse table partitions incrementally. The architecture consists of three main components:

### Core Components

**Settings (`settings.py`)**
- Uses Pydantic with CLI argument parsing enabled
- Supports both CLI arguments and environment variables
- Enforces required fields and handles secure password storage
- Connection parameters, timeouts, and operational flags

**Optimizer (`optimizer.py`)**
- Main business logic for partition optimization
- Queries `system.parts`, `system.merges`, and `system.replication_queue` for partition discovery, merge monitoring, and pending merge detection
- Supports cluster-wide merge monitoring via `clusterAllReplicas()` for replicated tables
- Skips already-optimized single-part partitions automatically
- Supports date range filtering with `--min-date` / `--max-date`
- Implements timeout handling and progress tracking with Rich library
- Performs sequential partition optimization with timing metrics and ETA calculation

**CLI (`cli.py`)**
- Minimal entry point that configures logging and exception handling
- Uses Rich for formatted console output and error display
- Handles keyboard interrupts and common error scenarios gracefully

### Key Patterns

- **Configuration**: Uses Pydantic Settings with automatic CLI parsing - no manual argparse needed
- **Error Handling**: Graceful degradation when OPTIMIZE commands timeout (continues monitoring merges)
- **Progress Display**: Rich progress bars with spinners, timing, and completion estimates
- **Logging**: Structured logging with Rich handlers for console output formatting

The tool operates by discovering all active partitions for a table, then optimizing each partition sequentially while monitoring merge completion through ClickHouse system tables.
