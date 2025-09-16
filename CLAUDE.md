# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Development
- `pip install -e '.[dev]'` - Install package in development mode with dev dependencies
- `ruff check` - Run linting checks
- `ruff format` - Format code (uses single quotes, 79 char line length)
- `pre-commit run --all-files` - Run pre-commit hooks on all files
- `python -m build` - Build package distributions

### Testing & Coverage
- `coverage run -m pytest` - Run tests with coverage tracking
- `coverage report` - Show coverage report in terminal
- `coverage html` - Generate HTML coverage report

### Running the Tool
- `clickhouse-optimizer --help` - Show CLI help and all options
- `clickhouse-optimizer --verbose <table>` - Run with detailed logging
- Environment variables can be used for connection settings (CH_HOST, CH_USER, etc.)

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
- Queries `system.parts` and `system.merges` tables for partition discovery and merge monitoring
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
