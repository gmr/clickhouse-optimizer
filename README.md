# ClickHouse Optimizer

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD_3--Clause-yellow.svg)](https://opensource.org/licenses/BSD-3-Clause)

A robust CLI tool for incrementally optimizing all partitions of ClickHouse tables. Optimize large tables efficiently by processing partitions sequentially with comprehensive progress tracking and merge monitoring.

## 🚀 Why ClickHouse Optimizer?

ClickHouse tables can accumulate many small parts over time, degrading query performance. The `OPTIMIZE TABLE` command typically processes all partitions at once, which can:

- **Overwhelm system resources** on large tables
- **Block other operations** during optimization
- **Fail on timeout** without completing any work
- **Provide no visibility** into progress

If you generate a file with a list of partitions to optimize, you can run the `OPTIMIZE TABLE` command manually. However, this approach is error-prone, failing when the `OPTIMIZE TABLE` command times out, and can be time-consuming.

This tool solves these problems by:

- ✅ **Processing partitions sequentially** to manage resource usage
- ✅ **Monitoring merge completion** with real-time progress bars
- ✅ **Graceful timeout handling** - continues with next partition if one times out
- ✅ **Rich console output** with ETA calculations

## 📦 Installation

```bash
pip install clickhouse-optimizer
```

For development:

```bash
git clone https://github.com/gmr/clickhouse-optimizer
cd clickhouse-optimizer
pip install -e '.[dev]'
```

## 🔧 Quick Start

### Basic Usage

```bash
# Optimize all partitions of a table
clickhouse-optimizer --host ch.example.com --user admin --password secret --database mydb mytable

# Run with verbose logging to see detailed progress
clickhouse-optimizer --verbose --host ch.example.com --user admin --password secret --database mydb mytable
```

### Environment Variables

Set connection parameters via environment variables:

```bash
export CH_HOST=ch.example.com
export CH_USER=admin
export CH_PASSWORD=secret
export CH_DATABASE=mydb

clickhouse-optimizer mytable
```

### Advanced Options

```bash
# Custom timeouts and polling intervals
clickhouse-optimizer \
  --optimize-timeout 7200 \
  --poll-interval 10 \
  mytable
```

## 📋 Command Reference

| Option | Environment Variable | Description | Default |
|--------|---------------------|-------------|---------|
| `--host` | `CH_HOST` | ClickHouse server hostname | Required |
| `--port` | `CH_PORT` | ClickHouse server port | 9440 |
| `--database` | `CH_DATABASE` | Database name | Required |
| `--user` | `CH_USER` | Username for authentication | Required |
| `--password` | `CH_PASSWORD` | Password for authentication | Required |
| `--secure` | `CH_SECURE` | Use secure connection | False |
| `--verbose` | `CH_VERBOSE` | Enable verbose logging | False |
| `--optimize-timeout` | `CH_OPTIMIZE_TIMEOUT` | Max seconds to wait for merges | 43200 (12h) |
| `--poll-interval` | `CH_POLL_INTERVAL` | Seconds between status checks | 5 |

## 🏗️ How It Works

1. **Discovery**: Queries `system.parts` to find all active partitions
2. **Sequential Processing**: Optimizes one partition at a time
3. **Merge Monitoring**: Tracks merge progress via `system.merges`
4. **Progress Display**: Shows completion status with Rich progress bars
5. **Fault Tolerance**: Continues with next partition if one times out

## 🎯 Use Cases

### Large Production Tables
Perfect for tables with hundreds of partitions where full optimization would be too resource-intensive.

### Maintenance Windows
Ideal for scheduled optimization during low-traffic periods with predictable progress tracking.

### Performance Recovery
Quickly improve query performance on tables with many small parts without system overload.

### Safe Operations
Monitor optimization progress with detailed logging and progress tracking.

## 🛠️ Development

### Setup Development Environment

```bash
# Install with development dependencies
pip install -e '.[dev]'

# Install pre-commit hooks
pre-commit install
```

### Code Quality

```bash
# Run linting
ruff check

# Format code
ruff format

# Run all pre-commit hooks
pre-commit run --all-files
```

### Testing

```bash
# Run tests with coverage
coverage run -m pytest

# Show coverage report
coverage report

# Generate HTML coverage report
coverage html
```

### Build

```bash
# Build package distributions
python -m build
```

## 📄 License

BSD 3-Clause License - see [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run the pre-commit hooks
5. Submit a pull request

## 📊 Architecture

This tool follows a clean, modular architecture:

- **Settings**: Pydantic-based configuration with automatic CLI parsing
- **Optimizer**: Core business logic with progress tracking and error handling
- **CLI**: Minimal entry point with Rich console formatting

The optimizer discovers partitions, processes them sequentially, and monitors merge completion through ClickHouse system tables.
