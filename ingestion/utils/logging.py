"""Logging configuration and utilities for ingestion pipelines.

Provides structured logging setup with support for:
- Console and file outputs
- JSON-formatted structured logs
- Contextual fields (batch_id, table_name, etc.)
- dlt library integration (capturing dlt's standard Python logging)
- Performance metrics and timing
- Proper exception handling and tracebacks

Integration with dlt:
    dlt uses Python's standard logging module. This module bridges dlt's logs
    to loguru using an InterceptHandler pattern, allowing all pipeline logs
    (including dlt's internal logs) to be captured in a unified structured format.
"""

import json
import logging
import sys
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger

if TYPE_CHECKING:
    from loguru import Record
else:
    Record = Any

# Context variables for structured logging
_batch_id: ContextVar[str | None] = ContextVar("batch_id", default=None)
_table_name: ContextVar[str | None] = ContextVar("table_name", default=None)
_source_name: ContextVar[str | None] = ContextVar("source_name", default=None)


@dataclass
class LogContext:
    """Context information for structured logging."""

    batch_id: str | None = None
    table_name: str | None = None
    source_name: str | None = None

    def to_dict(self) -> dict:
        """Convert context to dictionary, excluding None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}


def _get_log_context() -> dict:
    """Retrieve current log context from context variables."""
    context = LogContext(
        batch_id=_batch_id.get(),
        table_name=_table_name.get(),
        source_name=_source_name.get(),
    )
    return context.to_dict()


class InterceptHandler(logging.Handler):
    """Bridge standard logging (used by dlt) to loguru.

    This handler intercepts all logs from dlt and other libraries using
    Python's standard logging module and routes them through loguru,
    allowing unified structured logging.

    Reference: https://loguru.readthedocs.io/en/stable/overview.html#better-integration-with-standard-logging-module
    """

    def emit(self, record: logging.LogRecord) -> None:
        """Convert a standard logging record to loguru format."""
        # Get the corresponding loguru level
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find the correct call frame to preserve stack information
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        # Log through loguru with proper depth and exception info
        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def _json_formatter(record: Record) -> str:
    """Format log records as JSON for structured logging.

    Includes context variables and standard fields.
    """
    log_entry = {
        "timestamp": record["time"].isoformat(),
        "level": record["level"].name,
        "message": record["message"],
        "module": record["name"],
        "line": record["line"],
        "function": record["function"],
        **_get_log_context(),
    }

    # Add exception information if present
    if record["exception"]:
        exc = record["exception"]
        log_entry["exception"] = {
            "type": getattr(exc.type, "__name__", "Unknown"),
            "value": str(exc.value),
        }

    return json.dumps(log_entry) + "\n"


def _console_formatter(record: Record) -> str:
    """Format log records for console output with colour and context.

    Includes context fields and proper exception formatting.
    """
    # Build base format with context
    context_parts = []
    ctx = _get_log_context()
    if ctx:
        context_str = " | ".join(f"{k}={v}" for k, v in ctx.items())
        context_parts.append(f"[{context_str}]")

    context_prefix = " ".join(context_parts)
    prefix = f"{context_prefix} " if context_prefix else ""

    # Format time and level
    time_str = record["time"].strftime("%Y-%m-%d %H:%M:%S")
    level = record["level"].name

    # Base message
    msg = f"{time_str} | {level:<8} | {prefix}{record['message']}"

    return msg + "\n"


def setup_logger(
    level: str = "INFO",
    json_output: bool = False,
    log_file: Path | None = None,
    console_output: bool = True,
    intercept_standard_logging: bool = True,
) -> None:
    """Initialise loguru logger with dlt integration and structured logging support.

    Configures both console and optional file logging with context-aware
    formatting. Supports JSON output for log aggregation systems.

    Bridges standard Python logging (used by dlt) to loguru for unified logging.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        json_output: If True, output JSON-formatted logs (for log aggregation).
                    If False, use human-readable console format.
        log_file: Optional path to write logs to file. If provided, logs are
                 written to both console and file.
        console_output: If True, logs are written to stderr. Set False if
                       only file output is desired.
        intercept_standard_logging: If True, intercept standard logging from
                                   dlt and other libraries using Python's
                                   logging module. Recommended for production.

    Example:
        >>> # Production setup with dlt integration and JSON output
        >>> from pathlib import Path
        >>> setup_logger(
        ...     level="INFO",
        ...     json_output=True,
        ...     log_file=Path(".dlt/logs/pipeline.log"),
        ...     intercept_standard_logging=True  # Capture dlt logs
        ... )
        >>>
        >>> # Development setup with human-readable output
        >>> setup_logger(level="DEBUG", console_output=True)
        >>>
        >>> # Use context variables for all subsequent logs
        >>> set_batch_id("batch_20250114_001")
        >>> set_table_name("customer_orders")
        >>> set_source_name("salesforce")
        >>>
        >>> from loguru import logger
        >>> logger.info("Processing started")
        >>> # Output: includes batch_id, table_name, source_name
        >>>
        >>> # dlt logs are automatically captured
        >>> import dlt
        >>> pipeline = dlt.pipeline(...)  # dlt logs flow through loguru
        >>>
        >>> # Clear context when switching batches
        >>> clear_context()
    """
    # Remove default loguru handler
    logger.remove()

    # Add console handler if requested
    if console_output:
        if json_output:
            # Use sink function for JSON output
            logger.add(
                lambda msg: sys.stderr.write(_json_formatter(msg.record) + "\n"),
                level=level,
                format="{message}",
                backtrace=True,
                diagnose=False,
            )
        else:
            # Use format function for console output
            logger.add(
                sys.stderr,
                level=level,
                format=_console_formatter,
                colorize=False,
                backtrace=True,
                diagnose=False,
            )

    # Add file handler if requested
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        if json_output:
            logger.add(
                log_file,
                level=level,
                format=lambda record: _json_formatter(record) + "\n",
                rotation="500 MB",
                retention="30 days",
                compression="gz",
                backtrace=True,
                diagnose=False,
            )
        else:
            logger.add(
                log_file,
                level=level,
                format=_console_formatter,
                rotation="500 MB",
                retention="30 days",
                compression="gz",
                backtrace=True,
                diagnose=False,
            )

    # Bridge standard logging to loguru (for dlt integration)
    if intercept_standard_logging:
        # Remove default standard logging handlers
        logging.root.handlers = []

        # Add the intercept handler to capture dlt logs
        logging.root.addHandler(InterceptHandler())

        # Set the root logger level to match our configured level
        level_num = getattr(logging, level.upper(), logging.INFO)
        logging.root.setLevel(level_num)

        # Suppress propagation to avoid duplicate logs
        logging.root.propagate = False


def set_batch_id(batch_id: str | None) -> None:
    """Set the batch ID for all subsequent log messages.

    Args:
        batch_id: Unique identifier for the current batch/load operation.
    """
    _batch_id.set(batch_id)


def set_table_name(table_name: str | None) -> None:
    """Set the table name for all subsequent log messages.

    Args:
        table_name: Name of the table being processed.
    """
    _table_name.set(table_name)


def set_source_name(source_name: str | None) -> None:
    """Set the source name for all subsequent log messages.

    Args:
        source_name: Name of the data source being ingested.
    """
    _source_name.set(source_name)


def clear_context() -> None:
    """Clear all context variables.

    Call this when transitioning between different batch operations
    to avoid log message pollution.
    """
    _batch_id.set(None)
    _table_name.set(None)
    _source_name.set(None)
