"""Tests for logging utilities."""

import logging

import pytest
from loguru import logger

from ingestion.utils.logging import (
    _get_log_context,
    clear_context,
    set_batch_id,
    set_source_name,
    set_table_name,
    setup_logger,
)


def test_logging_context():
    """Test setting and clearing logging context."""
    clear_context()
    assert _get_log_context() == {}

    set_batch_id("B1")
    set_table_name("T1")
    set_source_name("S1")

    ctx = _get_log_context()
    assert ctx["batch_id"] == "B1"
    assert ctx["table_name"] == "T1"
    assert ctx["source_name"] == "S1"

    clear_context()
    assert _get_log_context() == {}


@pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_setup_logger_levels(level):
    """Test logger setup with different log levels."""
    # Just verify setup_logger doesn't raise with different levels
    setup_logger(level=level)
    # Verify logger can log without error
    logger.info("Test message")


def test_intercept_standard_logging():
    """Test intercepting standard python logging."""
    setup_logger(level="INFO", intercept_standard_logging=True)

    std_logger = logging.getLogger("test_std")
    # This should be captured by loguru due to InterceptHandler
    std_logger.info("Captured log")

    # Check that root handler is set
    assert any(isinstance(h, logging.Handler) for h in logging.root.handlers)
