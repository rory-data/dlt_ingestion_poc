"""Pytest configuration and shared fixtures."""

import logging

import pyarrow as pa
import pytest
from loguru import logger


@pytest.fixture
def caplog(caplog):
    """Override caplog to work with loguru."""

    class PropagateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    handler_id = logger.add(PropagateHandler(), format="{message}")
    yield caplog
    logger.remove(handler_id)


@pytest.fixture
def sample_arrow_table():
    """Provides a basic Arrow table for testing."""
    data = [
        pa.array(["Alice", "Bob", "Charlie"]),
        pa.array([25, 30, 35]),
        pa.array(["NY", "SF", "LA"]),
    ]
    schema = pa.schema(
        [("name", pa.string()), ("age", pa.int32()), ("city", pa.string())]
    )
    return pa.Table.from_arrays(data, schema=schema)


@pytest.fixture
def sample_record_batch(sample_arrow_table):
    """Provides a basic RecordBatch for testing."""
    return sample_arrow_table.to_batches()[0]


@pytest.fixture
def table_with_bad_data():
    """Provides an Arrow table with some 'bad' string data (replacement chars/non-printable)."""
    data = [
        pa.array(["Clean", "Replacement\ufffd", "Control\x07"]),
        pa.array([1, 2, 3]),
    ]
    schema = pa.schema([("text", pa.string()), ("id", pa.int32())])
    return pa.Table.from_arrays(data, schema=schema)
