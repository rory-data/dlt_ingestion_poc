"""Shared test fixtures and configuration for jestr tests."""

from unittest.mock import MagicMock

import pyarrow as pa
import pytest


@pytest.fixture
def sample_table() -> pa.Table:
    """Create a sample PyArrow Table for testing."""
    return pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "email": [
                "alice@example.com",
                "bob@example.com",
                "charlie@example.com",
                "diana@example.com",
                "eve@example.com",
            ],
            "age": [25, 30, 35, 28, 32],
        }
    )


@pytest.fixture
def sample_record_batch() -> pa.RecordBatch:
    """Create a sample PyArrow RecordBatch for testing."""
    return pa.record_batch(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.3, 15.7],
        }
    )


@pytest.fixture
def string_table_with_issues() -> pa.Table:
    """Create a PyArrow Table with string encoding issues for testing."""
    return pa.table(
        {
            "id": [1, 2, 3, 4],
            "text": [
                "valid string",
                "string with \ufffd replacement character",
                "normal text",
                "another valid string",
            ],
            "description": ["clean", "has issue", "clean", "clean"],
        }
    )


@pytest.fixture
def mock_connection():
    """Create a mock database connection."""
    mock_conn = MagicMock()
    mock_conn.execute = MagicMock(return_value=MagicMock())
    mock_conn.close = MagicMock()
    return mock_conn


@pytest.fixture
def mock_adapter():
    """Create a mock database adapter."""
    mock = MagicMock()
    mock.to_arrow = MagicMock(return_value=pa.RecordBatch.from_arrays([]))
    mock.get_arrow_schema = MagicMock(return_value=pa.schema([]))
    mock.to_parquet = MagicMock()
    return mock


@pytest.fixture
def mock_odcs_contract():
    """Create a mock ODCS contract."""
    mock_contract = MagicMock()
    mock_contract.id = "test_contract"
    mock_contract.version = "1.0.0"
    mock_contract.status = "active"
    mock_contract.evolution_mode = "notify"
    mock_contract.validation_mode = "reject"
    mock_contract.custom_properties = {}
    mock_contract.to_dlt_schemas = MagicMock(return_value={})
    return mock_contract


@pytest.fixture
def temp_parquet_path(tmp_path):
    """Create a temporary Parquet file path."""
    return tmp_path / "test_data.parquet"


@pytest.fixture
def temp_contract_path(tmp_path):
    """Create a temporary contract file path."""
    return tmp_path / "contract.yaml"
