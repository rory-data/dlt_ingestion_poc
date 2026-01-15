"""Tests for schema mapping logic."""

from datetime import date, datetime, time

import pytest

from ingestion.schema.mapping import get_python_type


@pytest.mark.parametrize(
    ("schema_type", "expected_type"),
    [
        ("string", str),
        ("date", date),
        ("timestamp", datetime),
        ("time", time),
        ("number", float),
        ("integer", int),
        ("boolean", bool),
        ("unknown", str),  # Default for unknown types
    ],
)
def test_get_python_type(schema_type, expected_type):
    """Test mapping ODCS types to Python types."""
    assert get_python_type(schema_type) == expected_type
