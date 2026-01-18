"""Tests for dlt state integration of reporting."""

from unittest.mock import MagicMock, patch

import dlt
import pytest

from ingestion.core.reporting.config import Severity
from ingestion.core.reporting.validation import DataQualityIssue
from ingestion.engines.dlt.state import (
    add_record_counts,
    get_validation_summary,
    update_validation_state,
)


@patch("dlt.current.state")
def test_update_validation_state(mock_state):
    """Test updating dlt state with validation data."""
    my_state = {}
    mock_state.return_value = my_state

    update_validation_state(issues=[], total_rows=100, bad_rows=5)

    assert "validation_summary" in my_state
    assert my_state["validation_summary"]["total_rows"] == 100
    assert my_state["validation_summary"]["total_bad_rows"] == 5


@patch("dlt.current.state")
def test_update_validation_state_with_expected_counts(mock_state):
    """Test updating state with expected record counts from trailers."""
    my_state = {}
    mock_state.return_value = my_state

    issues = [
        DataQualityIssue(
            severity=Severity.WARNING,
            issue_type="MissingValue",
            column="col1",
            issue_count=2,
            sample_values=["", "None"],
        )
    ]
    expected_counts = {"10": 100, "20": 50}

    update_validation_state(
        issues=issues,
        total_rows=150,
        bad_rows=2,
        expected_record_counts=expected_counts,
    )

    assert my_state["validation_summary"]["total_rows"] == 150
    assert my_state["validation_summary"]["total_bad_rows"] == 2
    assert my_state["validation_summary"]["expected_record_counts"] == expected_counts


@patch("dlt.current.state")
def test_update_validation_state_multiple_calls(mock_state):
    """Test that multiple calls to update_validation_state accumulate data."""
    my_state = {}
    mock_state.return_value = my_state

    # First call
    update_validation_state(issues=[], total_rows=100, bad_rows=5)
    # Second call
    update_validation_state(issues=[], total_rows=50, bad_rows=2)

    assert my_state["validation_summary"]["total_rows"] == 150
    assert my_state["validation_summary"]["total_bad_rows"] == 7


@patch("dlt.current.state")
def test_add_record_counts(mock_state):
    """Test adding record counts to dlt state."""
    my_state = {
        "validation_summary": {
            "total_rows": 0,
            "total_bad_rows": 0,
            "record_counts": {},
        }
    }
    mock_state.return_value = my_state

    add_record_counts({"10": 50})

    assert my_state["validation_summary"]["record_counts"] == {"10": 50}


def test_get_validation_summary_from_sources():
    """Test extracting summary from pipeline source state."""
    pipeline = MagicMock()
    pipeline.state = {
        "sources": {"my_source": {"validation_summary": {"total_rows": 100}}}
    }
    summary = get_validation_summary(pipeline)
    assert summary == {"total_rows": 100}


def test_get_validation_summary_fallback():
    """Test fallback to general pipeline state."""
    pipeline = MagicMock()
    pipeline.state = {"validation_summary": {"total_rows": 200}}
    summary = get_validation_summary(pipeline)
    assert summary == {"total_rows": 200}
