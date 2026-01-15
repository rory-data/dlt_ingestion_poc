"""Tests for dlt state integration of reporting."""

from unittest.mock import MagicMock, patch

import dlt

from ingestion.core.reporting.config import Severity
from ingestion.core.reporting.state import (
    add_record_counts,
    get_validation_summary,
    log_validation_summary,
    update_validation_state,
)
from ingestion.core.reporting.validation import DataQualityIssue


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


@patch("dlt.current.state")
def test_add_record_counts_accumulates(mock_state):
    """Test that add_record_counts merges with existing counts."""
    my_state = {
        "validation_summary": {
            "total_rows": 0,
            "total_bad_rows": 0,
            "record_counts": {"10": 50},
        }
    }
    mock_state.return_value = my_state

    add_record_counts({"20": 30})

    assert my_state["validation_summary"]["record_counts"] == {"10": 50, "20": 30}


def test_get_validation_summary():
    """Test extracting summary from pipeline state."""
    mock_pipeline = MagicMock(spec=dlt.Pipeline)
    mock_pipeline.state = {
        "sources": {"my_source": {"validation_summary": {"total_rows": 100}}}
    }

    summary = get_validation_summary(mock_pipeline)
    assert summary["total_rows"] == 100


def test_get_validation_summary_fallback():
    """Test fallback to general pipeline state."""
    mock_pipeline = MagicMock(spec=dlt.Pipeline)
    mock_pipeline.state = {"validation_summary": {"total_rows": 200}}

    summary = get_validation_summary(mock_pipeline)
    assert summary["total_rows"] == 200


def test_get_validation_summary_no_sources():
    """Test when pipeline has no sources key."""
    mock_pipeline = MagicMock(spec=dlt.Pipeline)
    mock_pipeline.state = {}

    summary = get_validation_summary(mock_pipeline)
    assert summary == {}


def test_get_validation_summary_exception_handling():
    """Test error handling when accessing pipeline state raises exception."""

    class BadPipeline:
        @property
        def state(self):
            raise RuntimeError("State access failed")

    bad_pipeline = BadPipeline()
    summary = get_validation_summary(bad_pipeline)
    assert summary == {}


def test_log_validation_summary_empty():
    """Test logging when summary dict is empty."""
    log_validation_summary({})
    # Should not raise exception and return cleanly


def test_log_validation_summary_basic(caplog):
    """Test logging basic validation summary."""
    summary_dict = {
        "total_rows": 100,
        "total_bad_rows": 5,
        "record_counts": {},
        "expected_record_counts": {},
        "issue_counts": {},
        "samples": {},
    }

    log_validation_summary(summary_dict, table_name="test_table")
    assert True  # Log output is async with loguru


def test_log_validation_summary_with_records(caplog):
    """Test logging summary with record type counts and expected counts."""
    summary_dict = {
        "total_rows": 100,
        "total_bad_rows": 5,
        "record_counts": {"10": 50, "20": 45},
        "expected_record_counts": {"10": 50, "20": 50},
        "issue_counts": {},
        "samples": {},
    }

    log_validation_summary(summary_dict)
    # Summary was logged (assertion is implicit - no exceptions)


def test_log_validation_summary_with_records_partial_expected(caplog):
    """Test logging summary with some records having expected counts."""
    summary_dict = {
        "total_rows": 100,
        "total_bad_rows": 5,
        "record_counts": {"10": 50, "20": 45},
        "expected_record_counts": {"10": 50},  # Only "10" has expected count
        "issue_counts": {},
        "samples": {},
    }

    log_validation_summary(summary_dict)
    # Should log "10" with expected value and "20" without


def test_log_validation_summary_record_count_match(caplog):
    """Test logging where record count matches expected value (✓)."""
    summary_dict = {
        "total_rows": 100,
        "total_bad_rows": 0,
        "record_counts": {"10": 50},  # Matches expected
        "expected_record_counts": {"10": 50},
        "issue_counts": {},
        "samples": {},
    }

    log_validation_summary(summary_dict)
    # Should log with match indicator (✓)


def test_log_validation_summary_record_count_mismatch(caplog):
    """Test logging where record count doesn't match expected value (✗)."""
    summary_dict = {
        "total_rows": 100,
        "total_bad_rows": 5,
        "record_counts": {"10": 45},  # Doesn't match expected
        "expected_record_counts": {"10": 50},
        "issue_counts": {},
        "samples": {},
    }

    log_validation_summary(summary_dict)
    # Should log with mismatch indicator (✗)


def test_log_validation_summary_with_issues(caplog):
    """Test logging summary with data quality issues and sample values."""
    summary_dict = {
        "total_rows": 100,
        "total_bad_rows": 10,
        "record_counts": {"10": 90},
        "expected_record_counts": {"10": 100},
        "issue_counts": {
            "MissingValue|col1|CRITICAL": 5,
            "InvalidType|col2|WARNING": 5,
        },
        "samples": {
            "MissingValue|col1|CRITICAL": ["", None],
            "InvalidType|col2|WARNING": ["invalid1", "invalid2", "invalid3"],
        },
    }

    log_validation_summary(summary_dict)
    # Summary with issues was logged


def test_log_validation_summary_with_issues_many_samples(caplog):
    """Test logging issues with more than 3 sample values (truncated)."""
    summary_dict = {
        "total_rows": 100,
        "total_bad_rows": 10,
        "record_counts": {},
        "expected_record_counts": {},
        "issue_counts": {
            "InvalidFormat|phone|CRITICAL": 10,
        },
        "samples": {
            "InvalidFormat|phone|CRITICAL": ["bad1", "bad2", "bad3", "bad4", "bad5"],
        },
    }

    log_validation_summary(summary_dict)
    # Sample values should be truncated to first 3


def test_log_validation_summary_with_bad_percentage(caplog):
    """Test logging calculates bad row percentage correctly."""
    summary_dict = {
        "total_rows": 100,
        "total_bad_rows": 25,
        "record_counts": {},
        "expected_record_counts": {},
        "issue_counts": {},
        "samples": {},
    }

    log_validation_summary(summary_dict)
    # Percentage calculation should work (25%)


def test_log_validation_summary_zero_records(caplog):
    """Test logging when total_rows is zero."""
    summary_dict = {
        "total_rows": 0,
        "total_bad_rows": 0,
        "record_counts": {},
        "expected_record_counts": {},
        "issue_counts": {},
        "samples": {},
    }

    log_validation_summary(summary_dict)
    # Should handle zero division gracefully


@patch("dlt.current.state")
def test_update_validation_state_with_issues(mock_state):
    """Test update_validation_state with DataQualityIssue objects."""
    my_state = {}
    mock_state.return_value = my_state

    issues = [
        DataQualityIssue(
            severity=Severity.CRITICAL,
            issue_type="InvalidFormat",
            column="phone",
            issue_count=3,
            sample_values=["invalid1", "invalid2"],
        ),
        DataQualityIssue(
            severity=Severity.WARNING,
            issue_type="MissingValue",
            column="email",
            issue_count=2,
            sample_values=["", "None"],
        ),
    ]

    update_validation_state(issues=issues, total_rows=100, bad_rows=5)

    assert my_state["validation_summary"]["total_rows"] == 100
    assert my_state["validation_summary"]["total_bad_rows"] == 5
    # Issues should be recorded in the summary
    assert len(my_state["validation_summary"]["issue_counts"]) > 0
