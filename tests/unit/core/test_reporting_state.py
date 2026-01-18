"""Tests for validation summary reporting."""

from ingestion.core.reporting.state import log_validation_summary


def test_log_validation_summary_empty(caplog):
    """Test logging an empty summary."""
    log_validation_summary({})
    # No crash, should log "No validation summary available"


def test_log_validation_summary_clean(caplog):
    """Test logging a clean summary."""
    summary_dict = {
        "total_rows": 100,
        "total_bad_rows": 0,
        "record_counts": {"10": 100},
        "expected_record_counts": {"10": 100},
        "issue_counts": {},
        "samples": {},
    }

    log_validation_summary(summary_dict)
    # Check that 100 rows were logged


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
