"""Tests for Arrow-based data validation."""

import pyarrow as pa
import pytest

from ingestion.core.reporting.config import Severity
from ingestion.core.reporting.validation import DataQualityError, ValidationSummary
from ingestion.core.validate.utf8 import validate_string_columns
from ingestion.core.validate.validator import ArrowValidator


def test_validate_string_columns_clean(sample_arrow_table):
    """Test that clean string columns return no issues."""
    issues = validate_string_columns(sample_arrow_table)
    assert len(issues) == 0


def test_validate_string_columns_bad(table_with_bad_data):
    """Test detection of replacement and control characters."""
    issues = validate_string_columns(table_with_bad_data)
    assert len(issues) == 2

    # Check severity and issue types
    # Expected: 1 Critical (Control char), 1 Warning (Replacement char)
    issue_types = {issue.issue_type for issue in issues}
    assert "Invalid Characters (control or non-printable)" in issue_types
    assert "Unicode Replacement Character (U+FFFD)" in issue_types

    severities = {issue.severity for issue in issues}
    assert Severity.CRITICAL in severities
    assert Severity.WARNING in severities


def test_arrow_validator_split_clean_only(sample_arrow_table):
    """Test validator when all data is clean."""
    validator = ArrowValidator(issue_action="reject")
    clean, bad, issues = validator.validate(sample_arrow_table)

    assert clean.num_rows == 3
    assert bad.num_rows == 0
    assert len(issues) == 0


def test_arrow_validator_split_mixed(table_with_bad_data):
    """Test validator splitting clean and bad rows."""
    validator = ArrowValidator(issue_action="quarantine")
    clean, bad, issues = validator.validate(table_with_bad_data)

    # Row 0: "Clean" -> clean
    # Row 1: "Replacement\ufffd" -> bad
    # Row 2: "Control\x07" -> bad
    assert clean.num_rows == 1
    assert bad.num_rows == 2
    assert len(issues) == 2
    assert clean.column("text")[0].as_py() == "Clean"


def test_arrow_validator_record_batch_support(sample_record_batch):
    """Test validator with RecordBatch input."""
    validator = ArrowValidator()
    clean, bad, _issues = validator.validate(sample_record_batch)

    assert isinstance(clean, pa.RecordBatch)
    assert isinstance(bad, pa.RecordBatch)
    assert clean.num_rows == 3


@pytest.mark.parametrize(
    ("issue_action", "should_raise"),
    [
        ("reject", True),
        ("quarantine", False),
    ],
)
def test_arrow_validator_action_behavior(
    table_with_bad_data, issue_action, should_raise
):
    """Test that ArrowValidator.raise_if_failed behaves correctly based on issue_action."""
    validator = ArrowValidator(issue_action=issue_action)
    summary = ValidationSummary(
        total_rows=3,
        total_bad_rows=2,
        issue_counts={"Invalid Characters|text|CRITICAL": 1},
    )

    if should_raise:
        with pytest.raises(DataQualityError, match="Pipeline rejected"):
            validator.raise_if_failed(summary)
    else:
        # Should not raise
        validator.raise_if_failed(summary)


def test_arrow_validator_verify_trailer():
    """Test trailer verification logic."""
    summary = ValidationSummary()
    summary.record_counts = {"10": 100, "20": 50}

    trailer_counts = {"10": 100, "20": 50}
    assert ArrowValidator.verify_trailer(summary, trailer_counts) is True

    bad_trailer_counts = {"10": 100, "20": 49}
    assert ArrowValidator.verify_trailer(summary, bad_trailer_counts) is False


def test_arrow_validator_empty_data():
    """Test validator with empty table."""
    schema = pa.schema([("a", pa.string())])
    table = schema.empty_table()
    validator = ArrowValidator()
    clean, bad, issues = validator.validate(table)

    assert clean.num_rows == 0
    assert bad.num_rows == 0
    assert len(issues) == 0


def test_arrow_validator_report_summary(caplog):
    """Test reporting summary to logs."""
    summary = ValidationSummary(
        total_rows=10,
        total_bad_rows=2,
        issue_counts={"Invalid Characters|text|CRITICAL": 2},
        samples={"Invalid Characters|text|CRITICAL": ["bad\x07"]},
    )

    ArrowValidator.report_summary(summary)

    assert "DATA QUALITY VALIDATION SUMMARY" in caplog.text
    assert "Total rows processed: 10" in caplog.text
    assert "Total bad rows:       2" in caplog.text
    assert "[CRITICAL] Invalid Characters in column 'text': 2 rows" in caplog.text
    assert "bad\\x07" in caplog.text
