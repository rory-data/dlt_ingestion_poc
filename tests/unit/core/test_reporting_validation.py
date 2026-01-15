"""Tests for data quality reporting validation logic."""

import pytest

from ingestion.core.reporting.config import Severity
from ingestion.core.reporting.validation import DataQualityIssue, ValidationSummary


@pytest.mark.parametrize(
    ("total_rows", "total_bad_rows", "record_counts"),
    [
        (10, 2, {"10": 8}),
        (100, 5, {"20": 95}),
        (0, 0, {}),
    ],
)
def test_validation_summary_basic(total_rows, total_bad_rows, record_counts):
    """Test basic functionality of ValidationSummary."""
    summary = ValidationSummary(
        total_rows=total_rows,
        total_bad_rows=total_bad_rows,
        record_counts=record_counts,
    )
    assert summary.total_rows == total_rows
    assert summary.total_bad_rows == total_bad_rows
    assert summary.record_counts == record_counts

    dict_data = summary.to_dict()
    assert dict_data["total_rows"] == total_rows
    assert dict_data["total_bad_rows"] == total_bad_rows

    new_summary = ValidationSummary.from_dict(dict_data)
    assert new_summary.total_rows == total_rows
    assert new_summary.total_bad_rows == total_bad_rows


def test_validation_summary_merge():
    """Test merging two summaries."""
    s1 = ValidationSummary(
        total_rows=10,
        total_bad_rows=2,
        record_counts={"10": 8},
        issue_counts={"TypeA|col|CRITICAL": 2},
        samples={"TypeA|col|CRITICAL": ["val1"]},
    )
    s2 = ValidationSummary(
        total_rows=20,
        total_bad_rows=5,
        record_counts={"10": 15, "20": 5},
        issue_counts={"TypeA|col|CRITICAL": 5},
        samples={"TypeA|col|CRITICAL": ["val2"]},
    )

    s1.merge(s2)

    assert s1.total_rows == 30
    assert s1.total_bad_rows == 7
    assert s1.record_counts == {"10": 23, "20": 5}
    assert s1.issue_counts == {"TypeA|col|CRITICAL": 7}
    assert s1.samples == {"TypeA|col|CRITICAL": ["val1", "val2"]}


def test_validation_summary_update():
    """Test updating summary with session results."""
    summary = ValidationSummary()
    issues = [
        DataQualityIssue(
            severity=Severity.CRITICAL,
            issue_type="TypeA",
            column="col1",
            issue_count=2,
            sample_values=["bad1", "bad2"],
        )
    ]

    summary.update(issues, batch_rows=10, bad_rows_count=2)

    assert summary.total_rows == 10
    assert summary.total_bad_rows == 2
    assert summary.issue_counts["TypeA|col1|CRITICAL"] == 2
    assert "bad1" in summary.samples["TypeA|col1|CRITICAL"]
