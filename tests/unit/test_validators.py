"""Tests for data validation modules."""

import pyarrow as pa
import pytest

from jestr.validators.core import (
    DataQualityIssue,
    ValidationResult,
    _get_bad_row_mask,
    _partition_by_validity,
)
from jestr.validators.core.issues import Severity


@pytest.mark.unit
class TestDataQualityIssue:
    """Test DataQualityIssue dataclass."""

    def test_issue_creation(self):
        """Test creating a data quality issue."""
        issue = DataQualityIssue(
            severity=Severity.CRITICAL,
            issue_type="Invalid Character",
            column="name",
            issue_count=5,
            sample_values=["sample1", "sample2"],
        )
        assert issue.severity == Severity.CRITICAL
        assert issue.issue_type == "Invalid Character"
        assert issue.column == "name"
        assert issue.issue_count == 5
        assert issue.sample_values == ["sample1", "sample2"]

    def test_issue_string_representation(self):
        """Test string representation of issue."""
        issue = DataQualityIssue(
            severity=Severity.WARNING,
            issue_type="Encoding Error",
            column="text",
            issue_count=3,
            sample_values=["bad1", "bad2"],
        )
        issue_str = str(issue)
        assert "warning" in issue_str.lower()
        assert "Encoding Error" in issue_str
        assert "text" in issue_str
        assert "3 rows affected" in issue_str

    def test_issue_with_empty_samples(self):
        """Test issue with empty sample values."""
        issue = DataQualityIssue(
            severity=Severity.CRITICAL,
            issue_type="Null Value",
            column="id",
            issue_count=10,
            sample_values=[],
        )
        assert issue.sample_values == []
        issue_str = str(issue)
        assert "10 rows affected" in issue_str

    @pytest.mark.parametrize(
        ("severity", "expected"),
        [
            (Severity.CRITICAL, "critical"),
            (Severity.WARNING, "warning"),
        ],
    )
    def test_issue_severity_values(self, severity, expected):
        """Test severity enum values."""
        issue = DataQualityIssue(
            severity=severity,
            issue_type="Test",
            column="col",
            issue_count=1,
            sample_values=[],
        )
        assert issue.severity == severity
        assert issue.severity.value == expected

    def test_issue_frozen(self):
        """Test that DataQualityIssue is immutable."""
        issue = DataQualityIssue(
            severity=Severity.CRITICAL,
            issue_type="Test",
            column="col",
            issue_count=1,
            sample_values=[],
        )
        with pytest.raises(AttributeError, match="cannot assign to field"):
            issue.severity = Severity.WARNING  # type: ignore[assignment]


@pytest.mark.unit
class TestValidationResult:
    """Test ValidationResult dataclass."""

    def test_validation_result_creation(self):
        """Test creating a validation result."""
        clean_data = pa.table({"id": [1, 2, 3]})
        bad_data = pa.table({"id": []})

        result = ValidationResult(
            clean_data=clean_data,
            bad_data=bad_data,
        )
        assert result.clean_data is clean_data
        assert result.bad_data is bad_data
        assert result.issues == []

    def test_validation_result_with_issues(self):
        """Test validation result with issues."""
        clean_data = pa.table({"id": [1, 2]})
        bad_data = pa.table({"id": [3]})
        issues = [
            DataQualityIssue(
                severity=Severity.CRITICAL,
                issue_type="Invalid",
                column="id",
                issue_count=1,
                sample_values=["3"],
            )
        ]

        result = ValidationResult(
            clean_data=clean_data,
            bad_data=bad_data,
            issues=issues,
        )
        assert len(result.issues) == 1
        assert result.issues[0].issue_type == "Invalid"

    @pytest.mark.parametrize(
        ("has_issues", "expected_is_valid"),
        [
            (False, True),
            (True, False),
        ],
    )
    def test_validation_result_is_valid(self, has_issues, expected_is_valid):
        """Test is_valid property logic."""
        clean_data = pa.table({"id": [1]})
        bad_data = pa.table({"id": []})
        issues = []
        if has_issues:
            issues.append(
                DataQualityIssue(
                    severity=Severity.CRITICAL,
                    issue_type="Test",
                    column="id",
                    issue_count=1,
                    sample_values=["1"],
                )
            )

        result = ValidationResult(
            clean_data=clean_data,
            bad_data=bad_data,
            issues=issues,
        )
        assert result.is_valid is expected_is_valid

    def test_validation_result_clean_row_count(self):
        """Test clean_row_count property."""
        clean_data = pa.table({"id": [1, 2, 3, 4, 5]})
        bad_data = pa.table({"id": [6, 7]})
        result = ValidationResult(clean_data=clean_data, bad_data=bad_data)
        assert result.clean_row_count == 5

    def test_validation_result_bad_row_count(self):
        """Test bad_row_count property."""
        clean_data = pa.table({"id": [1, 2, 3]})
        bad_data = pa.table({"id": [4, 5]})
        result = ValidationResult(clean_data=clean_data, bad_data=bad_data)
        assert result.bad_row_count == 2

    def test_validation_result_total_row_count(self):
        """Test total_row_count property."""
        clean_data = pa.table({"id": [1, 2, 3]})
        bad_data = pa.table({"id": [4, 5]})
        result = ValidationResult(clean_data=clean_data, bad_data=bad_data)
        assert result.total_row_count == 5

    def test_validation_result_with_record_batch(self):
        """Test validation result with RecordBatch."""
        clean_batch = pa.record_batch({"id": [1, 2, 3]})
        bad_batch = pa.record_batch({"id": [4]})
        result = ValidationResult(clean_data=clean_batch, bad_data=bad_batch)
        assert result.clean_row_count == 3
        assert result.bad_row_count == 1
        assert result.total_row_count == 4

    def test_validation_result_with_metadata(self):
        """Test validation result with schema metadata."""
        schema = pa.schema([("id", pa.int64())], metadata={"name": "order_table"})
        result = ValidationResult(
            clean_data=pa.table({"id": [1, 2]}),
            bad_data=pa.table({"id": [3]}),
            record_type="Order",
            schema_metadata=schema,
        )
        assert result.record_type == "Order"
        assert result.schema_metadata == schema


@pytest.mark.unit
class TestGetBadRowMask:
    """Test _get_bad_row_mask function."""

    def test_get_bad_row_mask_clean_data(self):
        """Test mask for clean data (no issues)."""
        table = pa.table(
            {
                "id": [1, 2, 3],
                "text": ["hello", "world", "test"],
            }
        )
        mask = _get_bad_row_mask(table)
        assert mask.type == pa.bool_()
        assert mask.null_count == 0
        # Clean data should have all False
        assert not any(mask.to_pylist())

    def test_get_bad_row_mask_detects_replacement_char(self):
        """Test mask detects Unicode replacement character."""
        table = pa.table(
            {
                "id": [1, 2, 3],
                "text": ["valid", "bad\ufffd", "valid"],
            }
        )
        mask = _get_bad_row_mask(table)
        mask_list = mask.to_pylist()
        assert mask_list[1] is True  # Row with replacement char

    def test_get_bad_row_mask_numeric_columns_ignored(self):
        """Test that numeric columns are not checked."""
        table = pa.table(
            {
                "id": [1, 2, 3],
                "value": [10.5, 20.3, 15.7],
            }
        )
        mask = _get_bad_row_mask(table)
        # All numeric, should be all False
        assert not any(mask.to_pylist())

    def test_get_bad_row_mask_mixed_columns(self):
        """Test mask with mixed column types."""
        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob\ufffd", "Charlie"],
                "age": [25, 30, 35],
            }
        )
        mask = _get_bad_row_mask(table)
        mask_list = mask.to_pylist()
        assert mask_list[1] is True  # Row with issue in name
        assert not mask_list[0]
        assert not mask_list[2]

    def test_get_bad_row_mask_null_handling(self):
        """Test mask handles nulls correctly."""
        table = pa.table(
            {
                "id": [1, 2, 3],
                "text": ["valid", None, "valid"],
            }
        )
        mask = _get_bad_row_mask(table)
        # Nulls should be considered clean
        assert not mask.to_pylist()[1]


@pytest.mark.unit
class TestPartitionByValidity:
    """Test _partition_by_validity function."""

    def test_partition_all_clean(self):
        """Test partitioning when all rows are clean."""
        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        mask = pa.array([False, False, False])
        clean, bad = _partition_by_validity(table, mask)
        assert clean.num_rows == 3
        assert bad.num_rows == 0

    def test_partition_all_bad(self):
        """Test partitioning when all rows are bad."""
        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["A\ufffd", "B\ufffd", "C\ufffd"],
            }
        )
        mask = pa.array([True, True, True])
        clean, bad = _partition_by_validity(table, mask)
        assert clean.num_rows == 0
        assert bad.num_rows == 3

    def test_partition_mixed(self):
        """Test partitioning with mixed clean and bad rows."""
        table = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["A", "B\ufffd", "C", "D\ufffd", "E"],
            }
        )
        mask = pa.array([False, True, False, True, False])
        clean, bad = _partition_by_validity(table, mask)
        assert clean.num_rows == 3
        assert bad.num_rows == 2

    def test_partition_preserves_schema(self):
        """Test that partitioning preserves schema."""
        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "value": [1.5, 2.5, 3.5],
            }
        )
        mask = pa.array([False, True, False])
        clean, bad = _partition_by_validity(table, mask)
        assert clean.schema == table.schema
        assert bad.schema == table.schema

    def test_partition_empty_result(self):
        """Test partitioning resulting in empty partition."""
        table = pa.table({"id": [1, 2, 3]})
        mask = pa.array([True, True, True])
        clean, bad = _partition_by_validity(table, mask)
        assert bad.num_rows == 3
        assert clean.num_rows == 0


@pytest.mark.unit
class TestValidatorProtocol:
    """Test the Validator protocol."""

    def test_validator_protocol_implementation(self):
        """Test implementing the Validator protocol."""

        class SimpleValidator:
            def validate(self, batch: pa.RecordBatch) -> ValidationResult:
                clean = pa.table({"id": [1, 2]})
                bad = pa.table({"id": []})
                return ValidationResult(clean_data=clean, bad_data=bad)

        validator = SimpleValidator()
        batch = pa.record_batch({"id": [1, 2]})
        result = validator.validate(batch)
        assert isinstance(result, ValidationResult)
        assert result.is_valid is True

    def test_validator_with_issues(self):
        """Test validator that detects issues."""

        class IssueDetector:
            def validate(self, batch: pa.RecordBatch) -> ValidationResult:
                clean = pa.table({"id": [1]})
                bad = pa.table({"id": [2]})
                issues = [
                    DataQualityIssue(
                        severity=Severity.CRITICAL,
                        issue_type="Bad Data",
                        column="id",
                        issue_count=1,
                        sample_values=["2"],
                    )
                ]
                return ValidationResult(clean_data=clean, bad_data=bad, issues=issues)

        validator = IssueDetector()
        batch = pa.record_batch({"id": [1, 2]})
        result = validator.validate(batch)
        assert not result.is_valid
        assert len(result.issues) == 1
