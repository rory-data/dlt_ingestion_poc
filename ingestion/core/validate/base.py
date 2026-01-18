"""Base protocols and types for data validation.

This module defines the interfaces and data structures for the validation layer.
All validators should implement the Validator protocol to enable composition
and pluggability in validation pipelines.
"""

from dataclasses import dataclass, field
from typing import Protocol

import pyarrow as pa

from ingestion.core.reporting.config import DataQualityIssue


@dataclass(frozen=True)
class ValidationResult:
    """Immutable result of validation operation.

    Represents the outcome of validating a PyArrow Table, including clean data,
    bad data, and any issues found. Use frozen=True to ensure immutability
    and enable safe sharing between components.

    Attributes:
        clean_data: PyArrow Table or RecordBatch with valid records
        bad_data: PyArrow Table or RecordBatch with invalid records
        issues: List of data quality issues found
        record_type: Optional record type identifier (for multi-record formats)
    """

    clean_data: pa.Table | pa.RecordBatch
    bad_data: pa.Table | pa.RecordBatch
    issues: list[DataQualityIssue] = field(default_factory=list)
    record_type: str | None = None

    @property
    def is_valid(self) -> bool:
        """Check if validation passed (no issues found)."""
        return len(self.issues) == 0

    @property
    def clean_row_count(self) -> int:
        """Get number of clean rows."""
        return self.clean_data.num_rows

    @property
    def bad_row_count(self) -> int:
        """Get number of bad rows."""
        return self.bad_data.num_rows

    @property
    def total_row_count(self) -> int:
        """Get total rows processed."""
        return self.clean_row_count + self.bad_row_count


class Validator(Protocol):
    """Protocol for data validators.

    Validators are responsible for checking data quality and partitioning
    input into clean and bad data. Implementations should:

    1. Be stateless (no side effects)
    2. Be idempotent (validate(data) twice = same result)
    3. Return ValidationResult with clean/bad partitions
    4. Provide clear, actionable error messages

    Example:
        >>> validator = UTF8Validator()
        >>> result = validator.validate(my_table)
        >>> if result.is_valid:
        ...     loader.load(result.clean_data)
        ... else:
        ...     quarantine.save(result.bad_data)
    """

    def validate(self, table: pa.Table | pa.RecordBatch) -> ValidationResult:
        """Validate a PyArrow Table or RecordBatch.

        Args:
            table: PyArrow Table or RecordBatch to validate

        Returns:
            ValidationResult containing clean data, bad data, and issues

        Raises:
            ValueError: If table schema is invalid
            Exception: If validation fails due to infrastructure error
        """
        ...
