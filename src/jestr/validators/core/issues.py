"""Data quality issue representations and exceptions."""

from dataclasses import dataclass
from enum import StrEnum


class Severity(StrEnum):
    """Enumeration of issue severity levels."""

    CRITICAL = "critical"  # The issue is severe enough to potentially halt processing
    WARNING = "warning"  # The issue is notable but does not prevent processing


@dataclass(frozen=True)
class DataQualityIssue:
    """Represents a summary of data quality issues in a batch."""

    severity: Severity
    issue_type: str
    column: str
    issue_count: int
    sample_values: list[str]

    def __str__(self) -> str:
        """Format issue for display."""
        samples_str = " | ".join(repr(v) for v in self.sample_values)
        return (
            f"[{self.severity.value}] {self.issue_type} in column '{self.column}' "
            f"({self.issue_count} rows affected). Samples: {samples_str}"
        )


class DataQualityError(Exception):
    """Custom exception for data quality validation errors."""
