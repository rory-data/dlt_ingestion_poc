"""Configuration and data models for data quality validation."""

from dataclasses import dataclass
from enum import Enum

# CONSTANTS
REPLACEMENT_CHAR = "\ufffd"  # Unicode replacement character to detect encoding issues


class Severity(Enum):
    """Data quality issue severity levels."""

    CRITICAL = "CRITICAL"  # The issue is severe enough to potentially block processing
    WARNING = "WARNING"  # The issue is noteworthy but not blocking


@dataclass
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
