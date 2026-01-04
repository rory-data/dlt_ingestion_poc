"""Configuration and data models for data quality validation."""

from dataclasses import dataclass
from enum import Enum


class Rules(str, Enum):
    """Data quality validation rules."""

    REPLACEMENT_CHAR = (
        "\ufffd"  # Unicode replacement character to detect encoding issues
    )


class Severity(Enum):
    """Data quality issue severity levels."""

    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass
class DataQualityIssue:
    """Represents a single data quality issue."""

    severity: Severity
    issue_type: str
    column: str
    row_indices: list[int]
    sample_values: list[str]

    def __str__(self) -> str:
        """Format issue for display."""
        rows_str = ", ".join(map(str, self.row_indices))
        samples_str = " | ".join(repr(v) for v in self.sample_values)
        return (
            f"[{self.severity.value}] {self.issue_type} in column '{self.column}' "
            f"at rows {rows_str}: {samples_str}"
        )

    def format_for_row(self, row_idx: int) -> str:
        """Format issue showing only the specific row's context.

        Args:
            row_idx: The specific row index to show context for

        Returns:
            Formatted string for display
        """
        try:
            row_position = self.row_indices.index(row_idx)
            sample_value = repr(self.sample_values[row_position])
            return (
                f"[{self.severity.value}] {self.issue_type} in column '{self.column}': "
                f"{sample_value}"
            )
        except ValueError:
            # Row not in this issue's indices, fall back to full format
            return str(self)
