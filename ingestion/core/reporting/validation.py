"""Common utilities for data quality validation."""

from dataclasses import asdict, dataclass, field
from typing import Any, Self

import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger

from .config import DataQualityIssue


class DataQualityError(Exception):
    """Raised when data quality validation fails in 'reject' mode."""


@dataclass
class ValidationSummary:
    """Accumulated validation results across multiple batches.

    This class provides a structured way to track validation results
    and can be serialized to/from dictionaries for persistence.
    """

    total_rows: int = 0
    total_bad_rows: int = 0
    # Record type -> count of clean rows
    record_counts: dict[str, int] = field(default_factory=dict)
    # Record type -> expected count from trailer
    expected_record_counts: dict[str, int] = field(default_factory=dict)
    # "issue_type|column|severity" -> count of affected rows
    issue_counts: dict[str, int] = field(default_factory=dict)
    # "issue_type|column|severity" -> list of sample values
    samples: dict[str, list[str]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create a summary from a dictionary (e.g. from dlt state)."""
        return cls(
            total_rows=data.get("total_rows", 0),
            total_bad_rows=data.get("total_bad_rows", 0),
            record_counts=data.get("record_counts", {}).copy(),
            expected_record_counts=data.get("expected_record_counts", {}).copy(),
            issue_counts=data.get("issue_counts", {}).copy(),
            samples=data.get("samples", {}).copy(),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert summary to a dictionary for persistence."""
        return asdict(self)

    def merge(self, other: Self) -> None:
        """Merge another summary into this one."""
        self.total_rows += other.total_rows
        self.total_bad_rows += other.total_bad_rows
        self._merge_counts(other)
        self._merge_samples(other)

    def _merge_counts(self, other: Self) -> None:
        """Merge record and issue counts."""
        for rt, count in other.record_counts.items():
            self.record_counts[rt] = self.record_counts.get(rt, 0) + count

        for rt, count in other.expected_record_counts.items():
            if rt not in self.expected_record_counts:
                self.expected_record_counts[rt] = count
            elif self.expected_record_counts[rt] != count:
                logger.warning(
                    "Conflicting counts for {rt}: {count}", rt=rt, count=count
                )

        for key, count in other.issue_counts.items():
            self.issue_counts[key] = self.issue_counts.get(key, 0) + count

    def _merge_samples(self, other: Self) -> None:
        """Merge unique samples."""
        for key, other_samples in other.samples.items():
            current_samples = self.samples.setdefault(key, [])
            unique = set(current_samples)
            for s in other_samples:
                if len(unique) >= 10:
                    break
                if s not in unique:
                    current_samples.append(s)
                    unique.add(s)

    def update(
        self, issues: list[DataQualityIssue], batch_rows: int, bad_rows_count: int
    ) -> None:
        """Update summary with issues from a new batch.

        Args:
            issues: List of issues found in the current batch
            batch_rows: Total rows in the current batch
            bad_rows_count: Total number of rows with at least one issue in this batch
        """
        # Ensure we have valid integers
        batch_rows = int(batch_rows) if batch_rows is not None else 0
        bad_rows_count = int(bad_rows_count) if bad_rows_count is not None else 0

        self.total_rows += batch_rows
        self.total_bad_rows += bad_rows_count

        for issue in issues:
            key = f"{issue.issue_type}|{issue.column}|{issue.severity.value}"
            issue_count = int(issue.issue_count) if issue.issue_count is not None else 0
            self.issue_counts[key] = self.issue_counts.get(key, 0) + issue_count

            if key not in self.samples:
                self.samples[key] = []

            # Keep a small set of unique samples
            current_samples = set(self.samples[key])
            if len(current_samples) < 10:
                for val in issue.sample_values:
                    if len(current_samples) >= 10:
                        break
                    if val not in current_samples:
                        self.samples[key].append(val)
                        current_samples.add(val)

    def add_record_counts(self, counts: dict[str, int]) -> None:
        """Add record counts to the summary.

        Args:
            counts: Dictionary mapping record type to row count
        """
        for rt, count in counts.items():
            self.record_counts[rt] = self.record_counts.get(rt, 0) + count

    def set_expected_counts(self, expected_counts: dict[str, int]) -> None:
        """Set expected record counts from trailer records.

        Args:
            expected_counts: Dictionary mapping record type to expected count
        """
        self.expected_record_counts = expected_counts


def extract_issue_summary(
    column: pa.ChunkedArray, mask: pa.Array | pa.ChunkedArray, max_samples: int = 5
) -> tuple[int, list[str]]:
    """Extract issue count and a small number of sample values.

    Args:
        column: The column to extract values from.
        mask: Boolean mask where True indicates an issue.
        max_samples: Maximum number of sample values to extract.

    Returns:
        Tuple of (issue_count, sample_values).
    """
    # Use Arrow compute to get count without bringing data to Python
    issue_count_scalar = pc.sum(mask).as_py()  # ty: ignore[unresolved-attribute]
    issue_count = int(issue_count_scalar) if issue_count_scalar is not None else 0

    if issue_count == 0:
        return 0, []

    # Only extract a few samples for reporting
    indices = pc.indices_nonzero(mask)  # ty: ignore[unresolved-attribute]
    if isinstance(indices, pa.ChunkedArray):
        indices = indices.combine_chunks()

    sample_indices = indices.slice(0, max_samples)
    problematic_samples = pc.take(column, sample_indices)

    if isinstance(problematic_samples, pa.ChunkedArray):
        problematic_samples = problematic_samples.combine_chunks()

    sample_values = [val.as_py() for val in problematic_samples]
    return issue_count, sample_values
