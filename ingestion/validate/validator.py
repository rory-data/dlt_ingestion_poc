"""Data quality validator for Arrow-related data structures."""

from dataclasses import dataclass, field
from typing import Annotated, Any, Literal

import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger

from ingestion.validate.config import DataQualityIssue, Severity
from ingestion.validate.utf8 import validate_string_columns


class DataQualityError(Exception):
    """Raised when data quality validation fails in 'reject' mode."""

    pass


@dataclass
class ValidationSummary:
    """Accumulated validation results across multiple batches.

    This class can be used to wrap a dictionary (e.g. dlt state) to provide
    convenient update and reporting methods.
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
    def from_dict(cls, data: dict[str, Any]) -> "ValidationSummary":
        """Create a summary from a dictionary (e.g. from dlt state)."""
        return cls(
            total_rows=data.get("total_rows", 0),
            total_bad_rows=data.get("total_bad_rows", 0),
            record_counts=data.get("record_counts", {}),
            expected_record_counts=data.get("expected_record_counts", {}),
            issue_counts=data.get("issue_counts", {}),
            samples=data.get("samples", {}),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert summary to a dictionary for persistence."""
        return {
            "total_rows": self.total_rows,
            "total_bad_rows": self.total_bad_rows,
            "record_counts": self.record_counts,
            "expected_record_counts": self.expected_record_counts,
            "issue_counts": self.issue_counts,
            "samples": self.samples,
        }

    def update(
        self, issues: list[DataQualityIssue], batch_rows: int, bad_rows_count: int
    ) -> None:
        """Update summary with issues from a new batch.

        Args:
            issues: List of issues found in the current batch
            batch_rows: Total rows in the current batch
            bad_rows_count: Total number of rows with at least one issue in this batch
        """
        self.total_rows += batch_rows
        self.total_bad_rows += bad_rows_count

        for issue in issues:
            key = f"{issue.issue_type}|{issue.column}|{issue.severity.value}"
            self.issue_counts[key] = self.issue_counts.get(key, 0) + issue.issue_count

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


class ArrowValidator:
    """Data quality validator for Arrow-related data structures.

    This class is stateless and focused on Arrow-based validation.
    """

    def __init__(
        self,
        issue_action: Annotated[
            Literal["reject", "quarantine"],
            "This selects the action to take when issues are found. 'quarantine' to separate \
            bad records and not block pipeline progress; 'reject' to block the entire batch.",
        ] = "reject",
        check_nfc: bool = False,
    ) -> None:
        """Initialise the validator.

        Args:
            issue_action: The action to take when issues are found.
            check_nfc: Whether to perform expensive NFC normalization check.
        """
        self.issue_action = issue_action
        self.check_nfc = check_nfc

    @staticmethod
    def get_bad_row_mask(table: pa.Table) -> pa.Array:
        """Create a boolean mask for rows that have any data quality issues.

        This is a simplified version that uses Arrow compute to find bad rows.
        """
        # Start with all False (no issues)
        bad_mask = pa.array([False] * table.num_rows, type=pa.bool_())

        # For string columns, check for invalid characters and replacement characters
        for name in table.schema.names:
            col = table.column(name)
            if not (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)):
                continue

            # Invalid characters
            printable_mask = pc.utf8_is_printable(col)
            bad_mask = pc.or_(bad_mask, pc.invert(printable_mask))

            # Replacement characters
            repl_mask = pc.match_substring(col, "\ufffd")
            bad_mask = pc.or_(bad_mask, repl_mask)

        return bad_mask

    def validate(
        self, data: pa.Table | pa.RecordBatch
    ) -> tuple[pa.Table, pa.Table, list[DataQualityIssue]]:
        """Validate the given PyArrow data structure for data quality issues.

        Args:
            data: PyArrow Table or RecordBatch to validate

        Returns:
            Tuple of (clean_table, bad_table, issues)
        """
        if isinstance(data, pa.RecordBatch):
            table = pa.Table.from_batches([data])
        else:
            table = data

        issues = validate_string_columns(table, check_nfc=self.check_nfc)

        if not issues:
            return table, table.schema.empty_table(), []

        # Efficiently split table using Arrow compute
        bad_mask = self.get_bad_row_mask(table)
        clean_table = table.filter(pc.invert(bad_mask))
        bad_table = table.filter(bad_mask)

        return clean_table, bad_table, issues

    @staticmethod
    def report_summary(summary: ValidationSummary) -> None:
        """Print a formatted summary of all detected issues.

        Args:
            summary: The ValidationSummary object containing accumulated results.
        """
        if summary.total_rows == 0:
            logger.info("No data processed by validator.")
            return

        if not summary.issue_counts:
            logger.success(
                "✓ Data Quality Pass: No issues detected across {} rows",
                summary.total_rows,
            )
            return

        logger.warning("=" * 80)
        logger.warning("DATA QUALITY VALIDATION SUMMARY")
        logger.warning("=" * 80)
        logger.warning(f"Total rows processed: {summary.total_rows}")
        logger.warning(f"Total bad rows:       {summary.total_bad_rows}")
        logger.warning(
            f"Bad row percentage:   "
            f"{(summary.total_bad_rows / summary.total_rows * 100):.2f}%"
        )

        logger.warning("-" * 80)
        logger.warning("Issues by Type:")

        # Sort issues by severity then count
        sorted_issues = sorted(
            summary.issue_counts.items(),
            key=lambda x: (x[0].split("|")[2] != Severity.CRITICAL.value, -x[1]),
        )

        for key, count in sorted_issues:
            issue_type, col, sev = key.split("|")
            log_level = "ERROR" if sev == Severity.CRITICAL.value else "WARNING"
            logger.log(
                log_level, f"  [{sev}] {issue_type} in column '{col}': {count} rows"
            )
            samples = summary.samples.get(key, [])
            if samples:
                logger.log(log_level, f"    Samples: {samples[:3]!r}")

        logger.warning("=" * 80)

    @staticmethod
    def verify_trailer(
        summary: ValidationSummary,
        trailer_counts: dict[str, int],
        exclude_types: list[str] | None = None,
    ) -> bool:
        """Verify record counts against trailer counts.

        Args:
            summary: The ValidationSummary object.
            trailer_counts: Expected counts from trailer.
            exclude_types: Record types to exclude from "extra record" warnings.

        Returns:
            True if all counts match, False otherwise.
        """
        if not trailer_counts:
            logger.warning("No trailer counts provided for verification.")
            return True

        exclude_types = exclude_types or []

        logger.info("=" * 80)
        logger.info("TRAILER RECORD VERIFICATION")
        logger.info("=" * 80)

        all_match = True
        for rt, expected in trailer_counts.items():
            actual = summary.record_counts.get(rt, 0)
            if actual == expected:
                logger.success(f"✓ {rt}: {actual} rows (matches trailer)")
            else:
                logger.error(
                    f"✗ {rt}: {actual} rows (expected {expected} from trailer)"
                )
                all_match = False

        # Check for extra record types in data not in trailer
        for rt, actual in summary.record_counts.items():
            if rt not in trailer_counts and rt not in exclude_types:
                logger.warning(f"! {rt}: {actual} rows (not found in trailer)")

        if all_match:
            logger.success("✅ Trailer verification passed!")
        else:
            logger.error("✗ Trailer verification failed!")

        return all_match

    def raise_if_failed(self, summary: ValidationSummary) -> None:
        """Raise DataQualityError if issues were found and action is 'reject'.

        Args:
            summary: The ValidationSummary object containing accumulated results.
        """
        if self.issue_action == "reject" and summary.total_bad_rows > 0:
            raise DataQualityError(
                f"Pipeline rejected due to {summary.total_bad_rows} "
                f"bad rows detected during validation."
            )
