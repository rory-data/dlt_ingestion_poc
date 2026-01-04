"""Data quality validator for Arrow-related data structures."""

from typing import Annotated, Literal

import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger

from ingestion.validate.config import DataQualityIssue, Severity
from ingestion.validate.utf8 import validate_string_columns


class Validator:
    """Data quality validator for Arrow-related data structures."""

    validator_type: Annotated[
        Literal["arrow", "syntactic", "semantic"],
        "This selects the type of validator to use. 'arrow' for PyArrow data structures, ",
    ] = "arrow"
    quarantine: bool = False
    reject: bool = False

    def __init__(
        self,
        validator_type: Literal["arrow", "syntactic", "semantic"] = "arrow",
        quarantine: bool = False,
        reject: bool = False,
    ) -> None:
        """Initialise the validator.

        Args:
            validator_type: The type of validation to perform.
            quarantine: Whether to quarantine bad records.
            reject: Whether to reject the entire batch if errors are found.
        """
        self.validator_type = validator_type
        self.quarantine = quarantine
        self.reject = reject

    @staticmethod
    def get_bad_rows(issues: list[DataQualityIssue]) -> set[int]:
        """Get all row indices that have any data quality issues.

        Args:
            issues: List of DataQualityIssue objects

        Returns:
            Set of row indices with issues
        """
        bad_rows = set()
        for issue in issues:
            bad_rows.update(issue.row_indices)
        return bad_rows

    @staticmethod
    def create_quarantine_filter(
        table: pa.Table, bad_row_indices: set[int]
    ) -> pa.Array:
        """Create a boolean mask for rows to KEEP (excluding bad rows).

        Args:
            table: PyArrow table (for row count)
            bad_row_indices: Set of row indices to exclude

        Returns:
            PyArrow boolean array (True = keep, False = quarantine)
        """
        num_rows = table.num_rows
        keep_mask = [i not in bad_row_indices for i in range(num_rows)]
        return pa.array(keep_mask, type=pa.bool_())

    @staticmethod
    def quarantine_bad_records(
        table: pa.Table, bad_row_indices: set[int]
    ) -> tuple[pa.Table, pa.Table]:
        """Split a table into clean and bad records.

        Args:
            table: PyArrow table to split
            bad_row_indices: Set of row indices to quarantine

        Returns:
            Tuple of (clean_table, bad_table)
        """
        keep_mask = Validator.create_quarantine_filter(table, bad_row_indices)
        clean_table = table.filter(keep_mask)
        bad_table = table.filter(pc.invert(keep_mask))  # type: ignore
        return clean_table, bad_table

    @staticmethod
    def get_issues_by_row(
        issues: list[DataQualityIssue],
    ) -> dict[int, list[DataQualityIssue]]:
        """Group issues by row index for detailed per-row reporting.

        Args:
            issues: List of DataQualityIssue objects

        Returns:
            Dictionary mapping row index to list of issues affecting that row
        """
        issues_by_row: dict[int, list[DataQualityIssue]] = {}
        for issue in issues:
            for row_idx in issue.row_indices:
                if row_idx not in issues_by_row:
                    issues_by_row[row_idx] = []
                issues_by_row[row_idx].append(issue)

        # Sort each row's issues by severity
        for row_issues in issues_by_row.values():
            row_issues.sort(key=lambda x: x.severity != Severity.ERROR)

        return dict(sorted(issues_by_row.items()))

    @staticmethod
    def report_issues(issues: list[DataQualityIssue]) -> None:
        """Print a formatted report of all detected data quality issues.

        Args:
            issues: List of DataQualityIssue objects to report
        """
        if not issues:
            logger.info("No data quality issues detected")
            return

        error_count = sum(1 for i in issues if i.severity == Severity.ERROR)
        warning_count = sum(1 for i in issues if i.severity == Severity.WARNING)
        bad_rows = Validator.get_bad_rows(issues)

        logger.warning(
            "Found {} error(s) and {} warning(s) affecting {} row(s)",
            error_count,
            warning_count,
            len(bad_rows),
        )

        # Build a mapping of row -> list of (issue, sample_value) tuples
        row_issues: dict[int, list[tuple[DataQualityIssue, str]]] = {}
        for issue in issues:
            for row_idx, sample_val in zip(
                issue.row_indices, issue.sample_values, strict=True
            ):
                if row_idx not in row_issues:
                    row_issues[row_idx] = []
                row_issues[row_idx].append((issue, sample_val))

        # Report grouped by row, sorted by severity (errors first)
        for row_idx in sorted(bad_rows):
            logger.warning(f"Row {row_idx}:")
            # Sort this row's issues: errors first, then warnings
            row_issue_list = sorted(
                row_issues[row_idx],
                key=lambda x: x[0].severity != Severity.ERROR,
            )
            for issue, sample_value in row_issue_list:
                log_level = "ERROR" if issue.severity == Severity.ERROR else "WARNING"
                msg = (
                    f"  [{issue.severity.value}] {issue.issue_type} in column "
                    f"'{issue.column}': {sample_value!r}"
                )
                logger.log(log_level, msg)

    def validate(self, data: pa.Table | pa.RecordBatch) -> list[DataQualityIssue]:
        """Validate the given PyArrow data structure for data quality issues.

        Args:
            data: PyArrow Table or RecordBatch to validate

        Returns:
            List of detected DataQualityIssue objects
        """
        if isinstance(data, pa.RecordBatch):
            table = pa.Table.from_batches([data])
        else:
            table = data

        match self.validator_type:
            case "arrow":
                issues = validate_string_columns(table)
            case _:
                raise TypeError(f"Unsupported validator type: {self.validator_type}")

        self.report_issues(issues)

        # Quarantine bad records for producer feedback
        if issues:
            bad_rows = self.get_bad_rows(issues)
            clean_table, bad_table = self.quarantine_bad_records(table, bad_rows)

            logger.info(
                "Total: {} rows | Clean: {} | Bad: {}",
                table.num_rows,
                clean_table.num_rows,
                bad_table.num_rows,
            )

        return issues
