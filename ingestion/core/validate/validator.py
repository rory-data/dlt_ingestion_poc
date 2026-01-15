"""Data quality validator for Arrow-related data structures."""

from typing import Annotated, Literal

import pyarrow as pa
from loguru import logger
from pyarrow import compute as pc

from ingestion.core.reporting.config import DataQualityIssue, Severity
from ingestion.core.reporting.validation import DataQualityError, ValidationSummary

from .utf8 import validate_string_columns


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

    @staticmethod
    def _get_bad_row_mask(table: pa.Table) -> pa.Array:
        """Create a boolean mask for rows that have any data quality issues.

        Uses Arrow compute functions to find rows with invalid characters
        or replacement characters.
        """
        # Start with all False (no issues) using a constant scalar repeated to the table size
        bad_mask = pa.repeat(pa.scalar(False, type=pa.bool_()), table.num_rows)

        # For string columns, check for invalid characters and replacement characters
        for name in table.schema.names:
            col = table.column(name)
            if not (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)):
                continue

            # Non-printable characters (fills nulls as valid - non-printable check handles this)
            non_printable = pc.utf8_is_printable(col)  # type: ignore
            # Invert and fill nulls (nulls are considered 'clean' for this check)
            is_bad_char = pc.invert(pc.fill_null(non_printable, True))  # type: ignore

            # Replacement characters
            has_replacement = pc.match_substring(col, "\ufffd")  # type: ignore
            has_replacement = pc.fill_null(has_replacement, False)

            bad_mask = pc.or_(bad_mask, is_bad_char)  # type: ignore
            bad_mask = pc.or_(bad_mask, has_replacement)  # type: ignore

        if isinstance(bad_mask, pa.ChunkedArray):
            bad_mask = bad_mask.combine_chunks()

        return bad_mask

    @staticmethod
    def _validate_arrow_structures(table: pa.Table) -> None:
        """Validate the integrity of Arrow table structures.

        Args:
            table: PyArrow table to validate
        """
        try:
            table.validate(full=True)
        except pa.ArrowInvalid as e:
            logger.error(f"Arrow table validation failed: {e}")
            raise DataQualityError(f"Arrow table validation failed: {e}") from e

    def validate(
        self, data: pa.Table | pa.RecordBatch
    ) -> tuple[pa.Table, pa.Table, list[DataQualityIssue]]:
        """Validate the given PyArrow data structure for data quality issues.

        Args:
            data: PyArrow Table or RecordBatch to validate

        Returns:
            Tuple of (clean_table, bad_table, issues)
        """
        is_record_batch = isinstance(data, pa.RecordBatch)
        table = pa.Table.from_batches([data]) if is_record_batch else data

        self._validate_arrow_structures(table)
        issues = validate_string_columns(table)

        if not issues:
            # No issues - all data is clean, no bad data
            empty_data = table.schema.empty_table()
            if is_record_batch:
                # Create empty RecordBatch with the same schema
                empty_batch = pa.RecordBatch.from_arrays(
                    [pa.array([], type=field.type) for field in table.schema],
                    schema=table.schema,
                )
                return (data, empty_batch, [])

            return table, empty_data, []

        # Efficiently split table using Arrow compute
        bad_mask = self._get_bad_row_mask(table)
        clean_table = table.filter(pc.invert(bad_mask))  # ty: ignore[unresolved-attribute]
        bad_table = table.filter(bad_mask)

        if is_record_batch:
            # Convert tables to batches, handling empty results
            clean_batches = clean_table.to_batches()
            bad_batches = bad_table.to_batches()

            # Create empty batch with proper schema if no rows
            if not clean_batches:
                clean_data = pa.RecordBatch.from_arrays(
                    [pa.array([], type=field.type) for field in table.schema],
                    schema=table.schema,
                )
            else:
                clean_data = clean_batches[0]

            if not bad_batches:
                bad_data = pa.RecordBatch.from_arrays(
                    [pa.array([], type=field.type) for field in table.schema],
                    schema=table.schema,
                )
            else:
                bad_data = bad_batches[0]
            return clean_data, bad_data, issues

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

        if not summary.issue_counts:
            logger.success("No validation issues detected.")
        else:
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
