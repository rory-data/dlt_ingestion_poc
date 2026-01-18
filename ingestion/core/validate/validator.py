"""Data quality validator for Arrow-related data structures."""

from typing import Annotated, Literal

import pyarrow as pa
from loguru import logger
from pyarrow import compute as pc

from ingestion.core.reporting.config import DataQualityIssue, Severity
from ingestion.core.reporting.validation import DataQualityError, ValidationSummary

from .base import ValidationResult, Validator
from .utf8 import validate_string_columns

# ============================================================================
# PURE FUNCTIONS (Functional Core)
# ============================================================================


def _get_bad_row_mask(table: pa.Table) -> pa.Array:
    """Create a boolean mask for rows that have any data quality issues.

    Pure function: takes a table, returns a mask. No side effects.

    Uses Arrow compute functions to find rows with invalid characters
    or replacement characters.

    Args:
        table: PyArrow Table to check

    Returns:
        Boolean array where True indicates a bad row
    """
    # Start with all False (no issues)
    bad_mask = pa.repeat(pa.scalar(False, type=pa.bool_()), table.num_rows)

    # For string columns, check for invalid characters and replacement characters
    for name in table.schema.names:
        col = table.column(name)
        if not (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)):
            continue

        # Non-printable characters (fills nulls as valid)
        non_printable = pc.utf8_is_printable(col)  # type: ignore
        # Invert and fill nulls (nulls are considered 'clean')
        is_bad_char = pc.invert(pc.fill_null(non_printable, True))  # type: ignore

        # Replacement characters
        has_replacement = pc.match_substring(col, "\ufffd")  # type: ignore
        has_replacement = pc.fill_null(has_replacement, False)

        bad_mask = pc.or_(bad_mask, is_bad_char)  # type: ignore
        bad_mask = pc.or_(bad_mask, has_replacement)  # type: ignore

    if isinstance(bad_mask, pa.ChunkedArray):
        bad_mask = bad_mask.combine_chunks()

    return bad_mask


def _validate_arrow_structures(table: pa.Table) -> None:
    """Validate the integrity of Arrow table structures.

    Pure function: validates or raises. No side effects except logging/errors.

    Args:
        table: PyArrow table to validate

    Raises:
        DataQualityError: If Arrow table structure is invalid
    """
    try:
        table.validate(full=True)
    except pa.ArrowInvalid as e:
        logger.error(f"Arrow table validation failed: {e}")
        raise DataQualityError(f"Arrow table validation failed: {e}") from e


def _partition_by_validity(
    table: pa.Table, bad_mask: pa.Array
) -> tuple[pa.Table, pa.Table]:
    """Partition a table into clean and bad data.

    Pure function: deterministic transformation.

    Args:
        table: PyArrow Table to partition
        bad_mask: Boolean array where True = bad row

    Returns:
        Tuple of (clean_table, bad_table)
    """
    clean_table = table.filter(pc.invert(bad_mask))  # type: ignore
    bad_table = table.filter(bad_mask)
    return clean_table, bad_table


def _convert_to_original_format(
    table: pa.Table, is_record_batch: bool, bad_mask: pa.Array
) -> tuple[pa.Table | pa.RecordBatch, pa.Table | pa.RecordBatch]:
    """Convert table results back to original format (Table or RecordBatch).

    Pure function: deterministic transformation.

    Args:
        table: Original PyArrow Table
        is_record_batch: Whether input was a RecordBatch
        bad_mask: Boolean array where True = bad row

    Returns:
        Tuple of (clean_data, bad_data) in original format
    """
    clean_table, bad_table = _partition_by_validity(table, bad_mask)

    if not is_record_batch:
        return clean_table, bad_table

    # Convert to RecordBatch, handling empty results
    clean_batches = clean_table.to_batches()
    bad_batches = bad_table.to_batches()

    clean_data = (
        clean_batches[0]
        if clean_batches
        else pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in table.schema],
            schema=table.schema,
        )
    )

    bad_data = (
        bad_batches[0]
        if bad_batches
        else pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in table.schema],
            schema=table.schema,
        )
    )

    return clean_data, bad_data


# ============================================================================
# VALIDATOR CLASS (Imperative Shell)
# ============================================================================


class UTF8Validator:
    """UTF-8 data quality validator implementing the Validator protocol.

    This validator checks for:
    - Non-printable characters in string columns
    - Unicode replacement characters (U+FFFD)

    Configuration:
        issue_action: 'reject' (raise error) or 'quarantine' (separate bad data)
        check_nfc: Whether to perform expensive NFC normalization (not yet implemented)

    Example:
        >>> validator = UTF8Validator(issue_action="quarantine")
        >>> result = validator.validate(my_table)
        >>> print(f"Clean: {result.clean_row_count}, Bad: {result.bad_row_count}")
    """

    def __init__(
        self,
        issue_action: Annotated[
            Literal["reject", "quarantine"],
            "Action to take: 'reject' blocks pipeline, 'quarantine' separates bad data",
        ] = "reject",
        check_nfc: bool = False,
    ) -> None:
        """Initialise the UTF-8 validator.

        Args:
            issue_action: Action when issues found ('reject' or 'quarantine')
            check_nfc: Whether to perform NFC normalization check (future)
        """
        self.issue_action = issue_action
        self.check_nfc = check_nfc

    def validate(self, data: pa.Table | pa.RecordBatch) -> ValidationResult:
        """Validate a PyArrow Table or RecordBatch for UTF-8 issues.

        Args:
            data: PyArrow Table or RecordBatch to validate

        Returns:
            ValidationResult with clean data, bad data, and issues

        Raises:
            DataQualityError: If issue_action is 'reject' and issues found
        """
        is_record_batch = isinstance(data, pa.RecordBatch)
        table = pa.Table.from_batches([data]) if is_record_batch else data

        # Validate Arrow structures
        _validate_arrow_structures(table)

        # Check for UTF-8 issues (pure function call)
        issues = validate_string_columns(table)

        # Handle case with no issues
        if not issues:
            empty_data = table.schema.empty_table()
            if is_record_batch:
                empty_batch = pa.RecordBatch.from_arrays(
                    [pa.array([], type=field.type) for field in table.schema],
                    schema=table.schema,
                )
                return ValidationResult(
                    clean_data=data,
                    bad_data=empty_batch,
                    issues=[],
                )
            return ValidationResult(
                clean_data=table,
                bad_data=empty_data,
                issues=[],
            )

        # Partition into clean and bad data (pure function calls)
        bad_mask = _get_bad_row_mask(table)
        clean_data, bad_data = _convert_to_original_format(
            table, is_record_batch, bad_mask
        )

        result = ValidationResult(
            clean_data=clean_data,
            bad_data=bad_data,
            issues=issues,
        )

        # Raise error if configured to reject on issues
        if self.issue_action == "reject" and not result.is_valid:
            raise DataQualityError(
                f"Validation failed with {len(issues)} issues. "
                f"Bad rows: {result.bad_row_count}/{result.total_row_count}"
            )

        return result

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


# Backward compatibility alias
ArrowValidator = UTF8Validator
