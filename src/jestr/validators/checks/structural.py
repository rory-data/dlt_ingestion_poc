"""PyArrow Compute checks for structural data quality issues."""

import pyarrow as pa
import pyarrow.compute as pc
import structlog

from ..core import DataQualityIssue

logger = structlog.get_logger()


def contains_no_replacement_char(
    column: pa.ChunkedArray, column_name: str
) -> list[DataQualityIssue]:
    """Validate string column for Unicode replacement characters (U+FFFD) indicating decode failures."""
    issues: list[DataQualityIssue] = []
    replacement_char = "\ufffd"

    try:
        # Check for Unicode replacement characters
        replacement_mask = pc.match_substring(column, replacement_char)
        if pc.any(replacement_mask).as_py():
            # Extract sample rows with this issue
            matching_values = pc.filter(column, replacement_mask)
            samples = [
                matching_values[i].as_py() for i in range(min(3, len(matching_values)))
            ]

        issues.append(
            DataQualityIssue(
                severity="WARNING",
                issue_type="Unicode Replacement Character (U+FFFD)",
                column=column_name,
                issue_count=pc.sum(pc.cast(replacement_mask, pa.int64())).as_py(),  # type: ignore
                sample_values=samples,
            )
        )
    except pa.ArrowException as exc:
        logger.exception(
            "Arrow column validation failed.",
            column=column_name,
            error_details=str(exc),
        )

    return issues


def contains_no_invalid_chars(
    column: pa.ChunkedArray, column_name: str
) -> list[DataQualityIssue]:
    """Validate string column for control or non-printable characters."""
    issues: list[DataQualityIssue] = []

    try:
        # Check for non-printable characters
        printable_mask = pc.utf8_is_printable(column)
        non_printable_mask = pc.invert(printable_mask)
        if pc.any(non_printable_mask).as_py():
            matching_values = pc.filter(column, non_printable_mask)
            samples = [
                matching_values[i].as_py() for i in range(min(3, len(matching_values)))
            ]

            issues.append(
                DataQualityIssue(
                    severity="CRITICAL",
                    issue_type="Invalid Characters (control or non-printable)",
                    column=column_name,
                    issue_count=pc.sum(pc.cast(non_printable_mask, pa.int64())).as_py(),
                    sample_values=samples,
                )
            )
    except pa.ArrowException as exc:
        logger.exception(
            "Arrow column validation failed.",
            column=column_name,
            error_details=str(exc),
        )

    return issues
