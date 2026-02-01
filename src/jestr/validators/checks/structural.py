"""PyArrow Compute checks for structural data quality issues."""

import pyarrow as pa
import pyarrow.compute as pc
import structlog

from ..core import DataQualityIssue, Severity

logger = structlog.get_logger()


def check_replacement_char(
    column: pa.ChunkedArray, column_name: str
) -> tuple[pa.Array, DataQualityIssue | None]:
    """Check for Unicode replacement characters (U+FFFD) indicating decode failures.

    Returns:
        Tuple of (mask, issue) where mask is True for bad rows, issue is None if no problems found
    """
    replacement_char = "\ufffd"

    try:
        # Check for Unicode replacement characters
        replacement_mask = pc.match_substring(column, replacement_char)
        replacement_mask = pc.fill_null(replacement_mask, False)

        if pc.any(replacement_mask).as_py():
            # Extract sample rows with this issue
            matching_values = pc.filter(column, replacement_mask)
            samples = [
                matching_values[i].as_py() for i in range(min(3, len(matching_values)))
            ]

            issue = DataQualityIssue(
                severity=Severity.WARNING,
                issue_type="Unicode Replacement Character (U+FFFD)",
                column=column_name,
                issue_count=pc.sum(pc.cast(replacement_mask, pa.int64())).as_py(),  # type: ignore
                sample_values=samples,
            )
            return replacement_mask, issue

        return replacement_mask, None

    except pa.ArrowException as exc:
        logger.exception(
            "Arrow column validation failed.",
            column=column_name,
            error_details=str(exc),
        )
        # Return empty mask on error
        return pa.repeat(pa.scalar(False, type=pa.bool_()), len(column)), None


def check_invalid_chars(
    column: pa.ChunkedArray, column_name: str
) -> tuple[pa.Array, DataQualityIssue | None]:
    """Check for control or non-printable characters.

    Returns:
        Tuple of (mask, issue) where mask is True for bad rows, issue is None if no problems found
    """
    try:
        # Check for non-printable characters
        printable_mask = pc.utf8_is_printable(column)
        non_printable_mask = pc.invert(pc.fill_null(printable_mask, True))

        if pc.any(non_printable_mask).as_py():
            matching_values = pc.filter(column, non_printable_mask)
            samples = [
                matching_values[i].as_py() for i in range(min(3, len(matching_values)))
            ]

            issue = DataQualityIssue(
                severity=Severity.CRITICAL,
                issue_type="Invalid Characters (control or non-printable)",
                column=column_name,
                issue_count=pc.sum(pc.cast(non_printable_mask, pa.int64())).as_py(),
                sample_values=samples,
            )
            return non_printable_mask, issue

        return non_printable_mask, None

    except pa.ArrowException as exc:
        logger.exception(
            "Arrow column validation failed.",
            column=column_name,
            error_details=str(exc),
        )
        # Return empty mask on error
        return pa.repeat(pa.scalar(False, type=pa.bool_()), len(column)), None


def validate_string_column(
    column: pa.ChunkedArray, column_name: str
) -> tuple[pa.Array, list[DataQualityIssue]]:
    """Run all structural checks on a string column in a single pass.

    Returns:
        Tuple of (bad_row_mask, issues) where mask is True for any row with issues
    """
    bad_mask = pa.repeat(pa.scalar(False, type=pa.bool_()), len(column))
    issues: list[DataQualityIssue] = []

    # Run all checks and accumulate results
    checks = [
        check_invalid_chars,
        check_replacement_char,
        # Add new checks here - they'll automatically be included in single pass
    ]

    for check_func in checks:
        mask, issue = check_func(column, column_name)
        bad_mask = pc.or_(bad_mask, mask)
        if issue is not None:
            issues.append(issue)

    return bad_mask, issues
