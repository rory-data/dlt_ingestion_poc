"""UTF-8 data quality validation for string columns."""

import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger

from ingestion.validate.common import extract_issue_details, validate_arrow_structures
from ingestion.validate.config import DataQualityIssue, Rules, Severity


def _detect_replacement_characters(
    column: pa.ChunkedArray, column_name: str
) -> list[DataQualityIssue]:
    """Detect rows containing the Unicode replacement character U+FFFD.

    Args:
        column: PyArrow column to check
        column_name: Name of the column for reporting

    Returns:
        List of DataQualityIssue objects with severity WARNING
    """
    mask = pc.match_substring(column, Rules.REPLACEMENT_CHAR)  # type: ignore
    row_indices, sample_values = extract_issue_details(column, mask)

    if not row_indices:
        return []

    return [
        DataQualityIssue(
            severity=Severity.WARNING,
            issue_type="Unicode Replacement Character (U+FFFD)",
            column=column_name,
            row_indices=row_indices,
            sample_values=sample_values,
        )
    ]


def _detect_invalid_characters(
    column: pa.ChunkedArray, column_name: str
) -> list[DataQualityIssue]:
    """Detect control or non-printable characters in a column.

    This combines detection of:
    - Control characters (0x00-0x08, 0x0B-0x0C, 0x0E-0x1F, 0x7F-0x9F)
    - Non-printable characters (including the above)

    Args:
        column: PyArrow column to check
        column_name: Name of the column for reporting

    Returns:
        List of DataQualityIssue objects with severity ERROR
    """
    # Detect non-printable characters (catches control chars and other invalid chars)
    printable_mask = pc.utf8_is_printable(column)  # type: ignore
    non_printable_mask = pc.invert(printable_mask)  # type: ignore
    row_indices, sample_values = extract_issue_details(column, non_printable_mask)

    if not row_indices:
        return []

    return [
        DataQualityIssue(
            severity=Severity.ERROR,
            issue_type="Invalid Characters (control or non-printable)",
            column=column_name,
            row_indices=row_indices,
            sample_values=sample_values,
        )
    ]


def validate_string_columns(table: pa.Table) -> list[DataQualityIssue]:
    """
    Validate all string columns in a PyArrow table for data quality issues.

    Args:
        table: PyArrow table to validate

    Returns:
        List of all detected DataQualityIssue objects, sorted by severity
    """
    issues: list[DataQualityIssue] = []

    logger.info("Validating all string columns for UTF-8 data quality issues")

    validate_arrow_structures(table)
    for name in table.schema.names:
        col = table.column(name)

        if not (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)):
            continue

        issues.extend(_detect_invalid_characters(col, name))
        issues.extend(_detect_replacement_characters(col, name))

    # Sort by severity (ERROR first) then by column name
    issues.sort(key=lambda x: (x.severity != Severity.ERROR, x.column))
    return issues
