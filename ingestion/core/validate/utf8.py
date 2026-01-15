"""UTF-8 data quality validation for string columns."""

import pyarrow as pa
from pyarrow import compute as pc

from ingestion.core.reporting.config import DataQualityIssue, Severity
from ingestion.core.reporting.validation import extract_issue_summary

# CONSTANTS
REPLACEMENT_CHAR = "\ufffd"  # Unicode replacement character to detect encoding issues


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
    mask = pc.match_substring(column, REPLACEMENT_CHAR)  # type: ignore
    issue_count, sample_values = extract_issue_summary(column, mask)

    if issue_count == 0:
        return []

    return [
        DataQualityIssue(
            severity=Severity.WARNING,
            issue_type="Unicode Replacement Character (U+FFFD)",
            column=column_name,
            issue_count=issue_count,
            sample_values=sample_values,
        )
    ]


def _detect_invalid_characters(
    column: pa.ChunkedArray, column_name: str
) -> list[DataQualityIssue]:
    """Detect control or non-printable characters in a column.

    Args:
        column: PyArrow column to check
        column_name: Name of the column for reporting

    Returns:
        List of DataQualityIssue objects with severity CRITICAL
    """
    # Detect non-printable characters
    printable_mask = pc.utf8_is_printable(column)  # type: ignore
    non_printable_mask = pc.invert(printable_mask)  # type: ignore
    issue_count, sample_values = extract_issue_summary(column, non_printable_mask)

    if issue_count == 0:
        return []

    return [
        DataQualityIssue(
            severity=Severity.CRITICAL,
            issue_type="Invalid Characters (control or non-printable)",
            column=column_name,
            issue_count=issue_count,
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

    for name in table.schema.names:
        col = table.column(name)

        if not (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)):
            continue

        issues.extend(_detect_invalid_characters(col, name))
        issues.extend(_detect_replacement_characters(col, name))

    # Sort by severity (CRITICAL first) then by column name
    issues.sort(key=lambda x: (x.severity != Severity.CRITICAL, x.column))
    return issues
