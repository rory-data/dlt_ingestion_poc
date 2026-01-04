"""UTF-8 data quality validation for string columns."""

import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger

from ingestion.validate.common import extract_issue_summary, validate_arrow_structures
from ingestion.validate.config import REPLACEMENT_CHAR, DataQualityIssue, Severity


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


def _detect_non_nfc_normalization(
    column: pa.ChunkedArray, column_name: str
) -> list[DataQualityIssue]:
    """Detect rows that are not in NFC normalized form.

    Args:
        column: PyArrow column to check
        column_name: Name of the column for reporting

    Returns:
        List of DataQualityIssue objects with severity WARNING
    """
    try:
        # Note: This is computationally expensive.
        normalized = pc.utf8_normalize(column, form="NFC")
        mask = pc.not_equal(column, normalized)
        issue_count, sample_values = extract_issue_summary(column, mask)

        if issue_count == 0:
            return []

        return [
            DataQualityIssue(
                severity=Severity.WARNING,
                issue_type="Non-NFC Normalization",
                column=column_name,
                issue_count=issue_count,
                sample_values=sample_values,
            )
        ]
    except Exception as e:
        logger.debug(f"NFC normalization check skipped for {column_name}: {e}")
        return []


def validate_string_columns(
    table: pa.Table, check_nfc: bool = False
) -> list[DataQualityIssue]:
    """
    Validate all string columns in a PyArrow table for data quality issues.

    Args:
        table: PyArrow table to validate
        check_nfc: Whether to perform expensive NFC normalization check

    Returns:
        List of all detected DataQualityIssue objects, sorted by severity
    """
    issues: list[DataQualityIssue] = []

    validate_arrow_structures(table)
    for name in table.schema.names:
        col = table.column(name)

        if not (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)):
            continue

        issues.extend(_detect_invalid_characters(col, name))
        issues.extend(_detect_replacement_characters(col, name))
        if check_nfc:
            issues.extend(_detect_non_nfc_normalization(col, name))

    # Sort by severity (CRITICAL first) then by column name
    issues.sort(key=lambda x: (x.severity != Severity.CRITICAL, x.column))
    return issues
