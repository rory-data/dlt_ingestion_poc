"""Base protocols and types for data validation.

This module defines the interfaces and data structures for the validation layer.
All validators should implement the Validator protocol to enable composition
and pluggability in validation pipelines.
"""

from typing import Protocol

import pyarrow as pa
import pyarrow.compute as pc

from .result import ValidationResult


class Validator(Protocol):
    """Protocol for data validators.

    Validators are responsible for checking data quality and partitioning
    input into clean and bad data. Implementations should:

    1. Be stateless (no side effects)
    2. Be idempotent (validate(data) twice = same result)
    3. Return ValidationResult with clean/bad partitions
    4. Provide clear, actionable error messages

    Example:
        >>> validator = UTF8Validator()
        >>> result = validator.validate(my_table)
        >>> if result.is_valid:
        ...     loader.load(result.clean_data)
        ... else:
        ...     quarantine.save(result.bad_data)
    """

    def validate(self, batch: pa.RecordBatch) -> ValidationResult:
        """Validate a PyArrow RecordBatch.

        Args:
            batch: PyArrow RecordBatch to validate

        Returns:
            ValidationResult containing clean data, bad data, and issues

        Raises:
            ValueError: If table schema is invalid
            Exception: If validation fails due to infrastructure error
        """
        ...


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
        non_printable = pc.utf8_is_printable(col)
        # Invert and fill nulls (nulls are considered 'clean')
        is_bad_char = pc.invert(pc.fill_null(non_printable, True))

        # Replacement characters
        has_replacement = pc.match_substring(col, "\ufffd")
        has_replacement = pc.fill_null(has_replacement, False)

        bad_mask = pc.or_(bad_mask, is_bad_char)
        bad_mask = pc.or_(bad_mask, has_replacement)

    if isinstance(bad_mask, pa.ChunkedArray):
        bad_mask = bad_mask.combine_chunks()

    return bad_mask


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
    clean_table = table.filter(pc.invert(bad_mask))
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
