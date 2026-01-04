"""Common utilities for data quality validation."""

import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger


def validate_arrow_structures(table: pa.Table) -> None:
    """Validate the integrity of PyArrow structures.

    Args:
        table: PyArrow table to validate

    Raises:
        pa.ArrowInvalid: If the table structure is invalid
    """
    try:
        table.validate(full=True)
    except pa.ArrowInvalid as e:
        logger.exception(e)
        raise


def extract_issue_details(
    column: pa.ChunkedArray, mask: pa.Array | pa.ChunkedArray
) -> tuple[list[int], list[str]]:
    """Extract row indices and sample values for rows matching the mask.

    Args:
        column: The column to extract values from.
        mask: Boolean mask where True indicates an issue.

    Returns:
        Tuple of (row_indices, sample_values).
    """
    indices = pc.indices_nonzero(mask)  # type: ignore
    if len(indices) == 0:
        return [], []

    problematic_rows = pc.filter(column, mask)  # type: ignore
    sample_values = [val.as_py() for val in problematic_rows.combine_chunks()]
    row_indices = (
        indices.to_pylist()
        if isinstance(indices, pa.Array)
        else indices.combine_chunks().to_pylist()
    )
    return row_indices, sample_values
