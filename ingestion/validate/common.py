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


def extract_issue_summary(
    column: pa.ChunkedArray, mask: pa.Array | pa.ChunkedArray, max_samples: int = 5
) -> tuple[int, list[str]]:
    """Extract issue count and a small number of sample values.

    Args:
        column: The column to extract values from.
        mask: Boolean mask where True indicates an issue.
        max_samples: Maximum number of sample values to extract.

    Returns:
        Tuple of (issue_count, sample_values).
    """
    # Use Arrow compute to get count without bringing data to Python
    issue_count = pc.sum(mask).as_py()
    if issue_count == 0:
        return 0, []

    # Only extract a few samples for reporting
    indices = pc.indices_nonzero(mask)
    if isinstance(indices, pa.ChunkedArray):
        indices = indices.combine_chunks()

    sample_indices = indices.slice(0, max_samples)
    problematic_samples = pc.take(column, sample_indices)

    if isinstance(problematic_samples, pa.ChunkedArray):
        problematic_samples = problematic_samples.combine_chunks()

    sample_values = [val.as_py() for val in problematic_samples]
    return issue_count, sample_values
