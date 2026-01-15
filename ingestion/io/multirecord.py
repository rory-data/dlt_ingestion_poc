"""Multi-record file format adapter for record type processing."""

import contextlib
from collections.abc import Iterator

import pyarrow as pa
from pyarrow import compute as pc


def parse_trailer_counts(batch: pa.RecordBatch | pa.Table) -> dict[str, int]:
    """Extract expected record counts from trailer records (type 'T').

    Trailer columns contain "TYPE-COUNT" format (e.g., "10-366", "20-123").
    The first column is assumed to be the record type indicator.

    Args:
        batch: PyArrow RecordBatch or Table with record type in first column.

    Returns:
        Dictionary mapping record type to expected count.

    Example:
        >>> trailer_batch = pa.RecordBatch.from_arrays(
        ...     [["T", "T"], ["10-100", "20-50"]],
        ...     names=["type", "counts"]
        ... )
        >>> parse_trailer_counts(trailer_batch)
        {'10': 100, '20': 50}
    """
    expected_counts = {}
    type_col = batch.column(0)
    trailer_mask = pc.equal(type_col, "T")  # type: ignore
    trailer_rows = batch.filter(trailer_mask)

    if trailer_rows.num_rows == 0:
        return {}

    # Iterate through data columns (skip the record type column)
    for i in range(1, trailer_rows.num_columns):
        col = trailer_rows.column(i)
        for val in col:
            if val.is_valid:
                s = val.as_py()
                if "-" in s:
                    with contextlib.suppress(ValueError):
                        rec_type, count_str = s.split("-", 1)
                        expected_counts[rec_type] = int(count_str)

    return expected_counts


def parse_trailer_records(batch: pa.RecordBatch | pa.Table) -> dict[str, int]:
    """Extract expected record counts from trailer records (alias for parse_trailer_counts).

    Args:
        batch: PyArrow RecordBatch or Table with record type in first column.

    Returns:
        Dictionary mapping record type to expected count.
    """
    return parse_trailer_counts(batch)


def partition_by_record_type(
    batch: pa.Table | pa.RecordBatch,
) -> Iterator[tuple[str, pa.Table | pa.RecordBatch]]:
    """Partition a batch by record type (first column).

    For homogeneous batches (all rows same type), yields once.
    For heterogeneous batches, yields separate partitions per type.

    Args:
        batch: PyArrow Table or RecordBatch with record type in first column.

    Yields:
        Tuples of (record_type, partition_data).
    """
    if batch.num_rows == 0:
        return

    type_col = batch.column(0)

    # Check if batch is homogeneous (all same type)
    first_type = type_col[0].as_py()
    last_type = type_col[-1].as_py()

    if first_type == last_type and pc.all(pc.equal(type_col, first_type)).as_py():  # type: ignore
        # All rows are the same type
        yield first_type, batch
        return

    # Batch is heterogeneous - partition by type
    for rt_scalar in type_col.unique():
        rt = rt_scalar.as_py()
        mask = pc.equal(type_col, rt)  # type: ignore
        filtered = batch.filter(mask)
        yield rt, filtered


def get_record_type_counts(batch: pa.Table | pa.RecordBatch) -> dict[str, int]:
    """Count rows by record type.

    Args:
        batch: PyArrow Table or RecordBatch with record type in first column.

    Returns:
        Dictionary mapping record type to row count.
    """
    counts = {}
    type_col = batch.column(0)

    for rt_scalar in type_col.unique():
        rt = rt_scalar.as_py()
        mask = pc.equal(type_col, rt)  # type: ignore
        filtered = batch.filter(mask)
        counts[rt] = filtered.num_rows

    return counts


def validate_record_counts(
    actual_counts: dict[str, int],
    expected_counts: dict[str, int],
) -> bool:
    """Validate that actual record counts match expected counts from trailer.

    Args:
        actual_counts: Dictionary mapping record type to actual count.
        expected_counts: Dictionary mapping record type to expected count.

    Returns:
        True if counts match, False otherwise.
    """
    for rec_type, expected in expected_counts.items():
        actual = actual_counts.get(rec_type, 0)
        if actual != expected:
            return False
    return True


def filter_expected_records(
    batch: pa.Table | pa.RecordBatch,
    valid_types: list[str],
) -> pa.Table | pa.RecordBatch:
    """Filter batch to only include records with types in valid_types.

    Args:
        batch: PyArrow Table or RecordBatch with record type in first column.
        valid_types: List of valid record type values.

    Returns:
        Filtered Table or RecordBatch.
    """
    type_col = batch.column(0)
    mask = pc.is_in(type_col, pa.array(valid_types))  # type: ignore
    return batch.filter(mask)
