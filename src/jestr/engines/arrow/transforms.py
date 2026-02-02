"""Minor Arrow transform utilities for casting and column manipulation."""

import pyarrow as pa
import pyarrow.compute as pc
import structlog

logger = structlog.get_logger()


def rename_columns(data: pa.RecordBatch, column_names: list[str]) -> pa.RecordBatch:
    """Rename columns of a PyArrow RecordBatch."""
    available_cols = min(data.num_columns, len(column_names))
    if available_cols == 0:
        return data

    target_names = column_names[:available_cols]
    selected = data.select(list(range(available_cols)))

    return pa.RecordBatch.from_arrays(
        [selected.column(i) for i in range(available_cols)], names=target_names
    )


def cast_single_column(
    col: pa.Array | pa.ChunkedArray,
    target_type: pa.DataType,
) -> pa.Array:
    """Cast a single column to a target Arrow type.

    Args:
        col: PyArrow Array or ChunkedArray to cast.
        target_type: Target PyArrow data type.

    Returns:
        Casted PyArrow Array.
    """
    casted = pc.cast(col, target_type)
    if isinstance(casted, pa.ChunkedArray):
        casted = casted.combine_chunks()
    return casted


def create_casted_table(
    casted_arrays: list[pa.Array],
    original_names: list[str],
) -> pa.Table:
    """Create a PyArrow table from casted arrays and original column names.

    Args:
        casted_arrays: List of PyArrow arrays.
        original_names: Original column names.

    Returns:
        PyArrow Table.
    """
    return pa.Table.from_arrays(casted_arrays, names=original_names)


def drop_first_column(table: pa.Table | pa.RecordBatch) -> pa.Table | pa.RecordBatch:
    """Remove the first column from a table or batch."""
    if table.num_columns <= 1:
        raise ValueError("Cannot drop first column: table has only one column")
    return table.select(list(range(1, table.num_columns)))


def select_and_rename_columns(
    data: pa.RecordBatch,
    target_names: list[str],
) -> pa.RecordBatch:
    """Select columns up to the number of target names and rename them.

    Uses dlt's rename_columns utility for robust handling of both Table and RecordBatch.

    Args:
        data: PyArrow RecordBatch.
        target_names: Target column names.

    Returns:
        RecordBatch with selected and renamed columns.
    """
    available_cols = min(data.num_columns, len(target_names))
    selected = data.select(list(range(available_cols)))
    return rename_columns(selected, target_names[:available_cols])


def standardise_string_column(col: pa.Array) -> pa.Array:
    """Standardises a single string column in a PyArrow Array."""
    if not (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)):
        return col

    try:
        col = pc.utf8_trim_whitespace(col)  # type: ignore
        col = pc.utf8_normalize(col, form="NFC")  # type: ignore
    except Exception as e:
        logger.exception("Error standardising string column")
        if isinstance(e, pa.ArrowInvalid):
            raise
        raise pa.ArrowInvalid(f"Error standardising string column: {e}") from e
    return col
