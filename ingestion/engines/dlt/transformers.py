"""Arrow engine for columnar data operations and type casting."""

from typing import cast

import pyarrow as pa
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.libs.pyarrow import (
    cast_arrow_as_columns_schema,
    remove_columns,
    rename_columns,
)
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from loguru import logger
from pyarrow import compute as pc


def cast_columns_to_dlt_types(
    table: pa.Table | pa.RecordBatch,
    schema_cols: list[dict],
    caps: DestinationCapabilitiesContext | None = None,
    tz: str = "UTC",
) -> tuple[list[pa.Array], bool]:
    """Cast table columns to target dlt types with standardization.

    For each column, applies the schema definition, casts to the corresponding
    Arrow type, and applies UTF-8 standardization for strings.

    Args:
        table: PyArrow Table or RecordBatch to cast.
        schema_cols: List of column schema dicts with 'data_type' keys.
        caps: Destination capabilities context (defaults to generic capabilities).
        tz: Timezone for timestamp columns (default: "UTC").

    Returns:
        Tuple of (casted_arrays, success_flag) where success_flag indicates
        whether all casts succeeded.
    """
    if caps is None:
        caps = DestinationCapabilitiesContext.generic_capabilities()

    # Build table schema columns from the schema_cols list
    columns: TTableSchemaColumns = {
        table.schema.names[i]: cast(TColumnSchema, col_def)
        for i, col_def in enumerate(schema_cols)
        if i < table.num_columns
    }

    try:
        # Use dlt's casting function which handles type conversion, fallbacks,
        # timezone normalization, and error handling
        casted_table = cast_arrow_as_columns_schema(
            table, columns, caps, tz, safe_arrow_conversion=True
        )

        # Apply standardization for string columns
        casted_arrays = []
        for i, field in enumerate(casted_table.schema):
            col = casted_table.column(i)
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                col = standardise_string_column(col)
            casted_arrays.append(col)

        return casted_arrays, True

    except Exception as e:
        logger.error(f"Column casting failed: {e}", exc_info=True)
        return [], False


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
    """Remove the first column from a table or batch.

    Useful for removing record type markers. Uses dlt's remove_columns utility.

    Args:
        table: PyArrow Table or RecordBatch.

    Returns:
        Table or RecordBatch with first column removed.
    """
    if table.num_columns <= 1:
        raise ValueError("Cannot drop first column: table has only one column")
    first_col_name = table.schema.names[0]
    return remove_columns(table, [first_col_name])


def select_and_rename_columns(
    table: pa.Table | pa.RecordBatch,
    target_names: list[str],
) -> pa.Table | pa.RecordBatch:
    """Select columns up to the number of target names and rename them.

    Uses dlt's rename_columns utility for robust handling of both Table and RecordBatch.

    Args:
        table: PyArrow Table or RecordBatch.
        target_names: Target column names.

    Returns:
        Table or RecordBatch with selected and renamed columns.
    """
    available_cols = min(table.num_columns, len(target_names))
    selected = table.select(list(range(available_cols)))
    return rename_columns(selected, target_names[:available_cols])


def standardise_string_column(col: pa.Array) -> pa.Array:
    """Standardises a single string column in a PyArrow Array.

    This function trims leading and trailing whitespace then applies NFC normalisation
    to the string data.

    Args:
        col: The PyArrow Array representing the string column.

    Returns:
        An updated PyArrow Array with standardised string data.
    """
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
