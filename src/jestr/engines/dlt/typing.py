"""Type casting for dlt pipelines."""

from typing import cast

import pyarrow as pa
import structlog
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.libs.pyarrow import (
    cast_arrow_as_columns_schema,
)
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns

from jestr.engines.arrow.transforms import standardise_string_column

logger = structlog.get_logger()


def cast_columns_to_dlt_types(
    data: pa.RecordBatch,
    schema_cols: list[dict],
    caps: DestinationCapabilitiesContext | None = None,
    tz: str = "UTC",
) -> tuple[list[pa.Array], bool]:
    """Cast table columns to target dlt types with standardisation."""
    if caps is None:
        caps = DestinationCapabilitiesContext.generic_capabilities()

    # Build table schema columns from the schema_cols list
    columns: TTableSchemaColumns = {
        data.schema.names[i]: cast(TColumnSchema, col_def)
        for i, col_def in enumerate(schema_cols)
        if i < data.num_columns
    }

    try:
        # Use dlt's casting function which handles type conversion, fallbacks,
        # timezone normalization, and error handling
        casted_data = cast_arrow_as_columns_schema(
            data, columns, caps, tz, safe_arrow_conversion=True
        )

        # Apply standardisation for string columns
        casted_arrays = []
        for i, field in enumerate(casted_data.schema):
            col = casted_data.column(i)
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                col = standardise_string_column(col)
            casted_arrays.append(col)

        return casted_arrays, True

    except Exception as e:
        logger.error(f"Column casting failed: {e}", exc_info=True)
        return [], False
