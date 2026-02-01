"""Common type mappings for converting between ODCS, Arrow, and Python."""

import decimal
from datetime import date, datetime, time
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    pass

# ODCS Logical Types to Python Types
LOGICAL_TO_PY_TYPE_MAP: dict[str, type] = {
    "string": str,
    "date": date,
    "timestamp": datetime,
    "time": time,
    "number": float,
    "integer": int,
    "boolean": bool,
    "object": dict,
    "array": list,
}

# ODCS Logical Types to Arrow Types
LOGICAL_TO_ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "string": pa.string(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("ms"),
    "time": pa.time32("ms"),
    "number": pa.float64(),
    "integer": pa.int64(),
    "boolean": pa.bool_(),
    "object": pa.struct([]),  # Placeholder for complex types
    "array": pa.list_(pa.string()),  # Placeholder for complex types
}

# Python Types to Arrow Types
PY_TO_ARROW_TYPE_MAP: dict[type, pa.DataType] = {
    bytes: pa.binary(),
    float: pa.float64(),
    int: pa.int64(),
    decimal.Decimal: pa.decimal128(38, 10),  # default precision and scale
    str: pa.string(),
    date: pa.date32(),
    time: pa.time32("ms"),
    datetime: pa.timestamp("ms"),
    bool: pa.bool_(),
}
