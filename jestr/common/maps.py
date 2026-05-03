"""Common type mappings for converting between ODCS, Arrow, and Python."""

import decimal
from datetime import date, datetime, time

import pyarrow as pa

from jestr.common.constants import DecimalDefaults

# ODCS Logical Types to Python Types
LOGICAL_TO_PY_TPYE_MAP: dict[str, type] = {
    "date": date,
    "timestamp": datetime,
    "time": time,
    "string": str,
    "number": float,
    "integer": int,
    "boolean": bool,
    "onject": dict,
    "array": list,
}

# ODCS Logical Types to Arrow Types
LOGICAL_TO_ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "date": pa.date32(),
    "timestamp": pa.timestamp("ms"),
    "time": pa.time32("ms"),
    "string": pa.string(),
    "number": pa.float64(),
    "integer": pa.int64(),
    "boolean": pa.bool_(),
    "object": pa.struct([]),  # Placeholder for complex types
    "array": pa.list_(pa.string()),  # Placeholder for complex types
}

# Python Types to Arrow Types
PY_TO_ARROW_TYPE_MAP: dict[type, pa.DataType] = {
    bytes: pa.binary(),
    date: pa.date32(),
    time: pa.time32("ms"),
    datetime: pa.timestamp("ms"),
    str: pa.string(),
    float: pa.float64(),
    int: pa.int64(),
    bool: pa.bool_(),
    dict: pa.struct([]),  # Placeholder for complex types
    list: pa.list_(pa.string()),  # Placeholder for complex types
    decimal.Decimal: pa.decimal128(DecimalDefaults.PRECISION, DecimalDefaults.SCALE),
}

# Type pairs where ODCS logicalType and physicalType are trivially equivalent
TRIVIAL_PAIRS: frozenset[tuple[str, str]] = frozenset(
    {
        ("VARCHAR2", "string"),
        ("VARCHAR", "string"),
        ("NVARCHAR2", "string"),
        ("NVARCHAR", "string"),
        ("CHAR", "string"),
        ("NCHAR", "string"),
        ("INT", "integer"),
        ("INTEGER", "integer"),
        ("BIGINT", "integer"),
        ("SMALLINT", "integer"),
        ("FLOAT", "double"),
        ("BOOLEAN", "boolean"),
        ("BOOL", "boolean"),
        ("NUMBER", "number"),
    }
)

# Known encoding for specifis source hints to bypass detection
KNOWN_ENCODINGS = {
    "teradata_latin": "windows-1252",
    "mssql_varchar": "windows-1252",
    "sybase_iso1": "iso-8859-1",
}

# Typographic character subsitutions for cleaning string data
TYPOGRAPHIC_SUBSTITUITIONS = [
    # Various single quotes to straight single quote
    (
        "[\u2018\u2019\u201a\u201b]",
        "'",
    ),
    # Various double quotes to straight double quote
    (
        "[\u201c\u201d\u201e\u201f]",
        '"',
    ),
    # Ellipsis to three dots
    ("\u2026", "..."),
    # Non-breaking space to regular space
    ("\u00a0", " "),
    # Various zero-width and line separator characters to space
    (
        "[\u200b-\u200f\u2028\u2029]",
        " ",
    ),
    # Remove zero-width no-break space (BOM)
    ("\ufeff", ""),
]
