"""Type mapping utilities for converting ODCS types to Python types."""

from datetime import date, datetime, time
from typing import Any

PY_TYPE_MAP = {
    "string": str,
    "date": date,
    "timestamp": datetime,
    "time": time,
    "number": float,
    "integer": int,
    "object": dict,
    "array": list,
    "boolean": bool,
}


def get_python_type(schema_type: str) -> Any:
    """Map a string type from ODCS to its corresponding Python type."""
    return PY_TYPE_MAP.get(schema_type, str)
