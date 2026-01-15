"""Data ingestion pipeline module.

Provides utilities for streaming data I/O, schema mapping, data validation,
and transformation in ETL/ELT pipelines.
"""

from . import core, engines, io, schema, utils

__all__ = [
    "core",
    "engines",
    "io",
    "schema",
    "utils",
]
