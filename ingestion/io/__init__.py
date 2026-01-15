"""Data I/O operations for streaming and batch processing.

Provides memory-efficient streaming from various file formats and data sources
using DuckDB and PyArrow for optimal performance.
"""

from .duckdb import rename_csv_columns, stream_csv_to_arrow
from .multirecord import (
    filter_expected_records,
    get_record_type_counts,
    parse_trailer_counts,
    parse_trailer_records,
    partition_by_record_type,
    validate_record_counts,
)

__all__ = [
    "filter_expected_records",
    "get_record_type_counts",
    "parse_trailer_counts",
    "parse_trailer_records",
    "partition_by_record_type",
    "rename_csv_columns",
    "stream_csv_to_arrow",
    "validate_record_counts",
]
