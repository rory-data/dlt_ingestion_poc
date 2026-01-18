"""Base protocols and types for data I/O operations.

This module defines the interfaces for reading and writing Arrow data.
All readers and writers should implement these protocols to enable composition
and pluggability in data pipelines.

Design Principles:
- Arrow is the data spine: all readers output Arrow, all writers accept Arrow
- Immutable results: ReadResult and WriteResult cannot be modified after creation
- Protocol-based: no mandatory inheritance, any class with the right signature works
- Pure functions where possible: I/O orchestration separated from logic
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pyarrow as pa


@dataclass(frozen=True)
class ReadResult:
    """Immutable result of a read operation.

    Represents data successfully read from an external source, including:
    - The data in Arrow format (columnar, efficient)
    - Number of rows read
    - Source location/identifier
    - Optional schema information

    Attributes:
        data: PyArrow Table containing the read data
        rows_read: Total rows in the returned data
        source: Source identifier (file path, table name, etc.)
        schema: Arrow schema of the data
    """

    data: pa.Table
    rows_read: int
    source: str
    schema: pa.Schema | None = None

    def __post_init__(self) -> None:
        """Validate that rows_read matches data."""
        if self.rows_read != self.data.num_rows:
            raise ValueError(
                f"rows_read ({self.rows_read}) does not match "
                f"data.num_rows ({self.data.num_rows})"
            )
        if self.schema is None:
            object.__setattr__(self, "schema", self.data.schema)

    @property
    def is_empty(self) -> bool:
        """Check if no rows were read."""
        return self.rows_read == 0


@dataclass(frozen=True)
class WriteResult:
    """Immutable result of a write operation.

    Represents successful write to an external destination, including:
    - Number of rows written
    - Destination identifier
    - Status information

    Attributes:
        rows_written: Total rows written
        destination: Destination identifier (file path, table name, etc.)
        metadata: Optional metadata (file size, duration, etc.)
    """

    rows_written: int
    destination: str
    metadata: dict[str, str | int] | None = None

    @property
    def is_empty(self) -> bool:
        """Check if nothing was written."""
        return self.rows_written == 0


class Reader(Protocol):
    """Protocol for reading data into Arrow format.

    Readers are responsible for fetching data from external sources and
    converting it to PyArrow Tables. Implementations should:

    1. Support streaming or batch reads efficiently
    2. Return data in Arrow format (zero-copy, columnar)
    3. Provide clear source identification
    4. Handle errors gracefully with informative messages

    Example:
        >>> reader = CSVReader("/path/to/file.csv")
        >>> result = reader.read()
        >>> print(f"Read {result.rows_read} rows from {result.source}")
        >>> df = result.data.to_pandas()  # Convert as needed
    """

    def read(self) -> ReadResult:
        """Read data and return as Arrow Table.

        Returns:
            ReadResult containing Arrow Table and metadata

        Raises:
            FileNotFoundError: If source does not exist
            ValueError: If data format is invalid
            Exception: For other I/O errors
        """
        ...


class StreamingReader(Protocol):
    """Protocol for streaming data into Arrow batches.

    For large data sources, streaming readers yield batches to reduce
    memory overhead. Use when data is larger than available RAM.

    Example:
        >>> reader = StreamingCSVReader("/path/to/large.csv", batch_size=10_000)
        >>> for result in reader.stream():
        ...     validate_and_process(result.data)
    """

    def stream(self, batch_size: int = 50_000) -> list[ReadResult]:
        """Stream data in batches.

        Args:
            batch_size: Rows per batch

        Yields:
            ReadResult for each batch

        Raises:
            Same as Reader.read()
        """
        ...


class Writer(Protocol):
    """Protocol for writing Arrow data to external destinations.

    Writers are responsible for taking PyArrow Tables and persisting them
    to external storage. Implementations should:

    1. Handle schema information correctly
    2. Support various data types (strings, numbers, dates, etc.)
    3. Provide clear error messages on failure
    4. Be idempotent if possible (safe to re-run)

    Example:
        >>> writer = ParquetWriter("/path/to/output/")
        >>> result = writer.write(arrow_table)
        >>> print(f"Wrote {result.rows_written} rows to {result.destination}")
    """

    def write(self, data: pa.Table) -> WriteResult:
        """Write Arrow Table to destination.

        Args:
            data: PyArrow Table to write

        Returns:
            WriteResult with write metadata

        Raises:
            ValueError: If data schema is incompatible
            IOError: If write fails
            Exception: For other errors
        """
        ...


# ============================================================================
# Adapter Pattern: Format-Specific Converters
# ============================================================================


class FormatAdapter(Protocol):
    """Protocol for format-specific conversions.

    Adapters bridge between external formats (CSV, JSON, etc.) and Arrow.
    Use when format-specific logic is needed beyond simple I/O.

    Example:
        >>> adapter = CSVAdapter(sep="|", encoding="latin1")
        >>> arrow_table = adapter.to_arrow(csv_file)
    """

    def to_arrow(self, source: str | Path) -> pa.Table:
        """Convert external format to Arrow Table.

        Args:
            source: Source file or identifier

        Returns:
            PyArrow Table

        Raises:
            Format-specific exceptions
        """
        ...

    def from_arrow(self, data: pa.Table, destination: str | Path) -> None:
        """Convert Arrow Table to external format.

        Args:
            data: PyArrow Table to convert
            destination: Target file or identifier

        Raises:
            Format-specific exceptions
        """
        ...


# ============================================================================
# Composition: Reader + Adapter Pattern
# ============================================================================


class AdaptiveReader(Protocol):
    """Protocol for readers that auto-detect format and adapt.

    These readers examine the source and choose the appropriate adapter
    automatically, useful for multi-format pipelines.

    Example:
        >>> reader = AutoReader()
        >>> result = reader.read("data.csv")      # Auto-detects CSV
        >>> result = reader.read("data.parquet")  # Auto-detects Parquet
    """

    def read(self, source: str | Path) -> ReadResult:
        """Auto-detect format and read.

        Args:
            source: File path or identifier

        Returns:
            ReadResult in Arrow format

        Raises:
            ValueError: If format cannot be detected or is unsupported
        """
        ...
