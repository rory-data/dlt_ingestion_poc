"""DuckDB streaming I/O operations.

This module provides utilities for streaming data via DuckDB's Arrow integration
for efficient, memory-conscious data loading.
"""

from collections.abc import Iterator
from pathlib import Path

import duckdb
import pyarrow as pa


def stream_csv_to_arrow(
    file_path: str | Path,
    batch_size: int = 50_000,
    header: bool = False,
    column_prefix: str = "column",
) -> Iterator[pa.RecordBatch]:
    """Stream CSV data from file via DuckDB Arrow reader.

    Uses DuckDB's streaming Arrow integration for memory-efficient processing.
    Automatically renames columns to generic names to avoid per-batch renaming.

    Args:
        file_path: Path to the CSV file.
        batch_size: Number of rows per batch (internal DuckDB buffer).
        header: Whether the CSV has a header row.
        column_prefix: Prefix for generic column names (e.g., "column_1", "column_2").

    Yields:
        PyArrow RecordBatch objects.
    """
    file_path_str = str(file_path)

    # Use a single connection for the duration of the stream
    with duckdb.connect(":memory:") as con:
        # Read CSV with DuckDB. all_varchar=True ensures no automatic type inference
        # during extraction, which is handled later by dlt transformers.
        rel = con.read_csv(
            file_path_str,
            header=header,
            sep="|",
            parallel=False,  # Preserve row order for positional record parsing
            null_padding=True,
            all_varchar=True,
        )

        # Rename columns to generic indices to simplify downstream mapping
        # This is more efficient than renaming individually per batch in Python.
        col_renames = [
            f'"{old}" AS "{column_prefix}_{i + 1}"' for i, old in enumerate(rel.columns)
        ]
        if col_renames:
            rel = rel.project(", ".join(col_renames))

        # Stream via Arrow using fetch_arrow_reader
        with rel.fetch_arrow_reader(batch_size) as reader:
            yield from reader


def rename_csv_columns(
    batch: pa.RecordBatch | pa.Table,
    column_names: list[str],
) -> pa.RecordBatch | pa.Table:
    """Rename columns in a batch or table to provided names.

    Args:
        batch: PyArrow RecordBatch or Table.
        column_names: Target column names.

    Returns:
        The data structure with renamed columns.
    """
    available_cols = min(batch.num_columns, len(column_names))
    if available_cols == 0:
        return batch

    target_names = column_names[:available_cols]
    selected = batch.select(list(range(available_cols)))

    if isinstance(selected, pa.RecordBatch):
        return pa.RecordBatch.from_arrays(
            [selected.column(i) for i in range(available_cols)],
            names=target_names,
        )

    return selected.rename_columns(target_names)
