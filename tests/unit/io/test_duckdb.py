"""Tests for DuckDB-based I/O operations."""

import pyarrow as pa

from ingestion.io.duckdb import rename_csv_columns, stream_csv_to_arrow


def test_stream_csv_to_arrow(tmp_path):
    """Test streaming CSV data via DuckDB."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("1|Alice|25\n2|Bob|30\n", encoding="utf-8")

    batches = list(stream_csv_to_arrow(csv_file, batch_size=1, header=False))

    # DuckDB might return one or two batches depending on its internal buffer
    assert len(batches) >= 1
    total_rows = sum(b.num_rows for b in batches)
    assert total_rows == 2

    # Check column names (should be generic)
    assert batches[0].schema.names[0] == "column_1"
    assert batches[0].schema.names[1] == "column_2"
    assert batches[0].schema.names[2] == "column_3"


def test_rename_csv_columns(sample_record_batch):
    """Test renaming columns in a RecordBatch."""
    new_names = ["ID", "Val", "City"]
    renamed = rename_csv_columns(sample_record_batch, new_names)

    assert renamed.schema.names == new_names
    assert renamed.num_rows == 3


def test_rename_csv_columns_partial(sample_record_batch):
    """Test renaming with fewer names than columns."""
    new_names = ["ID", "Val"]
    renamed = rename_csv_columns(sample_record_batch, new_names)

    # Should select only the first 2 columns
    assert renamed.schema.names == new_names
    assert renamed.num_columns == 2


def test_rename_csv_columns_table(sample_arrow_table):
    """Test renaming columns in a Table."""
    new_names = ["ID", "Val", "City"]
    renamed = rename_csv_columns(sample_arrow_table, new_names)

    assert renamed.schema.names == new_names
    assert isinstance(renamed, pa.Table)
