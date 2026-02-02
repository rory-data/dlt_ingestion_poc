"""Comprehensive tests for PyArrow engine modules."""

from collections.abc import Iterator
from pathlib import Path
from typing import cast
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from jestr.engines.arrow import transforms
from jestr.engines.arrow.extractor import ArrowBatchExtractor
from jestr.engines.arrow.parquet import CompressionType, write_batches_to_parquet_impl


class TestArrowBatchExtractor:
    """Test suite for ArrowBatchExtractor."""

    @pytest.fixture
    def simple_schema(self) -> pa.Schema:
        """Create a simple test schema."""
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
            ]
        )

    @pytest.fixture
    def extractor(self, simple_schema: pa.Schema) -> ArrowBatchExtractor:
        """Create an ArrowBatchExtractor instance."""
        return ArrowBatchExtractor(schema=simple_schema)

    def test_init_default_memory_pool(self, simple_schema: pa.Schema) -> None:
        """Test initialisation with default memory pool."""
        extractor = ArrowBatchExtractor(schema=simple_schema)
        assert extractor.schema == simple_schema
        assert extractor.memory_pool is not None
        assert extractor._batch_count == 0

    def test_init_custom_memory_pool(self, simple_schema: pa.Schema) -> None:
        """Test initialisation with custom memory pool."""
        custom_pool = pa.default_memory_pool()
        extractor = ArrowBatchExtractor(schema=simple_schema, memory_pool=custom_pool)
        assert extractor.memory_pool == custom_pool

    def test_context_manager_enter(self, extractor: ArrowBatchExtractor) -> None:
        """Test context manager __enter__ returns self."""
        with extractor as ctx:
            assert ctx is extractor

    def test_context_manager_exit(self, extractor: ArrowBatchExtractor) -> None:
        """Test context manager __exit__ calls cleanup."""
        with patch.object(extractor, "cleanup") as mock_cleanup:
            with extractor:
                pass
            mock_cleanup.assert_called_once()

    def test_context_manager_exit_with_exception(
        self, extractor: ArrowBatchExtractor
    ) -> None:
        """Test context manager cleanup runs even with exception."""
        with patch.object(extractor, "cleanup") as mock_cleanup:
            try:
                with extractor:
                    raise ValueError("Test error")
            except ValueError:
                pass
            mock_cleanup.assert_called_once()

    def test_cleanup_logs_memory_info(self, extractor: ArrowBatchExtractor) -> None:
        """Test cleanup performs garbage collection and logs."""
        with patch("jestr.engines.arrow.extractor.logger") as mock_logger:
            extractor.cleanup()
            # Verify logging was called
            assert mock_logger.debug.called

    def test_rows_to_columns_basic(self, extractor: ArrowBatchExtractor) -> None:
        """Test converting rows to columns."""
        rows = [(1, "Alice", 10.5), (2, "Bob", 20.3)]
        columns = extractor._rows_to_columns(rows)

        assert len(columns) == 3
        assert columns[0].to_pylist() == [1, 2]
        assert columns[1].to_pylist() == ["Alice", "Bob"]
        assert columns[2].to_pylist() == [10.5, 20.3]

    def test_rows_to_columns_empty(self, extractor: ArrowBatchExtractor) -> None:
        """Test converting empty rows returns empty list."""
        rows: list[tuple] = []
        columns = extractor._rows_to_columns(rows)
        assert len(columns) == 0

    def test_rows_to_columns_single_row(self, extractor: ArrowBatchExtractor) -> None:
        """Test converting single row."""
        rows = [(1, "Alice", 10.5)]
        columns = extractor._rows_to_columns(rows)

        assert columns[0].to_pylist() == [1]
        assert columns[1].to_pylist() == ["Alice"]
        assert columns[2].to_pylist() == [10.5]

    def test_extract_cursor_batches_single_batch(
        self, extractor: ArrowBatchExtractor
    ) -> None:
        """Test extracting single batch from cursor."""
        rows = [(1, "Alice", 10.5), (2, "Bob", 20.3)]
        mock_cursor = Mock()
        mock_cursor.fetchmany.side_effect = [rows, []]

        batches = list(extractor.extract_cursor_batches(mock_cursor, batch_size=10))

        assert len(batches) == 1
        assert batches[0].num_rows == 2
        assert batches[0].column(0).to_pylist() == [1, 2]

    def test_extract_cursor_batches_multiple_batches(
        self, extractor: ArrowBatchExtractor
    ) -> None:
        """Test extracting multiple batches from cursor."""
        rows_batch1 = [(1, "Alice", 10.5), (2, "Bob", 20.3)]
        rows_batch2 = [(3, "Charlie", 15.7)]
        mock_cursor = Mock()
        mock_cursor.fetchmany.side_effect = [rows_batch1, rows_batch2, []]

        batches = list(extractor.extract_cursor_batches(mock_cursor, batch_size=2))

        assert len(batches) == 2
        assert batches[0].num_rows == 2
        assert batches[1].num_rows == 1

    def test_extract_cursor_batches_empty(self, extractor: ArrowBatchExtractor) -> None:
        """Test extracting from empty cursor."""
        mock_cursor = Mock()
        mock_cursor.fetchmany.return_value = []

        batches = list(extractor.extract_cursor_batches(mock_cursor))

        assert len(batches) == 0

    def test_extract_cursor_batches_fetch_error(
        self, extractor: ArrowBatchExtractor
    ) -> None:
        """Test handling fetch error from cursor."""
        mock_cursor = Mock()
        mock_cursor.fetchmany.side_effect = Exception("Database error")

        with pytest.raises(RuntimeError, match="Fetch failed"):
            list(extractor.extract_cursor_batches(mock_cursor))

    def test_extract_cursor_batches_arrow_conversion_error(
        self, extractor: ArrowBatchExtractor
    ) -> None:
        """Test handling Arrow conversion error."""
        rows = [("invalid_type_for_int64",)]
        mock_cursor = Mock()
        mock_cursor.fetchmany.side_effect = [rows, []]

        with pytest.raises(RuntimeError, match="Arrow conversion failed"):
            list(extractor.extract_cursor_batches(mock_cursor))

    def test_extract_cursor_batches_cleanup_interval(
        self, extractor: ArrowBatchExtractor
    ) -> None:
        """Test periodic cleanup at specified intervals."""
        rows = [(i, f"name_{i}", float(i)) for i in range(1, 31)]
        mock_cursor = Mock()
        mock_cursor.fetchmany.side_effect = [
            rows[0:10],
            rows[10:20],
            rows[20:30],
            [],
        ]

        with patch.object(extractor, "cleanup") as mock_cleanup:
            batches = list(
                extractor.extract_cursor_batches(
                    mock_cursor, batch_size=10, cleanup_interval=2
                )
            )
            assert len(batches) == 3
            # Cleanup called at batch 2
            assert mock_cleanup.call_count >= 1

    def test_extract_cursor_batches_no_cleanup(
        self, extractor: ArrowBatchExtractor
    ) -> None:
        """Test disabling periodic cleanup."""
        rows = [(i, f"name_{i}", float(i)) for i in range(1, 21)]
        mock_cursor = Mock()
        mock_cursor.fetchmany.side_effect = [rows, []]

        with patch.object(extractor, "cleanup") as mock_cleanup:
            list(extractor.extract_cursor_batches(mock_cursor, cleanup_interval=0))
            # Should only be called in context manager exit
            assert not mock_cleanup.called

    @pytest.mark.parametrize("batch_size", [1, 10, 100])
    def test_extract_cursor_batches_various_sizes(
        self, extractor: ArrowBatchExtractor, batch_size: int
    ) -> None:
        """Test extracting batches with various batch sizes."""
        rows = [(i, f"name_{i}", float(i)) for i in range(1, 21)]
        mock_cursor = Mock()
        mock_cursor.fetchmany.side_effect = [rows, []]

        batches = list(
            extractor.extract_cursor_batches(mock_cursor, batch_size=batch_size)
        )

        assert len(batches) > 0
        total_rows = sum(b.num_rows for b in batches)
        assert total_rows == len(rows)


class TestWriteParquetImpl:
    """Test suite for write_batches_to_parquet_impl."""

    @pytest.fixture
    def sample_batches(self) -> Iterator[pa.RecordBatch]:
        """Create sample RecordBatches for testing."""
        batch1 = pa.record_batch(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10.5, 20.3, 15.7],
            }
        )
        batch2 = pa.record_batch(
            {
                "id": [4, 5],
                "name": ["Diana", "Eve"],
                "value": [25.1, 30.8],
            }
        )
        return iter([batch1, batch2])

    @pytest.fixture
    def temp_parquet_file(self, tmp_path: Path) -> Path:
        """Provide a temporary Parquet file path."""
        return tmp_path / "test.parquet"

    def test_write_empty_batches(self, temp_parquet_file: Path) -> None:
        """Test writing empty batch iterator."""
        result = write_batches_to_parquet_impl(
            iter([]), temp_parquet_file, "write_batch"
        )

        assert result["row_count"] == 0
        assert result["file_size"] == 0
        assert "test.parquet" in result["output_path"]

    def test_write_single_batch(
        self,
        sample_batches: Iterator[pa.RecordBatch],
        temp_parquet_file: Path,
    ) -> None:
        """Test writing single batch to Parquet."""
        batch = pa.record_batch(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10.5, 20.3, 15.7],
            }
        )

        result = write_batches_to_parquet_impl(
            iter([batch]), temp_parquet_file, "write_batch"
        )

        assert result["row_count"] == 3
        assert result["file_size"] > 0
        assert result["file_size_mb"] > 0
        assert temp_parquet_file.exists()

    def test_write_multiple_batches(
        self,
        sample_batches: Iterator[pa.RecordBatch],
        temp_parquet_file: Path,
    ) -> None:
        """Test writing multiple batches to Parquet."""
        result = write_batches_to_parquet_impl(
            sample_batches, temp_parquet_file, "write_batch"
        )

        assert result["row_count"] == 5
        assert result["file_size"] > 0
        assert temp_parquet_file.exists()

    @pytest.mark.parametrize("compression", ["snappy", "gzip", "none"])
    def test_write_with_various_compressions(
        self, temp_parquet_file: Path, compression: str
    ) -> None:
        """Test writing with different compression types."""
        batch = pa.record_batch(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        result = write_batches_to_parquet_impl(
            iter([batch]),
            temp_parquet_file,
            "write_batch",
            compression=cast(CompressionType, compression),
        )

        assert result["compression"] == compression
        assert temp_parquet_file.exists()

    def test_write_creates_parent_directories(self, tmp_path: Path) -> None:
        """Test that parent directories are created if missing."""
        output_path = tmp_path / "nested" / "dir" / "test.parquet"
        batch = pa.record_batch({"id": [1, 2]})

        _ = write_batches_to_parquet_impl(iter([batch]), output_path, "write_batch")

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_write_with_write_statistics(self, temp_parquet_file: Path) -> None:
        """Test writing with statistics enabled."""
        batch = pa.record_batch({"id": [1, 2, 3]})

        result = write_batches_to_parquet_impl(
            iter([batch]), temp_parquet_file, "write_batch", write_statistics=True
        )

        assert result["row_count"] == 3

    def test_write_with_dictionary_encoding(self, temp_parquet_file: Path) -> None:
        """Test writing with dictionary encoding."""
        batch = pa.record_batch(
            {
                "id": [1, 2, 3],
                "category": ["A", "B", "A"],
            }
        )

        result = write_batches_to_parquet_impl(
            iter([batch]), temp_parquet_file, "write_batch", use_dictionary=True
        )

        assert result["row_count"] == 3


class TestArrowTransforms:
    """Test suite for Arrow transform utilities."""

    def test_rename_columns_basic(self) -> None:
        """Test renaming columns of a RecordBatch."""
        batch = pa.record_batch(
            {
                "col1": [1, 2, 3],
                "col2": ["a", "b", "c"],
                "col3": [1.5, 2.5, 3.5],
            }
        )

        result = transforms.rename_columns(batch, ["id", "name", "value"])

        assert result.column_names == ["id", "name", "value"]
        assert result.num_rows == 3

    def test_rename_columns_more_names_than_columns(self) -> None:
        """Test renaming with more names than columns."""
        batch = pa.record_batch(
            {
                "col1": [1, 2],
                "col2": ["a", "b"],
            }
        )

        result = transforms.rename_columns(batch, ["id", "name", "extra"])

        assert result.column_names == ["id", "name"]
        assert result.num_columns == 2

    def test_rename_columns_empty_batch(self) -> None:
        """Test renaming empty batch."""
        batch = pa.record_batch({})
        result = transforms.rename_columns(batch, ["id"])
        assert result.num_columns == 0

    def test_cast_single_column_int_to_string(self) -> None:
        """Test casting column from int to string."""
        col = pa.array([1, 2, 3])
        result = transforms.cast_single_column(col, pa.string())

        assert result.type == pa.string()
        assert result.to_pylist() == ["1", "2", "3"]

    def test_cast_single_column_string_to_int(self) -> None:
        """Test casting column from string to int."""
        col = pa.array(["1", "2", "3"])
        result = transforms.cast_single_column(col, pa.int64())

        assert result.type == pa.int64()
        assert result.to_pylist() == [1, 2, 3]

    def test_cast_single_column_float_to_int(self) -> None:
        """Test casting column from float to int truncates values."""
        # Note: Arrow's cast truncates, which may raise error for some values
        col = pa.array([1.0, 2.0, 3.0])
        result = transforms.cast_single_column(col, pa.int64())

        assert result.type == pa.int64()
        assert result.to_pylist() == [1, 2, 3]

    def test_cast_single_column_with_nulls(self) -> None:
        """Test casting column with null values."""
        col = pa.array([1, None, 3])
        result = transforms.cast_single_column(col, pa.string())

        assert result.type == pa.string()
        assert result.to_pylist() == ["1", None, "3"]

    def test_cast_chunked_array(self) -> None:
        """Test casting ChunkedArray combines chunks."""
        col = pa.chunked_array([[1, 2], [3, 4]])
        result = transforms.cast_single_column(col, pa.string())

        assert isinstance(result, pa.Array)
        assert result.to_pylist() == ["1", "2", "3", "4"]

    def test_create_casted_table(self) -> None:
        """Test creating table from casted arrays."""
        arrays = [
            pa.array([1, 2, 3]),
            pa.array(["a", "b", "c"]),
        ]
        names = ["id", "name"]

        result = transforms.create_casted_table(arrays, names)

        assert result.column_names == ["id", "name"]
        assert result.num_rows == 3

    def test_drop_first_column_table(self) -> None:
        """Test dropping first column from table."""
        table = pa.table(
            {
                "col1": [1, 2],
                "col2": ["a", "b"],
                "col3": [1.5, 2.5],
            }
        )

        result = transforms.drop_first_column(table)

        assert result.column_names == ["col2", "col3"]
        assert result.num_rows == 2

    def test_drop_first_column_batch(self) -> None:
        """Test dropping first column from RecordBatch."""
        batch = pa.record_batch(
            {
                "col1": [1, 2],
                "col2": ["a", "b"],
            }
        )

        result = transforms.drop_first_column(batch)

        assert result.column_names == ["col2"]

    def test_drop_first_column_single_column_raises(self) -> None:
        """Test dropping first column with only one column raises error."""
        table = pa.table({"col1": [1, 2]})

        with pytest.raises(ValueError, match="Cannot drop first column"):
            transforms.drop_first_column(table)

    def test_select_and_rename_columns(self) -> None:
        """Test selecting and renaming columns."""
        batch = pa.record_batch(
            {
                "col1": [1, 2],
                "col2": ["a", "b"],
                "col3": [1.5, 2.5],
            }
        )

        result = transforms.select_and_rename_columns(batch, ["id", "name"])

        assert result.column_names == ["id", "name"]
        assert result.num_columns == 2

    def test_standardise_string_column_trims_whitespace(self) -> None:
        """Test string standardisation trims whitespace."""
        col = pa.array(["  hello  ", "world  ", "  test"])
        result = transforms.standardise_string_column(col)

        assert result.to_pylist() == ["hello", "world", "test"]

    def test_standardise_string_column_non_string_unchanged(self) -> None:
        """Test non-string columns are unchanged."""
        col = pa.array([1, 2, 3])
        result = transforms.standardise_string_column(col)

        assert result is col

    def test_standardise_string_column_with_nulls(self) -> None:
        """Test string standardisation handles nulls."""
        col = pa.array(["  hello  ", None, "  world  "])
        result = transforms.standardise_string_column(col)

        assert result.to_pylist() == ["hello", None, "world"]

    def test_standardise_large_string_column(self) -> None:
        """Test standardisation works with large_string type."""
        col = pa.array(["  hello  ", "world  "], type=pa.large_string())
        result = transforms.standardise_string_column(col)

        assert result.type == pa.large_string()
        assert result.to_pylist() == ["hello", "world"]
