"""Tests for arrow table transformers."""

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ingestion.engines.pipeline.transformers import (
    cast_single_column,
    create_casted_table,
)


def test_cast_single_column():
    """Test casting a single column to a different type."""
    arr = pa.array(["1", "2", "3"])
    casted = cast_single_column(arr, pa.int32())
    assert casted.type == pa.int32()
    assert casted.to_pylist() == [1, 2, 3]


def test_create_casted_table():
    """Test creating a table from casted arrays."""
    arrays = [pa.array([1, 2]), pa.array(["a", "b"])]
    names = ["id", "name"]
    table = create_casted_table(arrays, names)

    assert table.num_rows == 2
    assert table.schema.names == names
    assert table.column("id").type == pa.int64()  # Default for [1,2]


def test_drop_first_column(sample_arrow_table):
    """Test dropping the first column."""
    from ingestion.engines.pipeline.transformers import drop_first_column

    dropped = drop_first_column(sample_arrow_table)
    assert dropped.num_columns == 2
    assert dropped.schema.names == ["age", "city"]


def test_drop_first_column_error():
    """Test error when dropping from single-column table."""
    from ingestion.engines.pipeline.transformers import drop_first_column

    table = pa.table({"a": [1]})
    with pytest.raises(ValueError, match="only one column"):
        drop_first_column(table)


def test_select_and_rename_columns(sample_arrow_table):
    """Test selecting and renaming."""
    from ingestion.engines.pipeline.transformers import select_and_rename_columns

    target_names = ["NAME", "AGE"]
    result = select_and_rename_columns(sample_arrow_table, target_names)
    assert result.num_columns == 2
    assert result.schema.names == target_names


def test_standardise_string_column():
    """Test trimming."""
    from ingestion.engines.pipeline.transformers import standardise_string_column

    arr = pa.array(["  hello  ", "  world  "])
    standardised = standardise_string_column(arr)

    assert standardised[0].as_py() == "hello"
    assert standardised[1].as_py() == "world"


def test_cast_columns_to_dlt_types(sample_arrow_table):
    """Test full table casting to dlt types."""
    from ingestion.engines.pipeline.transformers import cast_columns_to_dlt_types

    schema_cols = [
        {"name": "name", "data_type": "text"},
        {"name": "age", "data_type": "bigint"},
        {"name": "city", "data_type": "text"},
    ]

    arrays, success = cast_columns_to_dlt_types(sample_arrow_table, schema_cols)
    assert success is True
    assert len(arrays) == 3
    assert arrays[0].type == pa.string()
    assert arrays[1].type == pa.int64()


@given(
    st.lists(
        st.integers(min_value=-(2**31), max_value=2**31 - 1), min_size=1, max_size=100
    )
)
def test_cast_single_column_preserves_length(int_list):
    """Property test: casting must preserve array length."""
    arr = pa.array(int_list, type=pa.int32())
    casted = cast_single_column(arr, pa.int64())
    assert len(casted) == len(int_list)


@given(
    st.lists(
        st.integers(min_value=-(2**31), max_value=2**31 - 1), min_size=1, max_size=100
    )
)
def test_cast_single_column_maintains_non_null_count(int_list):
    """Property test: casting must not introduce or remove non-null values."""
    arr = pa.array(int_list, type=pa.int32())
    casted = cast_single_column(arr, pa.int64())
    assert casted.null_count == arr.null_count


@given(
    st.lists(st.text(), min_size=1, max_size=50),
    st.sampled_from([pa.int32(), pa.int64(), pa.float32(), pa.float64()]),
)
def test_create_casted_table_schema_matches(strings, target_type):
    """Property test: created table schema must match provided names and types."""
    arr1 = pa.array(strings)
    arr2 = cast_single_column(
        pa.array(list(range(len(strings))), type=pa.int32()), target_type
    )

    arrays = [arr1, arr2]
    names = ["col_text", "col_number"]

    table = create_casted_table(arrays, names)

    assert table.num_columns == 2
    assert table.schema.names == names
    assert table.num_rows == len(strings)
