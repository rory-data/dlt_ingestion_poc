"""Unit tests for the generic helper functions."""

import polars as pl
import pyarrow as pa
import pytest
from polars.testing import assert_frame_equal

# Assuming the helpers module is importable, adjust path if necessary
from helpers.generic import clean_data, clean_string_columns, coerce_arrow_string_type

# --- Test Data Fixtures ---


@pytest.fixture
def sample_df_mixed_types() -> pl.DataFrame:
    """DataFrame with mixed data types including strings needing cleaning."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": [
                "  Alice\n ",
                "Bob\t",
                " Charlie ",
                "David\u000c",  # Form feed control char
                "Eve\u00e9",  # e with acute accent (precomposed)
            ],
            "city": ["New York ", " London", "Paris\r", None, "Berlin\u0065\u0301"],  # e + combining acute
            "value": [10.5, 20.0, 15.5, 30.0, 25.5],
            "active": [True, False, True, False, True],
        }
    )


@pytest.fixture
def expected_df_cleaned() -> pl.DataFrame:
    """Expected DataFrame after cleaning sample_df_mixed_types."""
    return pl.DataFrame(
        {
            "id": pl.Series([1, 2, 3, 4, 5], dtype=pl.Int64),
            "name": pl.Series(["Alice", "Bob", "Charlie", "David", "Eve\u00e9"], dtype=pl.Utf8),
            "city": pl.Series(["New York", "London", "Paris", None, "Berlin\u00e9"], dtype=pl.Utf8),  # Normalised
            "value": pl.Series([10.5, 20.0, 15.5, 30.0, 25.5], dtype=pl.Float64),
            "active": pl.Series([True, False, True, False, True], dtype=pl.Boolean),
        }
    )


@pytest.fixture
def sample_arrow_table(sample_df_mixed_types: pl.DataFrame) -> pa.Table:
    """PyArrow Table version of sample_df_mixed_types."""
    # Explicitly create schema to test coercion later if needed
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.large_string()),  # Use large_string to test coercion
            pa.field("city", pa.string()),
            pa.field("value", pa.float64()),
            pa.field("active", pa.bool_()),
        ]
    )
    return pa.Table.from_pandas(sample_df_mixed_types.to_pandas(), schema=schema)


@pytest.fixture
def expected_arrow_table_cleaned(expected_df_cleaned: pl.DataFrame) -> pa.Table:
    """Expected PyArrow Table after cleaning sample_arrow_table."""
    # Expected schema should have pa.string() for string types
    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),  # Should be coerced to string
            pa.field("city", pa.string()),
            pa.field("value", pa.float64()),
            pa.field("active", pa.bool_()),
        ]
    )
    return pa.Table.from_pandas(expected_df_cleaned.to_pandas(), schema=expected_schema)


@pytest.fixture
def df_no_strings() -> pl.DataFrame:
    """DataFrame with no string columns."""
    return pl.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})


@pytest.fixture
def arrow_table_no_strings(df_no_strings: pl.DataFrame) -> pa.Table:
    """PyArrow Table with no string columns."""
    return pa.Table.from_pandas(df_no_strings.to_pandas())


@pytest.fixture
def df_with_string_view() -> pl.DataFrame:
    """DataFrame that might produce string_view in Arrow."""
    # Polars doesn't directly map to string_view, Arrow decides this.
    # We create a DF that Arrow *might* convert to string_view,
    # then test if our function coerces it back to pa.string().
    df = pl.DataFrame({"text": ["long string data"] * 10})  # Example
    arrow_table = pa.table(df)
    # Manually change schema to simulate string_view for testing coercion
    schema_with_view = pa.schema([pa.field("text", pa.string_view())])
    return pa.table(arrow_table.to_pydict(), schema=schema_with_view)


# --- Tests for clean_string_columns ---


def test_clean_string_columns_positive(sample_df_mixed_types, expected_df_cleaned):
    """Test cleaning works correctly on mixed-type DataFrame."""
    df_cleaned = clean_string_columns(sample_df_mixed_types)
    # Check only string columns from expected_df_cleaned
    expected_strings = expected_df_cleaned.select(pl.col("name", "city"))
    cleaned_strings = df_cleaned.select(pl.col("name", "city"))
    assert_frame_equal(cleaned_strings, expected_strings)
    # Ensure non-string columns are untouched (by comparing full frames after selecting)
    assert_frame_equal(df_cleaned, expected_df_cleaned)


def test_clean_string_columns_no_strings(df_no_strings):
    """Test cleaning on a DataFrame with no string columns."""
    df_cleaned = clean_string_columns(df_no_strings)
    assert_frame_equal(df_cleaned, df_no_strings)  # Should be unchanged


def test_clean_string_columns_empty():
    """Test cleaning on an empty DataFrame."""
    df_empty = pl.DataFrame({"a": pl.Series([], dtype=pl.Utf8), "b": pl.Series([], dtype=pl.Int64)})
    df_cleaned = clean_string_columns(df_empty)
    assert_frame_equal(df_cleaned, df_empty)


# --- Tests for coerce_arrow_string_type ---


def test_coerce_arrow_string_type_large_string(sample_arrow_table):
    """Test coercion from large_string to string."""
    df = pl.from_arrow(sample_arrow_table)  # type: ignore[assignment]
    assert isinstance(df, pl.DataFrame)  # Ensure it's a DataFrame
    coerced_schema = coerce_arrow_string_type(df)

    assert pa.types.is_string(coerced_schema.field("name").type)  # Coerced to string
    assert pa.types.is_string(coerced_schema.field("city").type)  # Already string
    assert pa.types.is_int64(coerced_schema.field("id").type)  # Non-string untouched


def test_coerce_arrow_string_type_string_view(df_with_string_view):
    """Test coercion from string_view to string."""
    df = pl.from_arrow(df_with_string_view)  # type: ignore[assignment]
    assert isinstance(df, pl.DataFrame)  # Ensure it's a DataFrame
    coerced_schema = coerce_arrow_string_type(df)
    assert pa.types.is_string(coerced_schema.field("text").type)


def test_coerce_arrow_string_type_no_change_needed(expected_arrow_table_cleaned):
    """Test schema coercion when no change is needed (already pa.string)."""
    df = pl.from_arrow(expected_arrow_table_cleaned)  # type: ignore[assignment]
    assert isinstance(df, pl.DataFrame)  # Ensure it's a DataFrame
    original_schema = expected_arrow_table_cleaned.schema
    coerced_schema = coerce_arrow_string_type(df)
    assert coerced_schema == original_schema  # Schemas should be identical


def test_coerce_arrow_string_type_no_strings(arrow_table_no_strings):
    """Test schema coercion on a table with no string types."""
    df = pl.from_arrow(arrow_table_no_strings)  # type: ignore[assignment]
    assert isinstance(df, pl.DataFrame)  # Ensure it's a DataFrame
    original_schema = arrow_table_no_strings.schema
    coerced_schema = coerce_arrow_string_type(df)
    assert coerced_schema == original_schema  # Schemas should be identical


def test_coerce_arrow_string_type_empty():
    """Test schema coercion on an empty table."""
    empty_df = pl.DataFrame({"a": pl.Series([], dtype=pl.Utf8), "b": pl.Series([], dtype=pl.Int64)})
    # No need for original_schema variable
    coerced_schema = coerce_arrow_string_type(empty_df)
    # Even if empty, the schema types should be correct (pa.string for Utf8)
    assert pa.types.is_string(coerced_schema.field("a").type)
    assert pa.types.is_int64(coerced_schema.field("b").type)
    # Check if it correctly identified the string type vs original Arrow inference
    expected_schema = pa.schema([pa.field("a", pa.string()), pa.field("b", pa.int64())])
    assert coerced_schema == expected_schema


# --- Tests for clean_data ---


def test_clean_data_positive(sample_arrow_table, expected_arrow_table_cleaned):
    """Test the end-to-end clean_data function."""
    cleaned_table = clean_data(sample_arrow_table)

    # Convert both to Polars for robust comparison (handles potential metadata diffs)
    df_cleaned_result = pl.from_arrow(cleaned_table)  # type: ignore[assignment]
    df_expected = pl.from_arrow(expected_arrow_table_cleaned)  # type: ignore[assignment]

    # Ensure they are DataFrames before comparison
    assert isinstance(df_cleaned_result, pl.DataFrame)
    assert isinstance(df_expected, pl.DataFrame)

    assert_frame_equal(df_cleaned_result, df_expected)
    # Also check the schema explicitly for string types
    assert pa.types.is_string(cleaned_table.schema.field("name").type)
    assert pa.types.is_string(cleaned_table.schema.field("city").type)


def test_clean_data_no_strings(arrow_table_no_strings):
    """Test clean_data with a table containing no string columns."""
    cleaned_table = clean_data(arrow_table_no_strings)
    # Convert to Polars for robust comparison
    df_cleaned_result = pl.from_arrow(cleaned_table)  # type: ignore[assignment]
    df_original = pl.from_arrow(arrow_table_no_strings)  # type: ignore[assignment]
    assert isinstance(df_cleaned_result, pl.DataFrame)
    assert isinstance(df_original, pl.DataFrame)
    assert_frame_equal(df_cleaned_result, df_original)  # Compare content and schema


def test_clean_data_empty():
    """Test clean_data with an empty table."""
    empty_schema = pa.schema([pa.field("col_str", pa.string()), pa.field("col_int", pa.int64())])
    empty_arrow_table = pa.Table.from_pylist([], schema=empty_schema)
    cleaned_table = clean_data(empty_arrow_table)
    assert cleaned_table.num_rows == 0
    assert cleaned_table.schema == empty_schema  # Schema should be preserved


def test_clean_data_invalid_input_type(sample_df_mixed_types):
    """Test clean_data raises TypeError for non-Arrow Table input."""
    with pytest.raises(TypeError, match="Input 'arrow_data' must be a pyarrow.Table."):
        clean_data(sample_df_mixed_types)  # Pass Polars DF instead of Arrow Table


def test_clean_data_input_none():
    """Test clean_data raises TypeError for None input."""
    with pytest.raises(TypeError, match="Input 'arrow_data' must be a pyarrow.Table."):
        clean_data(None)


# Optional: Test PolarsError propagation (requires mocking or specific error-inducing data)
# def test_clean_data_polars_error():
#     # This is harder to reliably trigger without mocking internals
#     # For now, assume Polars errors propagate as tested indirectly
#     pass
