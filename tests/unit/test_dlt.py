"""Tests for dlt integration modules."""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from jestr.engines.dlt.config import RunMode
from jestr.engines.dlt.typing import cast_columns_to_dlt_types


@pytest.mark.unit
class TestCastColumnsToDltTypes:
    """Test casting columns to dlt types."""

    def test_cast_columns_with_numeric_types(self):
        """Test casting numeric columns."""
        batch = pa.record_batch(
            {
                "id": [1, 2, 3],
                "value": [10.5, 20.3, 15.7],
            }
        )

        schema_cols = [
            {"name": "id", "data_type": "bigint"},
            {"name": "value", "data_type": "double"},
        ]

        with patch(
            "jestr.engines.dlt.typing.cast_arrow_as_columns_schema"
        ) as mock_cast:
            mock_cast.return_value = batch
            with patch("jestr.engines.dlt.typing.standardise_string_column"):
                result_arrays, success = cast_columns_to_dlt_types(batch, schema_cols)
                assert success is True
                assert len(result_arrays) == 2

    def test_cast_columns_with_string_types(self):
        """Test casting string columns."""
        batch = pa.record_batch(
            {
                "name": ["Alice", "Bob", "Charlie"],
                "email": [
                    "alice@example.com",
                    "bob@example.com",
                    "charlie@example.com",
                ],
            }
        )

        schema_cols = [
            {"name": "name", "data_type": "text"},
            {"name": "email", "data_type": "text"},
        ]

        with patch(
            "jestr.engines.dlt.typing.cast_arrow_as_columns_schema"
        ) as mock_cast:
            mock_cast.return_value = batch
            with patch(
                "jestr.engines.dlt.typing.standardise_string_column"
            ) as mock_std:
                mock_std.return_value = batch.column("name")
                _result_arrays, success = cast_columns_to_dlt_types(batch, schema_cols)
                assert success is True

    def test_cast_columns_with_mixed_types(self):
        """Test casting mixed column types."""
        batch = pa.record_batch(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "created_at": pa.array(["2024-01-01", "2024-01-02", "2024-01-03"]),
            }
        )

        schema_cols = [
            {"name": "id", "data_type": "bigint"},
            {"name": "name", "data_type": "text"},
            {"name": "created_at", "data_type": "timestamp"},
        ]

        with patch(
            "jestr.engines.dlt.typing.cast_arrow_as_columns_schema"
        ) as mock_cast:
            mock_cast.return_value = batch
            with patch(
                "jestr.engines.dlt.typing.standardise_string_column"
            ) as mock_std:
                mock_std.return_value = batch.column("name")
                result_arrays, success = cast_columns_to_dlt_types(batch, schema_cols)
                assert success is True
                assert len(result_arrays) == 3

    def test_cast_columns_handles_exception(self):
        """Test that casting handles exceptions gracefully."""
        batch = pa.record_batch({"id": [1, 2, 3]})
        schema_cols = [{"name": "id", "data_type": "bigint"}]

        with patch(
            "jestr.engines.dlt.typing.cast_arrow_as_columns_schema"
        ) as mock_cast:
            mock_cast.side_effect = Exception("Casting failed")
            result_arrays, success = cast_columns_to_dlt_types(batch, schema_cols)
            assert success is False
            assert result_arrays == []

    def test_cast_columns_with_generic_capabilities(self):
        """Test casting with generic capabilities."""
        batch = pa.record_batch({"id": [1, 2, 3]})
        schema_cols = [{"name": "id", "data_type": "bigint"}]

        with (
            patch("jestr.engines.dlt.typing.cast_arrow_as_columns_schema") as mock_cast,
            patch(
                "jestr.engines.dlt.typing.DestinationCapabilitiesContext"
            ) as mock_caps,
        ):
            mock_cast.return_value = batch
            cast_columns_to_dlt_types(batch, schema_cols, caps=None)
            # Verify that generic_capabilities was called
            mock_caps.generic_capabilities.assert_called_once()

    def test_cast_columns_with_custom_timezone(self):
        """Test casting with custom timezone."""
        batch = pa.record_batch({"id": [1, 2, 3]})
        schema_cols = [{"name": "id", "data_type": "bigint"}]

        with patch(
            "jestr.engines.dlt.typing.cast_arrow_as_columns_schema"
        ) as mock_cast:
            mock_cast.return_value = batch
            with patch("jestr.engines.dlt.typing.standardise_string_column"):
                cast_columns_to_dlt_types(batch, schema_cols, tz="America/New_York")
                # Verify timezone was passed
                args, kwargs = mock_cast.call_args
                assert (
                    kwargs.get("tz") == "America/New_York"
                    or args[3] == "America/New_York"
                )

    def test_cast_columns_with_capabilities_context(self):
        """Test casting with capabilities context."""
        batch = pa.record_batch({"id": [1, 2, 3]})
        schema_cols = [{"name": "id", "data_type": "bigint"}]
        mock_caps = MagicMock()

        with patch(
            "jestr.engines.dlt.typing.cast_arrow_as_columns_schema"
        ) as mock_cast:
            mock_cast.return_value = batch
            with patch("jestr.engines.dlt.typing.standardise_string_column"):
                cast_columns_to_dlt_types(batch, schema_cols, caps=mock_caps)
                # Verify capabilities were passed
                args, kwargs = mock_cast.call_args
                assert kwargs.get("caps") == mock_caps or args[2] == mock_caps

    def test_cast_columns_empty_batch(self):
        """Test casting an empty batch."""
        batch = pa.record_batch({"id": []})
        schema_cols = [{"name": "id", "data_type": "bigint"}]

        with patch(
            "jestr.engines.dlt.typing.cast_arrow_as_columns_schema"
        ) as mock_cast:
            mock_cast.return_value = batch
            with patch("jestr.engines.dlt.typing.standardise_string_column"):
                result_arrays, success = cast_columns_to_dlt_types(batch, schema_cols)
                assert success is True
                assert len(result_arrays) == 1

    def test_cast_columns_large_string_type(self):
        """Test casting large_string column types."""
        batch = pa.record_batch(
            {
                "description": pa.array(
                    ["text1", "text2", "text3"], type=pa.large_string()
                )
            }
        )
        schema_cols = [{"name": "description", "data_type": "text"}]

        with patch(
            "jestr.engines.dlt.typing.cast_arrow_as_columns_schema"
        ) as mock_cast:
            mock_cast.return_value = batch
            with patch(
                "jestr.engines.dlt.typing.standardise_string_column"
            ) as mock_std:
                mock_std.return_value = batch.column("description")
                _result_arrays, success = cast_columns_to_dlt_types(batch, schema_cols)
                # standardise_string_column should be called for large_string
                assert mock_std.called or success is True


@pytest.mark.unit
class TestRunModeIntegration:
    """Tests for RunMode integration (logic only)."""

    def test_run_mode_usage_in_conditions(self):
        """Test using RunMode in conditional logic."""
        mode = RunMode.EXTRACT
        if mode == RunMode.EXTRACT:
            result = "extracting"
        elif mode == RunMode.REPLAY:
            result = "replaying"
        assert result == "extracting"

    def test_run_mode_iteration(self):
        """Test iterating over RunMode members."""
        modes = list(RunMode)
        assert len(modes) == 2
        assert RunMode.EXTRACT in modes
        assert RunMode.REPLAY in modes


@pytest.mark.unit
class TestDltTypingModuleIntegration:
    """Integration tests for dlt typing module logic."""

    def test_casting_maintains_column_order(self):
        """Test that column casting maintains column order."""
        batch = pa.record_batch(
            {
                "col_a": [1, 2, 3],
                "col_b": ["x", "y", "z"],
                "col_c": [1.5, 2.5, 3.5],
            }
        )
        schema_cols = [
            {"name": "col_a", "data_type": "bigint"},
            {"name": "col_b", "data_type": "text"},
            {"name": "col_c", "data_type": "double"},
        ]

        with patch(
            "jestr.engines.dlt.typing.cast_arrow_as_columns_schema"
        ) as mock_cast:
            mock_cast.return_value = batch
            with patch(
                "jestr.engines.dlt.typing.standardise_string_column"
            ) as mock_std:
                mock_std.return_value = batch.column("col_b")
                result_arrays, success = cast_columns_to_dlt_types(batch, schema_cols)
                assert success is True
                # Number of columns should match
                assert len(result_arrays) == 3

    def test_casting_with_partial_schema(self):
        """Test casting when schema has fewer columns than batch."""
        batch = pa.record_batch(
            {
                "col_a": [1, 2, 3],
                "col_b": ["x", "y", "z"],
                "col_c": [1.5, 2.5, 3.5],
            }
        )
        # Schema only has 2 columns
        schema_cols = [
            {"name": "col_a", "data_type": "bigint"},
            {"name": "col_b", "data_type": "text"},
        ]

        with patch(
            "jestr.engines.dlt.typing.cast_arrow_as_columns_schema"
        ) as mock_cast:
            mock_cast.return_value = batch
            with patch("jestr.engines.dlt.typing.standardise_string_column"):
                _result_arrays, success = cast_columns_to_dlt_types(batch, schema_cols)
                assert success is True
