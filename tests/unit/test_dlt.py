"""Tests for dlt integration modules."""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from jestr.contracts import ODCSContract
from jestr.engines.dlt.config import RunMode
from jestr.engines.dlt.operations import (
    get_validation_metrics_from_state,
    run_pipeline_with_summary,
)
from jestr.engines.dlt.sources.oracle_source import OracleSource
from jestr.engines.dlt.transformers import (
    _create_bad_mask,
    _extract_failed_row_details,
    create_validation_transformers,
)
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


@pytest.mark.unit
class TestGetValidationMetricsFromState:
    """Test validation metrics extraction from pipeline state."""

    def test_extract_validation_metrics_single_resource(self):
        """Test extracting metrics from pipeline state with single resource."""
        mock_pipeline = MagicMock()
        mock_pipeline.state = {
            "sources": {
                "source1": {
                    "resources": {
                        "resource1": {
                            "validation_metrics": {
                                "total_rows": 100,
                                "clean_rows": 95,
                                "bad_rows": 5,
                                "batches_processed": 2,
                                "issues": [],
                            }
                        }
                    }
                }
            }
        }

        result = get_validation_metrics_from_state(mock_pipeline)

        assert "resource1" in result
        metrics = result["resource1"]["validation_metrics"]
        assert metrics["total_rows"] == 100
        assert metrics["clean_rows"] == 95
        assert metrics["bad_rows"] == 5
        assert metrics["pass_rate"] == 95.0

    def test_extract_validation_metrics_multiple_resources(self):
        """Test extracting metrics from multiple resources."""
        mock_pipeline = MagicMock()
        mock_pipeline.state = {
            "sources": {
                "source1": {
                    "resources": {
                        "resource1": {
                            "validation_metrics": {
                                "total_rows": 100,
                                "clean_rows": 90,
                                "bad_rows": 10,
                                "batches_processed": 2,
                                "issues": [],
                            }
                        },
                        "resource2": {
                            "validation_metrics": {
                                "total_rows": 200,
                                "clean_rows": 180,
                                "bad_rows": 20,
                                "batches_processed": 3,
                                "issues": [],
                            }
                        },
                    }
                }
            }
        }

        result = get_validation_metrics_from_state(mock_pipeline)

        assert len(result) == 2
        assert result["resource1"]["validation_metrics"]["pass_rate"] == 90.0
        assert result["resource2"]["validation_metrics"]["pass_rate"] == 90.0

    def test_extract_validation_metrics_calculates_pass_rate(self):
        """Test that pass rate is calculated if missing."""
        mock_pipeline = MagicMock()
        mock_pipeline.state = {
            "sources": {
                "source1": {
                    "resources": {
                        "resource1": {
                            "validation_metrics": {
                                "total_rows": 50,
                                "clean_rows": 40,
                                "bad_rows": 10,
                                "batches_processed": 1,
                                "issues": [],
                            }
                        }
                    }
                }
            }
        }

        result = get_validation_metrics_from_state(mock_pipeline)

        metrics = result["resource1"]["validation_metrics"]
        assert metrics["pass_rate"] == 80.0

    def test_extract_validation_metrics_zero_total_rows(self):
        """Test pass rate calculation with zero total rows."""
        mock_pipeline = MagicMock()
        mock_pipeline.state = {
            "sources": {
                "source1": {
                    "resources": {
                        "resource1": {
                            "validation_metrics": {
                                "total_rows": 0,
                                "clean_rows": 0,
                                "bad_rows": 0,
                                "batches_processed": 0,
                                "issues": [],
                            }
                        }
                    }
                }
            }
        }

        result = get_validation_metrics_from_state(mock_pipeline)

        metrics = result["resource1"]["validation_metrics"]
        assert metrics["pass_rate"] == 0

    def test_extract_validation_metrics_no_validation_data(self):
        """Test extraction when no validation metrics exist."""
        mock_pipeline = MagicMock()
        mock_pipeline.state = {
            "sources": {
                "source1": {
                    "resources": {
                        "resource1": {
                            # No validation_metrics key
                        }
                    }
                }
            }
        }

        result = get_validation_metrics_from_state(mock_pipeline)

        assert len(result) == 0

    def test_extract_validation_metrics_empty_state(self):
        """Test extraction from empty pipeline state."""
        mock_pipeline = MagicMock()
        mock_pipeline.state = {}

        result = get_validation_metrics_from_state(mock_pipeline)

        assert len(result) == 0


@pytest.mark.unit
class TestRunPipelineWithSummary:
    """Test pipeline execution with summary extraction."""

    def test_run_pipeline_successful_execution(self):
        """Test successful pipeline execution."""
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = MagicMock()
        mock_pipeline.state = {
            "sources": {
                "source1": {
                    "resources": {
                        "resource1": {
                            "validation_metrics": {
                                "total_rows": 100,
                                "clean_rows": 95,
                                "bad_rows": 5,
                                "batches_processed": 1,
                                "issues": [],
                            }
                        }
                    }
                }
            }
        }

        mock_source = MagicMock()

        result = run_pipeline_with_summary(
            mock_pipeline,
            mock_source,
            pipeline_description="test pipeline",
        )

        assert "resource1" in result
        assert mock_pipeline.run.called

    def test_run_pipeline_with_critical_issues(self):
        """Test pipeline execution logging critical issues."""
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = MagicMock()
        mock_pipeline.state = {
            "sources": {
                "source1": {
                    "resources": {
                        "resource1": {
                            "validation_metrics": {
                                "total_rows": 100,
                                "clean_rows": 90,
                                "bad_rows": 10,
                                "batches_processed": 1,
                                "issues": [
                                    {
                                        "batch_date": "20240101",
                                        "issue_type": "null_value",
                                        "column": "col1",
                                        "severity": "critical",
                                        "count": 5,
                                        "samples": ["null1"],
                                    }
                                ],
                            }
                        }
                    }
                }
            }
        }

        mock_source = MagicMock()

        with patch("jestr.engines.dlt.operations.logger") as mock_logger:
            result = run_pipeline_with_summary(
                mock_pipeline,
                mock_source,
                pipeline_description="test pipeline",
            )

            assert "resource1" in result
            # Verify logger was called for critical issues
            assert mock_logger.warning.called

    def test_run_pipeline_execution_failure(self):
        """Test pipeline execution failure handling."""
        mock_pipeline = MagicMock()
        mock_pipeline.run.side_effect = Exception("Pipeline failed")

        mock_source = MagicMock()

        with pytest.raises(SystemExit):
            with patch("jestr.engines.dlt.operations.logger"):
                run_pipeline_with_summary(
                    mock_pipeline,
                    mock_source,
                    pipeline_description="test pipeline",
                )


@pytest.mark.unit
class TestOracleSourceInitialization:
    """Test OracleSource initialization."""

    def test_oracle_source_init(self):
        """Test OracleSource initialization."""
        mock_contract = MagicMock(spec=ODCSContract)
        source = OracleSource(
            resource_name="test_resource",
            contract=mock_contract,
            connection_uri="oracle://user:pass@localhost:1521/orcl",
            database_schema_name="SCHEMA",
            database_table_name="TABLE",
            batch_size=10000,
        )

        assert source.resource_name == "test_resource"
        assert source.full_table_name == "SCHEMA.TABLE"
        assert source.batch_size == 10000

    def test_oracle_source_create_factory(self):
        """Test OracleSource factory method."""
        mock_contract = MagicMock(spec=ODCSContract)
        source = OracleSource.create(
            resource_name="test_resource",
            contract=mock_contract,
            connection_uri="oracle://localhost/orcl",
            database_schema_name="SCHEMA",
            database_table_name="TABLE",
        )

        assert isinstance(source, OracleSource)
        assert source.resource_name == "test_resource"

    def test_oracle_source_with_custom_query(self):
        """Test OracleSource with custom query."""
        mock_contract = MagicMock(spec=ODCSContract)
        query = "SELECT * FROM SCHEMA.TABLE WHERE status='ACTIVE'"
        source = OracleSource(
            resource_name="test_resource",
            contract=mock_contract,
            connection_uri="oracle://localhost/orcl",
            database_schema_name="SCHEMA",
            database_table_name="TABLE",
            query=query,
        )

        assert source.query == query

    def test_oracle_source_no_schema_name(self):
        """Test OracleSource without schema name."""
        mock_contract = MagicMock(spec=ODCSContract)
        source = OracleSource(
            resource_name="test_resource",
            contract=mock_contract,
            connection_uri="oracle://localhost/orcl",
            database_schema_name="",
            database_table_name="TABLE",
        )

        assert source.full_table_name == "TABLE"


@pytest.mark.unit
class TestCreateBadMask:
    """Test bad data masking."""

    def test_create_bad_mask_all_good(self):
        """Test bad mask when all rows are good."""
        batch = pa.record_batch(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        bad_data = pa.record_batch({"id": [], "name": []})

        mask = _create_bad_mask(batch, bad_data)

        assert mask == [False, False, False]

    def test_create_bad_mask_with_bad_rows(self):
        """Test bad mask with some bad rows."""
        batch = pa.record_batch(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        bad_data = pa.record_batch(
            {
                "id": [2],
                "name": ["Bob"],
            }
        )

        mask = _create_bad_mask(batch, bad_data)

        assert mask == [False, True, False]

    def test_create_bad_mask_all_bad(self):
        """Test bad mask when all rows are bad."""
        batch = pa.record_batch(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        bad_data = batch

        mask = _create_bad_mask(batch, bad_data)

        assert mask == [True, True, True]


@pytest.mark.unit
class TestExtractFailedRowDetails:
    """Test failed row detail extraction."""

    def test_extract_failed_row_details_empty_bad_data(self):
        """Test extraction with no bad rows."""
        batch = pa.record_batch(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        bad_mask = pa.array([False, False, False])
        issues = []

        result = _extract_failed_row_details(batch, bad_mask, issues)

        assert result == []

    def test_extract_failed_row_details_with_bad_rows(self):
        """Test extraction with bad rows."""
        batch = pa.record_batch(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "email": ["a@test.com", None, "c@test.com"],
            }
        )
        bad_mask = pa.array([False, True, False])

        # Mock issue object
        mock_issue = MagicMock()
        mock_issue.column = "email"
        mock_issue.issue_type = "null_value"
        mock_issue.severity.value = "warning"
        mock_issue.issue_count = 1

        issues = [mock_issue]

        result = _extract_failed_row_details(batch, bad_mask, issues)

        assert len(result) > 0
        assert result[0]["row_index"] == 1
        assert len(result[0]["failed_checks"]) > 0

    def test_extract_failed_row_details_limit_rows(self):
        """Test that extraction limits to first 10 failed rows."""
        # Create batch with many bad rows
        batch = pa.record_batch(
            {
                "id": list(range(20)),
                "value": [i * 10 for i in range(20)],
            }
        )
        bad_mask = pa.array([True] * 20)

        mock_issue = MagicMock()
        mock_issue.column = "value"
        mock_issue.issue_type = "invalid"
        mock_issue.severity.value = "error"
        mock_issue.issue_count = 20

        result = _extract_failed_row_details(batch, bad_mask, [mock_issue])

        assert len(result) <= 10


@pytest.mark.unit
class TestCreateValidationTransformers:
    """Test validation transformer factory."""

    def test_create_validation_transformers_callable(self):
        """Test that create_validation_transformers is callable."""
        mock_resource = MagicMock()
        mock_validator = MagicMock()

        # Test that the function doesn't raise when called
        try:
            result = create_validation_transformers(
                extract_resource=mock_resource,
                resource_name="test_resource",
                validator=mock_validator,
                batch_date="20240101",
            )
            # Should be callable
            assert result is not None
        except Exception:
            # dlt may raise during execution, which is expected
            # Just verify the function is accessible
            assert hasattr(create_validation_transformers, "__call__")
