"""Tests for dlt pipeline orchestration."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from jestr.contracts import ODCSContract
from jestr.pipelines.ingest_oracle import (
    dlt_ingest_oracle,
    execute_oracle_pipeline,
)


@pytest.mark.unit
class TestDltIngestOracle:
    """Test dlt_ingest_oracle source factory."""

    def test_dlt_ingest_oracle_returns_oracle_source(self):
        """Test that dlt_ingest_oracle returns an OracleSource instance."""
        mock_contract = MagicMock(spec=ODCSContract)

        result = dlt_ingest_oracle(
            resource_name="customers",
            batch_date="20240101",
            contract=mock_contract,
            connection_uri="oracle://localhost/orcl",
            database_schema_name="SALES",
            database_table_name="CUSTOMERS",
            batch_size=50000,
        )

        from jestr.engines.dlt.sources.oracle_source import OracleSource

        assert isinstance(result, OracleSource)

    def test_dlt_ingest_oracle_with_default_batch_size(self):
        """Test dlt_ingest_oracle with default batch size."""
        mock_contract = MagicMock(spec=ODCSContract)

        result = dlt_ingest_oracle(
            resource_name="customers",
            batch_date="20240101",
            contract=mock_contract,
            connection_uri="oracle://localhost/orcl",
            database_schema_name="SALES",
            database_table_name="CUSTOMERS",
        )

        from jestr.engines.dlt.sources.oracle_source import OracleSource

        assert isinstance(result, OracleSource)
        assert result.batch_size == 50000


@pytest.mark.unit
class TestExecuteOraclePipeline:
    """Test Oracle pipeline execution."""

    def test_execute_oracle_pipeline_successful(self):
        """Test successful pipeline execution."""
        mock_contract = MagicMock(spec=ODCSContract)
        mock_contract.id = "contract_001"
        mock_contract.version = "1.0"
        mock_contract.status = "active"

        contract_path = Path("/tmp/test_contract.yaml")

        with (
            patch(
                "jestr.pipelines.ingest_oracle.ODCSContract.from_file",
                return_value=mock_contract,
            ) as mock_load,
            patch("jestr.pipelines.ingest_oracle.dlt.pipeline") as mock_pipeline,
            patch(
                "jestr.pipelines.ingest_oracle.operations.run_pipeline_with_summary"
            ) as mock_run,
        ):
            # Setup mocks
            mock_pipeline_instance = MagicMock()
            mock_pipeline_instance.state = {"schema_evolution": {}}
            mock_pipeline.return_value = mock_pipeline_instance

            mock_run.return_value = {
                "resource1": {
                    "source_name": "source1",
                    "validation_metrics": {
                        "total_rows": 100,
                        "clean_rows": 95,
                        "bad_rows": 5,
                    },
                }
            }

            result = execute_oracle_pipeline(
                pipeline_name="oracle_ingestion",
                resource_name="customers",
                connection_uri="oracle://localhost/orcl",
                database_schema_name="SALES",
                database_table_name="CUSTOMERS",
                contract_path=contract_path,
                batch_size=50000,
            )

            assert result["pipeline_name"] == "oracle_ingestion"
            assert "duration_sec" in result
            assert "validation_metrics" in result
            assert "schema_changes_detected" in result
            mock_load.assert_called_once()

    def test_execute_oracle_pipeline_contract_load_failure(self):
        """Test pipeline execution with contract loading failure."""
        contract_path = Path("/tmp/nonexistent_contract.yaml")

        with (
            patch(
                "jestr.pipelines.ingest_oracle.ODCSContract.from_file",
                side_effect=Exception("File not found"),
            ) as mock_load,
            patch("jestr.pipelines.ingest_oracle.logger") as mock_logger,
        ):
            with pytest.raises(ValueError, match="Could not load contract"):
                execute_oracle_pipeline(
                    pipeline_name="oracle_ingestion",
                    resource_name="customers",
                    connection_uri="oracle://localhost/orcl",
                    database_schema_name="SALES",
                    database_table_name="CUSTOMERS",
                    contract_path=contract_path,
                )

            mock_logger.error.assert_called_once()

    def test_execute_oracle_pipeline_schema_evolution_detection(self):
        """Test schema evolution detection in pipeline result."""
        mock_contract = MagicMock(spec=ODCSContract)
        mock_contract.id = "contract_001"
        mock_contract.version = "1.0"
        mock_contract.status = "active"

        contract_path = Path("/tmp/test_contract.yaml")

        with (
            patch(
                "jestr.pipelines.ingest_oracle.ODCSContract.from_file",
                return_value=mock_contract,
            ),
            patch("jestr.pipelines.ingest_oracle.dlt.pipeline") as mock_pipeline,
            patch(
                "jestr.pipelines.ingest_oracle.operations.run_pipeline_with_summary"
            ) as mock_run,
        ):
            # Setup mocks with schema changes
            mock_pipeline_instance = MagicMock()
            mock_pipeline_instance.state = {
                "schema_evolution": {
                    "changes": [
                        {
                            "resource": "customers",
                            "change_type": "new_column",
                            "column": "new_col",
                        }
                    ]
                }
            }
            mock_pipeline.return_value = mock_pipeline_instance

            mock_run.return_value = {}

            result = execute_oracle_pipeline(
                pipeline_name="oracle_ingestion",
                resource_name="customers",
                connection_uri="oracle://localhost/orcl",
                database_schema_name="SALES",
                database_table_name="CUSTOMERS",
                contract_path=contract_path,
            )

            assert result["schema_changes_detected"] is True

    def test_execute_oracle_pipeline_no_schema_evolution(self):
        """Test when no schema evolution occurs."""
        mock_contract = MagicMock(spec=ODCSContract)
        mock_contract.id = "contract_001"
        mock_contract.version = "1.0"
        mock_contract.status = "active"

        contract_path = Path("/tmp/test_contract.yaml")

        with (
            patch(
                "jestr.pipelines.ingest_oracle.ODCSContract.from_file",
                return_value=mock_contract,
            ),
            patch("jestr.pipelines.ingest_oracle.dlt.pipeline") as mock_pipeline,
            patch(
                "jestr.pipelines.ingest_oracle.operations.run_pipeline_with_summary"
            ) as mock_run,
        ):
            # Setup mocks without schema changes
            mock_pipeline_instance = MagicMock()
            mock_pipeline_instance.state = {"schema_evolution": {}}
            mock_pipeline.return_value = mock_pipeline_instance

            mock_run.return_value = {}

            result = execute_oracle_pipeline(
                pipeline_name="oracle_ingestion",
                resource_name="customers",
                connection_uri="oracle://localhost/orcl",
                database_schema_name="SALES",
                database_table_name="CUSTOMERS",
                contract_path=contract_path,
            )

            assert result["schema_changes_detected"] is False

    def test_execute_oracle_pipeline_returns_required_fields(self):
        """Test that result contains all required fields."""
        mock_contract = MagicMock(spec=ODCSContract)
        mock_contract.id = "contract_001"
        mock_contract.version = "1.0"
        mock_contract.status = "active"

        contract_path = Path("/tmp/test_contract.yaml")

        with (
            patch(
                "jestr.pipelines.ingest_oracle.ODCSContract.from_file",
                return_value=mock_contract,
            ),
            patch("jestr.pipelines.ingest_oracle.dlt.pipeline") as mock_pipeline,
            patch(
                "jestr.pipelines.ingest_oracle.operations.run_pipeline_with_summary"
            ) as mock_run,
        ):
            mock_pipeline_instance = MagicMock()
            mock_pipeline_instance.state = {"schema_evolution": {}}
            mock_pipeline.return_value = mock_pipeline_instance

            mock_run.return_value = {
                "resource1": {
                    "validation_metrics": {
                        "total_rows": 100,
                        "clean_rows": 95,
                        "bad_rows": 5,
                    }
                }
            }

            result = execute_oracle_pipeline(
                pipeline_name="oracle_ingestion",
                resource_name="customers",
                connection_uri="oracle://localhost/orcl",
                database_schema_name="SALES",
                database_table_name="CUSTOMERS",
                contract_path=contract_path,
            )

            required_fields = [
                "pipeline_name",
                "loads_ids",
                "duration_sec",
                "schema_changes_detected",
                "validation_metrics",
            ]
            for field in required_fields:
                assert field in result

    def test_execute_oracle_pipeline_batch_date_format(self):
        """Test that batch date is correctly formatted."""
        mock_contract = MagicMock(spec=ODCSContract)
        mock_contract.id = "contract_001"
        mock_contract.version = "1.0"
        mock_contract.status = "active"

        contract_path = Path("/tmp/test_contract.yaml")

        with (
            patch(
                "jestr.pipelines.ingest_oracle.ODCSContract.from_file",
                return_value=mock_contract,
            ),
            patch("jestr.pipelines.ingest_oracle.dlt.pipeline") as mock_pipeline,
            patch(
                "jestr.pipelines.ingest_oracle.operations.run_pipeline_with_summary"
            ) as mock_run,
            patch("jestr.pipelines.ingest_oracle.datetime") as mock_datetime,
        ):
            # Mock current date
            mock_now = MagicMock()
            mock_now.strftime.return_value = "20240115"
            mock_datetime.now.return_value = mock_now

            mock_pipeline_instance = MagicMock()
            mock_pipeline_instance.state = {"schema_evolution": {}}
            mock_pipeline.return_value = mock_pipeline_instance

            mock_run.return_value = {}

            execute_oracle_pipeline(
                pipeline_name="oracle_ingestion",
                resource_name="customers",
                connection_uri="oracle://localhost/orcl",
                database_schema_name="SALES",
                database_table_name="CUSTOMERS",
                contract_path=contract_path,
            )

            # Verify datetime was called with UTC
            mock_datetime.now.assert_called()
