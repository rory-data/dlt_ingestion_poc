"""Oracle to Parquet dlt pipeline with contract-driven governance."""

import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import dlt
import structlog
from dlt.destinations import filesystem

from jestr.contracts import ODCSContract
from jestr.engines.dlt import OracleSource, operations

logger = structlog.get_logger()


def dlt_ingest_oracle(
    resource_name: str,
    batch_date: str,
    contract: ODCSContract,
    connection_uri: str,
    database_schema_name: str,
    database_table_name: str,
    batch_size: int = 50_000,
) -> Callable:
    """Load and validate data from Oracle to Parquet using dlt with contract governance."""
    source_factory = OracleSource.create(
        resource_name=resource_name,
        contract=contract,
        connection_uri=connection_uri,
        database_schema_name=database_schema_name,
        database_table_name=database_table_name,
        batch_date=batch_date,
        batch_size=batch_size,
    )
    return source_factory


def execute_oracle_pipeline(
    pipeline_name: str,
    resource_name: str,
    connection_uri: str,
    database_schema_name: str,
    database_table_name: str,
    contract_path: Path,
    batch_size: int = 50_000,
) -> dict:
    """Execute the Oracle to Parquet dlt pipeline with contract governance."""
    batch_date = datetime.now(UTC).strftime("%Y%m%d")

    try:
        contract = ODCSContract.from_file(str(contract_path))
        logger.info(
            "Loaded contract from %s",
            contract_path,
            contract_id=contract.id,
            contract_version=contract.version,
            contract_status=contract.status,
        )
    except Exception as exc:
        logger.error("Failed to load contract from %s: %s", contract_path, exc)
        raise ValueError(f"Could not load contract from {contract_path}") from exc

    logger.info(
        "Starting Oracle to Parquet pipeline",
        pipeline_name=pipeline_name,
        resource_name=resource_name,
    )

    start_time = time.time()

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        dataset_name=f"oracle_{resource_name}",
        destination=filesystem(bucket_url=os.getenv("DLT_STORAGE_PATH")),
        progress="log",
    )

    validation_metrics = operations.run_pipeline_with_summary(
        pipeline,
        dlt_ingest_oracle(
            resource_name=resource_name,
            batch_date=batch_date,
            contract=contract,
            connection_uri=connection_uri,
            database_schema_name=database_schema_name,
            database_table_name=database_table_name,
            batch_size=batch_size,
        ),
        pipeline_description=f"Oracle to Parquet ingestion for {resource_name}",
        contract=contract,
    )

    duration_sec = time.time() - start_time

    schema_changes_detected = bool(
        pipeline.state.get("schema_evolution", {}).get("changes")
    )

    return {
        "pipeline_name": pipeline_name,
        "loads_ids": [],
        "duration_sec": duration_sec,
        "schema_changes_detected": schema_changes_detected,
        "validation_metrics": validation_metrics,
    }
