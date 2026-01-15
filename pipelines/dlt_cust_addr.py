"""Clean DuckDB-based pipe-delimited text file source implementation.

Efficiently reads standard pipe-delimited text files using DuckDB's streaming
Arrow integration, applies validation, and loads to dlt.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
import pyarrow as pa
from dlt.destinations import filesystem
from ingestion.engines.dlt.operations import (
    load_odcs_schemas,
    run_pipeline_with_summary,
    setup_validator,
    validate_source_file,
)
from ingestion.engines.dlt.transformers import (
    cast_columns_to_dlt_types,
    create_casted_table,
)
from loguru import logger

from ingestion.core.reporting.state import update_validation_state
from ingestion.core.validate.validator import ArrowValidator
from ingestion.io.duckdb import rename_csv_columns, stream_csv_to_arrow
from ingestion.schema.odcs import get_table_requirements
from ingestion.utils.logging import setup_logger


@dlt.source
def dlt_customer_address(
    table_name: str,
    file_path: str | Path,
    schemas: dict[str, Any] | None = None,
    batch_size: int = 50_000,
    write_disposition: str = "replace",
    validator: ArrowValidator | None = None,
    issue_action: str = "quarantine",
) -> Iterator[Any]:
    """Load pipe-delimited text file with validation.

    Staging: Reads input -> Staging Parquet.
    Validation: Reads Staging -> Validation -> Good/Bad Parquet & Yield Clean.

    Args:
        table_name: Name of the table to load data into.
        file_path: Path to the input file.
        schemas: Dictionary of dlt column definitions.
        batch_size: Rows per batch for processing.
        write_disposition: dlt write disposition.
        validator: ArrowValidator instance.
        issue_action: Action for bad records.
    """
    file_path = str(file_path)
    schemas = schemas or {}
    validator = validator or ArrowValidator(issue_action=issue_action)

    # Extract table-level configuration from schemas
    config = get_table_requirements(schemas, table_name)
    table_schema_cols = config["columns"]
    column_names = config["column_names"]
    primary_keys = config["primary_keys"]
    references = config["references"]

    @dlt.resource(name=f"staging_{table_name}")
    def stage_data() -> Iterator[pa.Table]:
        """Read raw data, write to staging parquet, and yield arrow tables."""
        for batch in stream_csv_to_arrow(
            file_path, batch_size=batch_size, header=True, column_prefix="column"
        ):
            # Convert batch to table
            table = pa.Table.from_batches([batch])

            # Rename columns to target schema
            renamed_table = rename_csv_columns(table, column_names)

            # Yield to pipeline
            yield renamed_table

    @dlt.transformer(
        data_from=stage_data,
        name=table_name,
        write_disposition=write_disposition,
        columns=table_schema_cols,
        primary_key=primary_keys if primary_keys else None,
        schema_contract="evolve" if write_disposition == "replace" else None,
        references=references,
    )
    def validate_and_split(batch_table: pa.Table) -> Iterator[pa.Table]:
        """Validate arrow table and split into good/bad records."""
        # Validate
        clean_table, bad_table, issues = validator.validate(batch_table)

        # Update dlt state
        update_validation_state(issues, batch_table.num_rows, bad_table.num_rows)

        # Yield Bad Data to Quarantine Table
        if bad_table.num_rows > 0:
            yield dlt.mark.with_table_name(bad_table, f"quarantine_{table_name}")

        # Process Clean Data
        if clean_table.num_rows > 0:
            # Cast columns to target types
            casted_arrays, valid_cast = cast_columns_to_dlt_types(
                clean_table, table_schema_cols
            )

            if valid_cast:
                # Create result table
                result_table = create_casted_table(
                    casted_arrays, clean_table.schema.names
                )

                # Yield for dlt loading
                yield result_table
            else:
                # Casting failed - quarantine batch
                logger.warning("Quarantining batch due to type casting failure")
                yield dlt.mark.with_table_name(clean_table, f"quarantine_{table_name}")

    return stage_data, validate_and_split


if __name__ == "__main__":
    setup_logger("DEBUG")
    logger.info("Starting pipe-delimited customer address pipeline...")

    FILE_PATH = Path("data/input/customer_address.txt")
    CONTRACT_PATH = "data/schemas/import/customer_address_odcs.yaml"
    TABLE_NAME = "customer_address"

    # Validate and load schemas
    FILE_PATH = validate_source_file(FILE_PATH)
    schemas = load_odcs_schemas(CONTRACT_PATH)

    # Configure validator
    validator = setup_validator(issue_action="quarantine")

    # Create pipeline with explicit schema management
    pipeline = dlt.pipeline(
        pipeline_name="customer_address_pipeline_fs",
        destination=filesystem(
            bucket_url=f"file://{Path.cwd()}/data/output/dlt_storage"
        ),
        dataset_name="customer_address_data",
        dev_mode=True,
    )

    logger.info(f"Pipeline initialised: {pipeline.pipeline_name}")

    # Run the pipeline
    run_pipeline_with_summary(
        pipeline,
        dlt_customer_address(
            table_name=TABLE_NAME,
            file_path=FILE_PATH,
            schemas=schemas,
            batch_size=50_000,
            write_disposition="replace",
            validator=validator,
        ),
        pipeline_description="customer address",
    )
