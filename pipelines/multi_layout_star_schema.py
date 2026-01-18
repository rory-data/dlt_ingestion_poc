"""Clean DuckDB-based multi-record source implementation.

Efficiently reads multi-record format files using DuckDB's streaming Arrow
integration, partitions by record type, applies validation, and loads to dlt.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
import pyarrow as pa
from dlt.extract.items import DataItemWithMeta
from loguru import logger

from ingestion.core.validate.validator import ArrowValidator
from ingestion.engines.dlt import (
    add_record_counts,
    cast_columns_to_dlt_types,
    create_casted_table,
    drop_first_column,
    select_and_rename_columns,
    update_validation_state,
)
from ingestion.engines.dlt.operations import (
    load_odcs_schemas,
    run_pipeline_with_summary,
    setup_validator,
    validate_source_file,
)
from ingestion.io.duckdb import stream_csv_to_arrow
from ingestion.io.multirecord import (
    get_record_type_counts,
    partition_by_record_type,
)
from ingestion.schema.odcs import get_table_requirements
from ingestion.utils.logging import setup_logger


@dlt.source
def multi_layout_star_schema(
    record_types: list[str] | dict[str, str],
    file_path: str | Path,
    schemas: dict[str, Any] | None = None,
    batch_size: int = 50_000,
    write_disposition: str = "replace",
    validator: ArrowValidator | None = None,
    issue_action: str = "quarantine",
) -> Iterator[Any]:
    """Load multi-record format files with record type partitioning.

    Reads the input file once via DuckDB, applies validation, partitions by
    record type, and yields transformers that dlt will execute in parallel.

    Args:
        record_types: List of record types to extract, or dict mapping
            record type codes to their schema names in the ODCS.
        file_path: Path to the input file.
        schemas: Dictionary of dlt column definitions from get_dlt_schemas.
        batch_size: Number of rows per batch for DuckDB reading.
        write_disposition: How to write the data (replace, append, merge).
        validator: ArrowValidator instance for data quality checks.
        issue_action: Action for bad records (quarantine, reject, report).
    """
    file_path = str(file_path)
    schemas = schemas or {}
    validator = validator or ArrowValidator(issue_action=issue_action)

    # Normalise record_types to dict mapping
    rt_mapping = (
        record_types
        if isinstance(record_types, dict)
        else {rt: rt for rt in record_types}
    )

    @dlt.resource(selected=False)
    def raw_batches() -> Iterator[pa.RecordBatch]:
        """Stream batches from the input file via DuckDB."""
        yield from stream_csv_to_arrow(
            file_path, batch_size=batch_size * 2, header=False, column_prefix="column"
        )

    @dlt.transformer(data_from=raw_batches, selected=False)
    def validated_batches(batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        """Validate batches and route bad records to quarantine."""
        clean_table, bad_table, issues = validator.validate(batch)

        # Update dlt state for persistent validation tracking
        update_validation_state(issues, batch.num_rows, bad_table.num_rows)

        # Quarantine bad records if requested and present
        if bad_table.num_rows > 0 and validator.issue_action == "quarantine":
            yield dlt.mark.with_table_name(bad_table, "quarantine_data")

        if clean_table.num_rows > 0:
            yield clean_table

    @dlt.transformer(data_from=validated_batches, selected=False)
    def partitioned_batches(batch: pa.Table) -> Iterator[tuple[str, pa.Table]]:
        """Partition clean batches by record type."""
        # Skip processing for marked batches (e.g., quarantine)
        if isinstance(batch, DataItemWithMeta):
            yield batch
            return

        if batch.num_rows == 0:
            return

        # Yield partitions by record type
        yield from partition_by_record_type(batch)

        # Update validation state with record counts
        batch_counts = get_record_type_counts(batch)
        add_record_counts(batch_counts)

    # Create transformers for each record type
    def make_record_transformer(rt: str, schema_name: str) -> Any:
        """Create a transformer for a specific record type.

        This transformer:
        - Filters incoming data by record type
        - Removes the record type marker column
        - Casts columns to target dlt types
        - Applies schema hints (primary keys, constraints)

        Args:
            rt: Record type code (e.g., "10", "20")
            schema_name: Corresponding table name in ODCS (e.g., "DimDate")

        Returns:
            A dlt transformer function
        """
        # Extract table-level configuration from schemas
        config = get_table_requirements(schemas, schema_name)
        table_schema_cols = config["columns"]
        column_names = config["column_names"]
        primary_keys = config["primary_keys"]
        references = config["references"]

        @dlt.transformer(
            data_from=partitioned_batches,
            name=f"{schema_name.lower()}_data",  # Use schema name, dlt-idiomatic naming
            table_name=schema_name,  # Explicit table name
            write_disposition=write_disposition,
            columns=table_schema_cols,
            primary_key=primary_keys if primary_keys else None,
            schema_contract="evolve" if write_disposition == "replace" else None,
            references=references,
        )
        def record_resource(item: Any) -> Iterator[pa.Table]:
            """Transform and cast columns for this record type."""
            # Skip marked batches (e.g., quarantine data)
            if isinstance(item, DataItemWithMeta):
                return

            rt_item, partition = item
            if rt_item != rt or partition.num_rows == 0:
                return

            # Drop record type column (first column)
            data = drop_first_column(partition)

            # Ensure we only use columns that exist in the schema
            data = select_and_rename_columns(data, column_names)

            # Cast columns to target types
            casted_arrays, valid_cast = cast_columns_to_dlt_types(
                data, table_schema_cols
            )

            if valid_cast:
                # Yield successfully casted table
                result_table = create_casted_table(casted_arrays, data.schema.names)
                yield result_table
            else:
                # Casting failed - quarantine partition
                logger.warning(f"Quarantining record {rt} due to type casting failure")
                yield dlt.mark.with_table_name(partition, "quarantine_data")

        return record_resource

    # Yield a transformer for each record type
    for rt, schema_name in rt_mapping.items():
        yield make_record_transformer(rt, schema_name)


if __name__ == "__main__":
    setup_logger("DEBUG")

    logger.info("Starting multi-record star schema pipeline...")

    # Configuration
    RECORD_TYPES = {
        "10": "DimDate",
        "20": "DimCustomer",
        "30": "DimProduct",
        "40": "DimStore",
        "50": "FactSales",
    }
    FILE_PATH = Path("data/input/star_multi_layout_data.txt")
    CONTRACT_PATH = "data/schemas/import/star_odcs.yaml"

    # Validate and load schemas
    FILE_PATH = validate_source_file(FILE_PATH)
    schemas = load_odcs_schemas(CONTRACT_PATH)

    # Configure validator
    validator = setup_validator(issue_action="quarantine")

    # Create pipeline with explicit schema management
    pipeline = dlt.pipeline(
        pipeline_name="multi_layout_star_schema_pipeline",
        destination="duckdb",
        dataset_name="star_schema_record_data",
        dev_mode=True,
    )

    logger.info(f"Pipeline initialized: {pipeline.pipeline_name}")

    # Run the pipeline
    run_pipeline_with_summary(
        pipeline,
        multi_layout_star_schema(
            record_types=RECORD_TYPES,
            file_path=FILE_PATH,
            schemas=schemas,
            batch_size=50_000,
            write_disposition="replace",
            validator=validator,
        ),
        pipeline_description="multi-layout star schema",
    )
