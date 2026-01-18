"""Clean DuckDB-based multi-record source implementation.

This implementation adapts the Polars-based pipeline to use DuckDB, focusing on:
- Memory efficiency using DuckDB's streaming Arrow integration
- Clean separation of concerns
- Proper resource isolation for dlt's async processing
- Post-processing validation of output files
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
    run_pipeline_with_summary,
    setup_validator,
    update_validation_state,
    validate_source_file,
)
from ingestion.io.duckdb import stream_csv_to_arrow
from ingestion.io.multirecord import (
    get_record_type_counts,
    partition_by_record_type,
)
from ingestion.utils.logging import setup_logger


@dlt.source
def multi_layout_source(
    record_types: list[str],
    file_path: str | Path,
    batch_size: int = 50_000,
    write_disposition: str = "replace",
    validator: ArrowValidator | None = None,
    issue_action: str = "quarantine",
) -> Iterator[Any]:
    """Multi-record source using DuckDB with single-pass efficiency.

    This source reads the file once and distributes rows to multiple
    tables using dlt transformers.
    """
    file_path = str(file_path)

    if validator is None:
        validator = ArrowValidator(issue_action=issue_action)

    @dlt.resource(selected=False)
    def raw_batches() -> Iterator[pa.RecordBatch]:
        """Read batches from the file using DuckDB."""
        yield from stream_csv_to_arrow(
            file_path, batch_size=batch_size * 2, header=False, column_prefix="column"
        )

    @dlt.transformer(data_from=raw_batches, selected=False)
    def validated_batches(batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        """Validate batches and route bad records to quarantine."""
        clean_table, bad_table, issues = validator.validate(batch)

        # Update dlt state for persistent validation tracking
        update_validation_state(issues, batch.num_rows, bad_table.num_rows)

        # Route bad records to a dedicated quarantine table if requested
        if bad_table.num_rows > 0 and validator.issue_action == "quarantine":
            yield dlt.mark.with_table_name(bad_table, "quarantine_data")

        if clean_table.num_rows > 0:
            yield clean_table

    @dlt.transformer(data_from=validated_batches, selected=False)
    def partitioned_batches(
        batch: pa.RecordBatch,
    ) -> Iterator[tuple[str, pa.RecordBatch]]:
        """Partition batches by record type."""
        # If this is a marked batch (e.g. quarantine), skip partitioning
        if isinstance(batch, DataItemWithMeta):
            yield batch
            return

        if batch.num_rows == 0:
            return

        # Yield partitions
        yield from partition_by_record_type(batch)

        # Update validation state with record counts
        batch_counts = get_record_type_counts(batch)
        add_record_counts(batch_counts)

    def _create_record_transformer(rt: str):
        """Helper to create a named transformer for a record type."""

        @dlt.transformer(
            data_from=partitioned_batches,
            name=f"record_{rt}",
            write_disposition=write_disposition,
        )
        def record_resource(
            item: Any,
        ) -> Iterator[pa.RecordBatch]:
            if isinstance(item, DataItemWithMeta):
                return
            rt_item, partition = item
            if rt_item == rt:
                yield partition

        return record_resource

    # Yield a transformer for each record type
    for record_type in record_types:
        yield _create_record_transformer(record_type)


if __name__ == "__main__":
    setup_logger("INFO")

    # Configuration
    RECORD_TYPES = [
        "9001",
        "9002",
        "9004",
        "9005",
        "9006",
        "9009",
        "9012",
        "9019",
        "9020",
        "9031",
    ]
    FILE_PATH = Path("data/input/multi_layout_data_xl.txt")

    # Validate source file and setup validator
    FILE_PATH = validate_source_file(FILE_PATH)
    validator = setup_validator(issue_action="quarantine")

    # Setup pipeline
    pipeline = dlt.pipeline(
        pipeline_name="multi_layout_source_pipeline",
        destination="filesystem",
        dataset_name="multi_record_data",
        progress="log",
        dev_mode=True,
    )

    logger.info(f"Pipeline initialised: {pipeline.pipeline_name}")

    # Run pipeline with summary
    run_pipeline_with_summary(
        pipeline,
        multi_layout_source(
            record_types=RECORD_TYPES,
            file_path=FILE_PATH,
            batch_size=50_000,
            write_disposition="replace",
            validator=validator,
        ),
        pipeline_description="multi-record",
    )
