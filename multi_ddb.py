"""Clean DuckDB-based multi-record source implementation.

This implementation adapts the Polars-based pipeline to use DuckDB, focusing on:
- Memory efficiency using DuckDB's streaming Arrow integration
- Clean separation of concerns
- Proper resource isolation for dlt's async processing
- Post-processing validation of output files
"""

import contextlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from dlt.extract.items import DataItemWithMeta
from loguru import logger

from ingestion.validate.validator import ArrowValidator, ValidationSummary


def parse_trailer_counts(batch: pa.RecordBatch) -> dict[str, int]:
    """Extract expected record counts from trailer records.

    Expected format in columns: "TYPE-COUNT" (e.g., "9001-123")
    """
    expected_counts = {}
    # column(0) is the record type column
    type_col = batch.column(0)
    mask = pc.equal(type_col, "T")
    trailer_rows = batch.filter(mask)

    if trailer_rows.num_rows == 0:
        return {}

    for i in range(1, trailer_rows.num_columns):
        col = trailer_rows.column(i)
        for val in col:
            if val.is_valid:
                s = val.as_py()
                if "-" in s:
                    with contextlib.suppress(ValueError):
                        rec_type, count_str = s.split("-", 1)
                        expected_counts[rec_type] = int(count_str)
    return expected_counts


@dlt.source
def multi_source_duckdb(
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
        # Connect to in-memory DuckDB using context manager for resource cleanup
        with duckdb.connect(":memory:") as con:
            # Create a relation for the CSV file
            rel = con.read_csv(
                file_path,
                header=False,
                sep="|",
                # parallel=False ensures row order is preserved, which is often
                # critical for multi-layout files with headers/trailers.
                parallel=False,
                null_padding=True,
                all_varchar=True,
            )

            # Rename columns to generic names (column_1, column_2, ...)
            # This avoids per-batch renaming in Python
            col_renames = [
                f'"{old}" AS "column_{i + 1}"' for i, old in enumerate(rel.columns)
            ]
            rel = rel.project(", ".join(col_renames))

            # Fetch Arrow reader for streaming using context manager
            # Larger batch size helps reduce Python overhead
            with rel.fetch_arrow_reader(batch_size * 2) as reader:
                for batch in reader:
                    if batch.num_rows > 0:
                        yield batch

    @dlt.transformer(data_from=raw_batches, selected=False)
    def validated_batches(batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        """Validate batches and route bad records to quarantine."""
        clean_table, bad_table, issues = validator.validate(batch)

        # Update dlt state for persistent validation tracking
        state = dlt.current.state()
        summary_dict = state.setdefault("validation_summary", {})
        summary = ValidationSummary.from_dict(summary_dict)
        summary.update(issues, batch.num_rows, bad_table.num_rows)

        # Save back to state
        state["validation_summary"] = summary.to_dict()

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

        # column(0) is the record type column (now named column_1)
        type_col = batch.column(0)

        if batch.num_rows == 0:
            return

        # Get state for count tracking
        state = dlt.current.state()
        summary_dict = state.setdefault("validation_summary", {})
        summary = ValidationSummary.from_dict(summary_dict)

        # Extract trailer counts if present
        trailer_counts = parse_trailer_counts(batch)
        for rt, count in trailer_counts.items():
            if rt in summary.expected_record_counts:
                existing = summary.expected_record_counts[rt]
                if existing != count:
                    logger.warning(
                        f"Duplicate trailer count for {rt} with different value: "
                        f"{count} vs {existing}"
                    )
            summary.expected_record_counts[rt] = count

        # Optimisation: Check if batch is homogeneous
        first_type = type_col[0].as_py()
        last_type = type_col[-1].as_py()

        if first_type == last_type and pc.all(pc.equal(type_col, first_type)).as_py():
            summary.add_record_counts({first_type: batch.num_rows})
            state["validation_summary"] = summary.to_dict()
            yield first_type, batch
            return

        # Batch is heterogeneous
        batch_counts = {}
        for rt_scalar in type_col.unique():
            rt = rt_scalar.as_py()
            mask = pc.equal(type_col, rt)
            filtered_batch = batch.filter(mask)
            batch_counts[rt] = filtered_batch.num_rows
            yield rt, filtered_batch

        summary.add_record_counts(batch_counts)
        state["validation_summary"] = summary.to_dict()

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

    # Setup pipeline
    pipeline = dlt.pipeline(
        pipeline_name="multi_layout_pipeline_duckdb",
        destination="filesystem",
        dataset_name="record_data",
        progress="log",
        dev_mode=True,
    )

    if not FILE_PATH.exists():
        logger.error(f"Source file not found: {FILE_PATH}")
        exit(1)

    # Setup validator
    validator = ArrowValidator(
        issue_action="quarantine",
    )

    # Run pipeline
    load_info = pipeline.run(
        multi_source_duckdb(
            record_types=RECORD_TYPES,
            file_path=FILE_PATH,
            batch_size=50_000,
            write_disposition="replace",
            validator=validator,
        )
    )

    logger.info("=" * 80)
    logger.info("PIPELINE RESULTS")
    logger.info("=" * 80)
    logger.info(f"Load info: {load_info}")

    # Report validation summary from dlt state
    # The state is stored under the source name 'multi_source_duckdb'
    pipeline_state = pipeline.state
    source_state = pipeline_state.get("sources", {}).get("multi_source_duckdb", {})
    summary_dict = source_state.get("validation_summary", {})

    if summary_dict:
        summary = ValidationSummary.from_dict(summary_dict)
        validator.report_summary(summary)

        # Idiomatic trailer verification using accumulated state
        if summary.expected_record_counts:
            validator.verify_trailer(
                summary, summary.expected_record_counts, exclude_types=["H", "T"]
            )

        # Finally, raise if we were in reject mode and found issues
        validator.raise_if_failed(summary)
    else:
        logger.warning("No validation summary found in pipeline state.")
