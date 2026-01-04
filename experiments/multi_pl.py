"""Clean Polars-based multi-record source implementation.

This implementation is designed from scratch for Polars, focusing on:
- Memory efficiency with streaming-like behavior
- Clean separation of concerns
- Proper resource isolation for dlt's async processing
- Post-processing validation of output files

Design Philosophy:
- Use Polars' lazy evaluation (scan_csv) for memory efficiency
- Process each record type independently (isolated resources)
- Yield Arrow Tables incrementally for dlt's streaming pipeline
- Polars owns all memory - no buffer corruption issues
- Post-process validation ensures data integrity

References:
- dlt documentation: https://dlthub.com/docs
- Polars documentation: https://docs.pola.rs/
"""

import glob
from collections.abc import Iterator
from pathlib import Path

import dlt
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger


@dlt.source
def multi_source_polars(
    record_types: list[str],
    file_path: str,
    batch_size: int = 50_000,
    write_disposition: str = "replace",
):
    """Multi-record source using Polars with single-pass efficiency.

    This source reads the file once and distributes rows to multiple
    tables using dlt transformers.

    Key Features:
    - Single-pass I/O: File is read once for all record types.
    - Polars partitioning: Efficient row distribution within each batch.
    - Isolated Resources: Each record type remains a selectable dlt resource.
    """

    @dlt.resource(selected=False)
    def partitioned_batches():
        """Read batches and partition them by record type once."""
        reader = pl.read_csv_batched(
            file_path,
            has_header=False,
            separator="|",
            batch_size=batch_size * 10,
            low_memory=True,
        )
        while (batches := reader.next_batches(1)) is not None:
            # Partition the batch once for all downstream transformers
            yield batches[0].partition_by("column_1", as_dict=True)

    # Create a single shared instance of the partitioned batches
    source_data = partitioned_batches()

    def make_transformer(rt: str):
        """Helper to create a named transformer for a record type."""

        @dlt.transformer(
            data_from=source_data,
            name=f"record_{rt}",
            write_disposition=write_disposition,
        )
        def record_resource(partitions):
            key = (rt,)
            if key in partitions:
                yield partitions[key].to_arrow()

        return record_resource

    # Yield a transformer for each record type
    for record_type in record_types:
        yield make_transformer(record_type)


def validate_parquet_outputs(
    pipeline_storage_path: str,
    file_path: str,
    record_types: list[str],
) -> dict[str, dict]:
    """Post-processing validation of generated parquet files.

    Validates:
    1. Record counts in parquet metadata match counts from source file trailer record (`T`)

    Args:
        pipeline_storage_path: Path to dlt pipeline storage
        file_path: Path to the source multi-layout file
        record_types: List of record types to validate

    Returns:
        Dict with validation results per record type

    Example:
        >>> results = validate_parquet_outputs(
        ...     pipeline._pipeline_storage.storage_path,
        ...     "/path/to/data.txt",
        ...     ["9001", "9002"]
        ... )
    """
    logger.info("=" * 80)
    logger.info("POST-PROCESSING VALIDATION")
    logger.info("=" * 80)

    # Extract expected counts from trailer record
    expected_counts = _extract_trailer_counts(file_path, record_types)

    if not expected_counts:
        logger.warning("No trailer record found or unable to extract counts")
    else:
        logger.info(f"Extracted counts from trailer record: {expected_counts}")

    # Find all parquet files
    parquet_pattern = str(Path(pipeline_storage_path) / "**" / "*.parquet")
    parquet_files = glob.glob(parquet_pattern, recursive=True)

    logger.info(f"Found {len(parquet_files)} parquet files to validate")

    results = {}

    for record_type in record_types:
        # Find files for this record type
        type_files = [
            f for f in parquet_files if f"record_{record_type}" in Path(f).name
        ]

        if not type_files:
            logger.warning(f"No parquet files found for record type {record_type}")
            results[record_type] = {
                "status": "missing",
                "files": 0,
                "total_rows": 0,
                "errors": ["No files found"],
            }
            continue

        logger.info(
            f"\nValidating record type {record_type} ({len(type_files)} files):"
        )

        total_rows = 0
        errors = []

        for file_path in type_files:
            try:
                # Read parquet metadata to get row count
                parquet_file = pq.ParquetFile(file_path)
                file_rows = parquet_file.metadata.num_rows
                total_rows += file_rows

                logger.info(f"  {Path(file_path).name}: {file_rows} rows")

            except Exception as e:
                error = f"Failed to read {Path(file_path).name}: {e}"
                errors.append(error)
                logger.error(f"    ✗ {error}")

        # Check against expected count from trailer record
        expected = expected_counts.get(record_type)
        count_match = total_rows == expected if expected is not None else None

        if expected is not None and not count_match:
            error = f"Count mismatch: expected {expected}, got {total_rows}"
            errors.append(error)
            logger.error(f"  ✗ {error}")

        results[record_type] = {
            "status": "valid" if not errors else "invalid",
            "files": len(type_files),
            "total_rows": total_rows,
            "expected_rows": expected,
            "count_match": count_match,
            "errors": errors,
        }

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)

    all_valid = all(r["status"] == "valid" for r in results.values())

    for record_type, result in results.items():
        status_icon = "✓" if result["status"] == "valid" else "✗"
        logger.info(
            f"{status_icon} {record_type}: {result['total_rows']} rows "
            f"in {result['files']} files - {result['status'].upper()}"
        )

        if result["errors"]:
            for error in result["errors"]:
                logger.error(f"    - {error}")

    if all_valid:
        logger.success("\n✅ All validations passed!")
    else:
        logger.error("\n✗ Some validations failed!")

    logger.info("=" * 80)

    return results


def _extract_trailer_counts(file_path: str, record_types: list[str]) -> dict[str, int]:
    """Extract record counts from trailer record (`T`) in source file.

    The trailer record format is:
        T|9001-0000001000|9002-0000000500|...

    Args:
        file_path: Path to the source multi-layout file
        record_types: List of record types to extract counts for

    Returns:
        Dict mapping record_type -> expected row count
    """
    trailer_counts = {}

    try:
        # Read file tail to find trailer record
        with open(file_path, "rb") as f:
            # Seek to end and read last chunk
            f.seek(0, 2)
            file_size = f.tell()

            # Read last 1KB (or less if file is smaller)
            chunk_size = min(1024, file_size)
            f.seek(-chunk_size, 2)
            chunk = f.read().decode("utf-8", errors="ignore")

            # Get the last line (trailer)
            lines = chunk.strip().split("\n")
            trailer_line = lines[-1].strip()

            # Validate it's a trailer record
            if not trailer_line.startswith("T|"):
                logger.error(f"Last line is not a trailer record: {trailer_line[:50]}")
                return trailer_counts

            # Parse trailer: T|rec_type-count|rec_type-count|...
            parts = trailer_line.split("|")[1:]  # Skip the "T" prefix

            for part in parts:
                if "-" in part:
                    rec_type, count_str = part.split("-", 1)
                    try:
                        count = int(count_str)
                        if rec_type in record_types:
                            trailer_counts[rec_type] = count
                    except ValueError:
                        logger.warning(f"Could not parse count from: {part}")

    except Exception as e:
        logger.error(f"Failed to extract trailer counts: {e}")

    return trailer_counts


# Example usage
if __name__ == "__main__":
    pl.disable_string_cache()  # Disable string cache for memory efficiency

    record_types = [
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

    # Define pipeline
    pipeline = dlt.pipeline(
        pipeline_name="multi_layout_pipeline_polars_clean",
        destination="filesystem",
        dataset_name="record_data_polars_clean",
        progress="log",
    )

    # Run pipeline
    file_path = "/Users/rory/github/sandbox/multi_layout_data_xl.txt"

    load_info = pipeline.run(
        multi_source_polars(
            record_types=record_types,
            file_path=file_path,
            batch_size=50_000,
            write_disposition="replace",
        ),
        write_disposition="replace",
    )

    logger.info("=" * 80)
    logger.info("PIPELINE RESULTS")
    logger.info("=" * 80)
    logger.info(f"Load info: {load_info}")

    # Post-processing validation
    validation_results = validate_parquet_outputs(
        pipeline._pipeline_storage.storage_path,
        file_path,
        record_types,
    )
