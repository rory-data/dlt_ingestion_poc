"""Clean DuckDB-based multi-record source implementation.

This implementation adapts the Polars-based pipeline to use DuckDB, focusing on:
- Memory efficiency using DuckDB's streaming Arrow integration
- Clean separation of concerns
- Proper resource isolation for dlt's async processing
- Post-processing validation of output files

Design Philosophy:
- Use DuckDB's `read_csv` for high-performance I/O
- Process batches using Arrow for seamless dlt integration
- Maintain single-pass efficiency by partitioning batches
- Post-process validation ensures data integrity
"""

import glob
from collections.abc import Iterator
from pathlib import Path

import dlt
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger


@dlt.source
def multi_source_duckdb(
    record_types: list[str],
    file_path: str,
    batch_size: int = 50_000,
    write_disposition: str = "replace",
):
    """Multi-record source using DuckDB with single-pass efficiency.

    This source reads the file once and distributes rows to multiple
    tables using dlt transformers.
    """

    @dlt.resource(selected=False)
    def partitioned_batches():
        """Read batches and partition them by record type once."""
        # Connect to in-memory DuckDB
        con = duckdb.connect(":memory:")

        # Create a relation for the CSV file
        rel = con.read_csv(
            file_path,
            header=False,
            sep="|",
            parallel=True,
            null_padding=True,
            all_varchar=True,
        )

        # Rename columns once in the relation to avoid per-batch renaming in Python
        col_names = rel.columns
        projection = ", ".join(
            [f'"{old}" AS "column_{i + 1}"' for i, old in enumerate(col_names)]
        )
        rel = rel.project(projection)

        # Get an Arrow reader for streaming
        # Larger batch size helps reduce the number of batches processed in Python
        reader = rel.fetch_arrow_reader(batch_size * 2)

        for batch in reader:
            if batch.num_rows == 0:
                continue

            # column(0) is the record type column (now named column_1)
            type_col = batch.column(0)

            # Check if the batch is homogeneous.
            # We use a fast check: if first and last are same, we still need to verify
            # the whole column to be safe since we removed the global sort.
            first_type = type_col[0].as_py()
            last_type = type_col[batch.num_rows - 1].as_py()

            is_homogeneous = False
            if first_type == last_type:
                # Verify all elements match first_type
                # This is still faster than full partitioning if it succeeds
                mask = pa.compute.equal(type_col, first_type)
                if pa.compute.all(mask).as_py():
                    is_homogeneous = True

            if is_homogeneous:
                # Entire batch is one type - zero-copy yield
                yield first_type, batch
            else:
                # Batch spans multiple types (happens at boundaries or if interleaved)
                # Use Arrow's unique() to find record types present in this batch
                for rt_scalar in type_col.unique():
                    rt = rt_scalar.as_py()
                    mask = pa.compute.equal(type_col, rt)
                    yield rt, batch.filter(mask)

    # Create a single shared instance of the partitioned batches
    source_data = partitioned_batches()

    def make_transformer(rt: str):
        """Helper to create a named transformer for a record type."""

        @dlt.transformer(
            data_from=source_data,
            name=f"record_{rt}",
            write_disposition=write_disposition,
        )
        def record_resource(item):
            rt_item, partition = item
            if rt_item == rt:
                yield partition

        return record_resource

    # Yield a transformer for each record type
    for record_type in record_types:
        yield make_transformer(record_type)


def validate_parquet_outputs(
    pipeline_storage_path: str,
    file_path: str,
    record_types: list[str],
) -> dict[str, dict]:
    """Post-processing validation of generated parquet files using DuckDB."""
    logger.info("=" * 80)
    logger.info("POST-PROCESSING VALIDATION (DUCKDB)")
    logger.info("=" * 80)

    # Extract expected counts from trailer record
    expected_counts = _extract_trailer_counts(file_path, record_types)

    if not expected_counts:
        logger.warning("No trailer record found or unable to extract counts")
    else:
        logger.info(f"Extracted counts from trailer record: {expected_counts}")

    results = {}
    total_dest_rows = 0

    for record_type in record_types:
        # Use DuckDB to count rows across all matching parquet files
        pattern = str(
            Path(pipeline_storage_path) / "**" / f"*record_{record_type}*.parquet"
        )

        total_rows = 0
        num_files = 0
        errors = []

        try:
            # Check if any files match the pattern first to avoid DuckDB IO Error
            file_list = glob.glob(pattern, recursive=True)
            num_files = len(file_list)

            if num_files > 0:
                # DuckDB's read_parquet is extremely fast as it only reads metadata for counts
                res = duckdb.query(
                    f"SELECT count(*) FROM read_parquet('{pattern}')"
                ).fetchone()
                total_rows = res[0] if res else 0
                total_dest_rows += total_rows
                logger.info(
                    f"Validating record type {record_type}: {total_rows} rows in {num_files} files"
                )
            else:
                logger.warning(f"No parquet files found for record type {record_type}")
                errors.append("No files found")

        except Exception as e:
            error = f"Failed to validate {record_type}: {e}"
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
            "files": num_files,
            "total_rows": total_rows,
            "expected_rows": expected,
            "count_match": count_match,
            "errors": errors,
        }

    # Global validation: sum of trailer counts should match total destination rows
    total_expected = sum(expected_counts.values())
    global_match = total_dest_rows == total_expected
    logger.info(
        f"\nGlobal Validation: Expected Rows ({total_expected}) == Dest Rows ({total_dest_rows})"
    )
    if global_match:
        logger.success("✓ Global row count matches!")
    else:
        logger.error(
            f"✗ Global row count mismatch! Difference: {total_expected - total_dest_rows}"
        )

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)

    all_valid = all(r["status"] == "valid" for r in results.values()) and global_match

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
        raise ValueError("Pipeline validation failed! Check logs for details.")

    logger.info("=" * 80)

    return results


def _extract_trailer_counts(file_path: str, record_types: list[str]) -> dict[str, int]:
    """Extract record counts from trailer record (`T`) in source file using DuckDB."""
    try:
        # Use DuckDB to find the trailer line
        # We read the file as a single column (no separator) to find the 'T|' line
        res = duckdb.query(
            f"SELECT column0 FROM read_csv('{file_path}', header=False, sep='\\0') WHERE column0 LIKE 'T|%'"
        ).fetchone()

        if not res:
            return {}

        trailer_line = res[0]
        parts = trailer_line.split("|")[1:]

        trailer_counts = {}
        for part in parts:
            if "-" in part:
                rec_type, count_str = part.split("-", 1)
                try:
                    count = int(count_str)
                    if rec_type in record_types:
                        trailer_counts[rec_type] = count
                except ValueError:
                    logger.warning(f"Could not parse count from: {part}")
        return trailer_counts

    except Exception as e:
        logger.error(f"Failed to extract trailer counts: {e}")
        return {}


if __name__ == "__main__":
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
        pipeline_name="multi_layout_pipeline_duckdb",
        destination="filesystem",
        dataset_name="record_data_duckdb_clean",
        progress="log",
    )

    # Run pipeline
    # Using the same file path as in multi_pl.py for consistency
    file_path = "/Users/rory/github/sandbox/multi_layout_data_xl.txt"

    # Clear previous output for clean validation in this demo
    import shutil

    output_dir = Path("/Users/rory/github/sandbox/output/record_data_duckdb_clean")
    if output_dir.exists():
        shutil.rmtree(output_dir)

    if not Path(file_path).exists():
        logger.error(f"Source file not found: {file_path}")
    else:
        load_info = pipeline.run(
            multi_source_duckdb(
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
