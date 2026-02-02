"""Common Parquet utilities for writing Arrow batches."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

logger = structlog.get_logger()

CompressionType = Literal["snappy", "gzip", "brotli", "zstd", "lz4", "none"]


def write_batches_to_parquet_impl(
    batches: Iterator[pa.RecordBatch],
    output_path: Path,
    write_method: str,
    *,
    compression: CompressionType = "snappy",
    use_dictionary: bool = True,
    row_group_size: int | None = None,
    write_statistics: bool = True,
    **parquet_kwargs: Any,
) -> dict[str, Any]:
    """Core implementation for writing Arrow RecordBatches to a Parquet file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    batch_iter = iter(batches)
    first_batch = next(batch_iter, None)

    if first_batch is None:
        logger.warning("No data to write to Parquet file: %s", output_path)
        return {
            "row_count": 0,
            "file_size": 0,
            "file_size_mb": 0,
            "compression": compression,
            "output_path": str(output_path),
        }

    writer_kwargs = {
        "compression": compression,
        "version": "2.6",
        "use_dictionary": use_dictionary,
        "write_statistics": write_statistics,
        "data_page_version": "2.0",
        "write_page_index": True,
    }
    writer_kwargs.update(parquet_kwargs)

    try:
        writer = pq.ParquetWriter(str(output_path), first_batch.schema, **writer_kwargs)
        total_rows = 0

        for batch in [first_batch, *list(batch_iter)]:
            getattr(writer, write_method)(batch)
            total_rows += batch.num_rows

        writer.close()

    except (pa.ArrowException, OSError) as exc:
        logger.exception("Failed to write Parquet file: %s", output_path)
        raise RuntimeError("Parquet write failed") from exc

    # get file size after successful write
    try:
        file_size = output_path.stat().st_size
    except OSError as exc:
        logger.exception("Failed to get file size for: %s", output_path)
        raise RuntimeError("Failed to retrieve Parquet file size") from exc

    result = {
        "row_count": total_rows,
        "file_size": file_size,
        "file_size_mb": file_size / (1024 * 1024),
        "compression": compression,
        "output_path": str(output_path),
        "schema": first_batch.schema.to_string(),
    }

    return result
