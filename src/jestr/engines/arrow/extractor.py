"""Arrow Batch Extractor: Convert DBAPI cursor rows to Arrow RecordBatches with memory management."""

from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import structlog

logger = structlog.get_logger()


class ArrowBatchExtractor:
    """Convert DBAPI cursor rows to Arrow RecordBatches with memory management."""

    def __init__(
        self,
        schema: pa.Schema,
        *,
        memory_pool: pa.MemoryPool | None = None,
    ) -> None:
        """Initialise ArrowBatchExtractor."""
        self.schema = schema
        self.memory_pool = memory_pool or pa.default_memory_pool()
        self._batch_count = 0

    def __enter__(self) -> "ArrowBatchExtractor":
        """Enter context manager for ArrowBatchExtractor."""
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, exc_tb: Any) -> None:
        """Exit context manager for ArrowBatchExtractor."""
        self.cleanup()
        return None

    def cleanup(self) -> None:
        """Perform memory cleanup for Arrow memory pool."""
        import gc

        logger.debug(
            "Cleaning up Arrow memory pool after %d batches", self._batch_count
        )
        gc.collect()
        logger.debug(
            "Memory cleanup complete. Current memory usage: %d bytes",
            self.memory_pool.bytes_allocated(),
        )

    def extract_cursor_batches(
        self,
        cursor: Iterator[tuple],
        *,
        batch_size: int = 50_000,
        cleanup_interval: int = 10,
    ) -> Iterator[pa.RecordBatch]:
        """Extract RecordBatches from a DBAPI cursor iterator."""
        batch_count = 0

        try:
            while True:
                # Fetch from database cursor
                try:
                    rows = cursor.fetchmany(batch_size)
                except Exception as exc:
                    logger.exception("Error fetching rows from batch %d", batch_count)
                    raise RuntimeError("Fetch failed at batch %d", batch_count) from exc

                if not rows:
                    logger.debug("Fetch complete: %d batches", batch_count)
                    break

                # Convert to Arrow RecordBatch
                try:
                    arrays = self._rows_to_columns(rows)
                    batch = pa.RecordBatch.from_arrays(arrays, schema=self.schema)
                    del arrays  # Free temporary arrays

                    batch_count += 1
                    logger.debug(
                        "Extracted batch %d with %d rows", batch_count, batch.num_rows
                    )

                    yield batch

                    if cleanup_interval > 0 and batch_count % cleanup_interval == 0:
                        logger.debug("Periodic cleanup at batch %d", batch_count)
                        self.cleanup()

                    del rows  # Free fetched rows

                except (pa.ArrowTypeError, pa.ArrowInvalid) as exc:
                    logger.exception("Arrow conversion error at batch %d", batch_count)
                    raise RuntimeError(
                        "Arrow conversion failed at batch %d", batch_count
                    ) from exc

        finally:
            logger.debug("Iterator complete, final batch count: %d", batch_count)

    def _rows_to_columns(self, rows: list[tuple]) -> list[pa.Array]:
        """Convert list of row tuples to list of PyArrow Arrays."""
        columns = list(zip(*rows, strict=True))
        return [
            pa.array(col, type=self.schema.field(i).type)
            for i, col in enumerate(columns)
        ]
