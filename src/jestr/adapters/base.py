"""Base adapter interface for database extractors."""

from abc import abstractmethod
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import structlog

from .connections import ConnectionPoolManager

logger = structlog.get_logger(__name__)


class BaseAdapter(ConnectionPoolManager):
    """Base adapter interface for database extractors."""

    def __init__(
        self,
        connection_uri: str | None = None,
        connection_factory: Any | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialise the base adapter."""
        super().__init__(
            connection_uri=connection_uri,
            connection_factory=connection_factory,
        )

    @abstractmethod
    def to_arrow(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute a query and return the results as PyArrow RecordBatch iterator."""
        ...

    @abstractmethod
    def get_arrow_schema(self, query: str) -> pa.Schema:
        """Get the schema of the results for a given query as a PyArrow Schema."""
        ...

    @abstractmethod
    def _execute_query_streaming(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute a query and yield results as an iterator of PyArrow RecordBatches."""
        ...

    @abstractmethod
    def _write_to_parquet_impl(
        self,
        batches: Iterator[pa.RecordBatch],
        output_path: Path,
        *,
        compression: str,
        write_statistics: bool,
        row_group_size: int | None,
        **parquet_kwargs: Any,
    ) -> dict[str, Any]:
        """Write an iterator of PyArrow RecordBatches to a Parquet file."""
        ...

    def to_parquet(
        self,
        query: str,
        output_path: Path,
        compression: str | None = None,
        row_group_size: int | None = None,
        *,
        write_statistics: bool | None = None,
        **parquet_kwargs: Any,
    ) -> None:
        """Execute a query and write the results to a Parquet file."""
        compression = compression or self._get_default_compression()
        write_statistics = (
            write_statistics
            if write_statistics is not None
            else self._get_default_write_statistics()
        )

        logger.info("Writing query results to Parquet file at %s", output_path)

        try:
            batches = self._execute_query_streaming(query)
            result = self._write_to_parquet_impl(
                batches,
                output_path,
                compression=compression,
                write_statistics=write_statistics,
                row_group_size=row_group_size,
                **parquet_kwargs,
            )
            logger.info(
                "Successfully wrote Parquet file at %s with %d rows at %.2f MB",
                output_path,
                result["row_count"],
                result["file_size_mb"],
            )
            return result
        except Exception as exc:
            logger.exception(
                "Failed to write Parquet file at %s: %s",
                output_path,
                exc,
            )
            raise

    def _get_default_compression(self) -> str:
        """Get the default compression codec for Parquet files."""
        return "snappy"

    def _get_default_write_statistics(self) -> bool:
        """Determine whether to write statistics to Parquet files by default."""
        return True
