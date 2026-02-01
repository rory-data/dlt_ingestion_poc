"""Oracle database adapter implementation."""

from collections.abc import Iterator
from typing import Any

import oracledb
import pyarrow as pa
import structlog
from pydantic import BaseModel, Field

from ..base import BaseAdapter

logger = structlog.get_logger()


# CONFIGURATION MODEL
class OracleConfig(BaseModel):
    """Configuration for OracleAdapter."""

    batch_size: int = Field(
        default=50_000,
        description="Number of rows to process in each batch during data operations.",
    )

    cleanup_interval: int = Field(
        default=10,
        description="Trigger garbage collection every N batches (0 = disabled).",
        ge=0,
    )

    write_statistics: bool = Field(
        default=True,
        description="Write column statistics for query optimisation.",
    )

    row_group_size: int | None = Field(
        default=None,
        description="Number of rows per row group in Parquet files (None = use engine defaults).",
    )

    compression: str = "snappy"

    decimal_precision: int = Field(
        default=38,
        description="Precision for decimal types when writing to Parquet files.",
    )

    decimal_scale: int = Field(
        default=10,
        description="Scale for decimal types when writing to Parquet files.",
    )


# MAIN ADAPTER
class OracleAdapter(BaseAdapter):
    """Oracle database adapter."""

    config_model = OracleConfig

    def __init__(
        self,
        connection_uri: str | None = None,
        connection_factory: Any | None = None,
        config: OracleConfig | None = None,
    ) -> None:
        """Initialise OracleAdapter with connection pool and configuration."""
        super().__init__(
            connection_uri=connection_uri,
            connection_factory=connection_factory,
            pool_name="OracleAdapter",
        )
        self.config = config or OracleConfig()
        logger.info("OracleAdapter initialised with connection pool manager")

    def _acquire_connection(self):
        """Acquire Oracle database connection from the pool."""
        from urllib.parse import urlparse

        try:
            if self._connection_factory:
                connection = self._connection_factory()
                logger.debug("Acquired connection via factory")
            else:
                if not self._connection_uri:
                    raise ValueError(
                        "Connection URI must be provided if no factory is set."
                    )

                parsed_uri = urlparse(self._connection_uri)
                if parsed_uri.scheme != "oracle":
                    raise ValueError(
                        "Invalid URI scheme %s for OracleAdapter.", parsed_uri.scheme
                    )

                host = parsed_uri.hostname
                username = parsed_uri.username
                password = parsed_uri.password
                port = parsed_uri.port or 1521
                service_name = parsed_uri.path.lstrip("/")

                if not all([host, username, password, service_name]):
                    raise ValueError(
                        "Invalid connection URI for OracleAdapter. Required: host, username, password, service_name. "
                        "Format: oracle://username:password@host:port/service_name"
                    )

                dsn = oracledb.makedsn(host, port, service_name=service_name)
                connection = oracledb.connect(user=username, password=password, dsn=dsn)
                logger.debug(
                    "Acquired connection via URI", host=host, service_name=service_name
                )

        except Exception as exc:
            raise ImportError(
                "oracledb library is required for OracleAdapter."
            ) from exc
        else:
            return connection

    def _release_connection(self, connection):
        """Release Oracle database connection back to the pool."""
        if connection:
            try:
                connection.close()
                logger.debug("Oracle database connection released.")
            except Exception as exc:
                logger.warning("Error releasing Oracle database connection.", exc)

    def _get_default_compression(self) -> str:
        """Get default compression algorithm for Parquet files."""
        return self.config.compression

    def _get_default_write_statistics(self) -> bool:
        """Get default setting for writing column statistics."""
        return self.config.write_statistics

    def _execute_query_streaming(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute a query and yield results as Arrow Table."""
        with self.get_connection() as conn:
            try:
                logger.debug("Executing query", query=query)
                batch_size = self.config.batch_size
                logger.info("Using fixed batch size: %d", batch_size)

                for odf in conn.fetch_df_batches(query, size=batch_size):
                    batch = pa.record_batch(odf)
                    logger.debug(
                        "Yielding batch of size %d rows, %d columns",
                        batch.num_rows,
                        batch.num_columns,
                    )
                    yield batch

            except Exception as exc:
                logger.exception("Error executing query", exc)
                raise RuntimeError(
                    "Failed to execute query on Oracle database."
                ) from exc

    def to_arrow(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute query and fetch all rows in batches with cleanup."""
        if not query or not query.strip():
            raise ValueError("Query must be a non-empty string.")

        yield from self._execute_query_streaming(query)

    def get_arrow_schema(self, query: str) -> pa.Schema:
        """Get Arrow schema for the result of a query."""
        if not query or not query.strip():
            raise ValueError("Query must be a non-empty string.")

        with self.get_connection() as conn:
            try:
                schema_query = f"SELECT * FROM ({query}) WHERE ROWNUM = 0"  # noqa: S608

                odf = conn.fetch_df_all(schema_query)
                table = pa.table(odf)
                logger.debug("Retrieved schema with %d columns", table.num_columns)

            except Exception as exc:
                logger.exception("Error getting schema for query", exc)
                raise RuntimeError(
                    "Failed to get schema from Oracle database."
                ) from exc

            else:
                return table.schema
