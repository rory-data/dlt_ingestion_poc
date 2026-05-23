"""Base adapter interface for all service adapters to implement."""

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import pyarrow as pa
import structlog

from jestr.common.exceptions import ConfigurationError

from .config import BaseAdapterConfig
from .connections import ConnectionStrategy

logger = structlog.get_logger(__name__)


class BaseAdapter(ABC):
    """Base adapter interface for all adapters to implement.

    Uses composition over inheritance: the connection lifecycle is delegated to an
    injected ConnectionStrategy rather than inheriting from ConnectionManager.
    """

    def __init_subclass__(cls, adapter_type: str | None = None, **kwargs: Any):
        """Auto-register subclasses with the adapter registry."""
        super().__init_subclass__(**kwargs)
        logger.debug("Initialising adapter subclass:", cls.__name__)
        if adapter_type is not None:
            from jestr.common.registry import _data_service_registry

            _data_service_registry.register(adapter_type, cls)

    def __init__(
        self,
        connection: ConnectionStrategy,
        parquet_writer: Callable | None = None,
        config: BaseAdapterConfig | None = None,
    ) -> None:
        """Initialise the adapter with a connection strategy and optional configuration.

        Args:
            connection: Connection strategy for managing connections to the data service.
            parquet_writer: Optional callable for writing Parquet files.
            config: Adapter configuration parameters. Defaults to BaseAdapterConfig with default values.
        """
        self.connection = connection
        self.parquet_writer = parquet_writer
        self.config = config or BaseAdapterConfig()

    def __enter__(self) -> "BaseAdapter":
        """Enter the context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit the context manager and close any open connections."""
        self.cleanup()

    def cleanup(self) -> None:
        """Cleanup any open connections or resources."""
        self.connection.close()

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        """Context manager for getting a connection from the connection strategy."""
        conn = self.connection.connect()
        try:
            yield conn
        finally:
            self.connection.close()

    @abstractmethod
    def to_arrow(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute a query and return an iterator of PyArrow RecordBatches."""
        ...

    @abstractmethod
    def get_arrow_schema(self, query: str) -> pa.Schema:
        """Get the PyArrow schema for a given query."""
        ...

    @abstractmethod
    def _execute_query_streaming(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute a query and return an iterator of RecordBatches."""
        ...

    def _validate_query(self, query: str) -> None:
        """Validate the query before execution. Can be overridden by subclasses."""
        if not query or not query.strip():
            raise ConfigurationError(
                "Query must be a non-empty string. Received empty or whitespace-only query."
            )
