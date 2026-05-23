"""Connection management abstraction."""

import contextlib
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Protocol

import structlog

from jestr.common.exceptions import AdapterError, ConfigurationError

logger = structlog.get_logger(__name__)


class ConnectionStrategy(Protocol):
    """Protocol for connection management strategies.

    Implementers provide the connection lifecycle without being coupled to the adapter hierarchy.
    """

    def connect(self) -> None:
        """Establish a connection."""
        ...

    def disconnect(self) -> None:
        """Close the connection."""
        ...

    def close(self) -> bool:
        """Release any resources associated with the connection."""
        ...


class UriConnectionStrategy:
    """Creates a connection strategy from a URI string."""

    def __init__(self, connection_uri: str, factory: Callable[[str], Any]) -> None:
        """Initialise the URI connection strategy with a connection URI and a factory function."""
        self._uri = connection_uri
        self._factory = factory
        self._connection: Any = None

    def connect(self) -> None:
        """Establish a connection, creating it from the URI if not yet open."""
        logger.debug(f"Connecting to {self._uri}")
        return (
            self._factory(self._uri) if self._connection is None else self._connection
        )

    def disconnect(self) -> None:
        """No-op for URI-based connections, as they are stateless."""

    def close(self) -> None:
        """Close the connection if it exists, suppressing any exceptions that occur."""
        with contextlib.suppress(Exception):
            self._connection.close() if self._connection is not None else None


class FactoryConnectionStrategy:
    """Creates a connection strategy from a factory function."""

    def __init__(self, factory: Callable[[], Any]) -> None:
        """Initialise the factory connection strategy with a zero-argument factory function."""
        self._factory = factory
        self._connection: Any = None

    def connect(self) -> None:
        """Establish a connection, creating it from the factory if not yet open."""
        logger.debug(f"Connecting using factory {self._factory}")
        return self._factory() if self._connection is None else self._connection

    def disconnect(self) -> None:
        """No-op for factory-based connections, as they are stateless."""

    def close(self) -> None:
        """Close the connection if it exists, suppressing any exceptions that occur."""
        with contextlib.suppress(Exception):
            self._connection.close() if self._connection is not None else None


class ConnectionManager[ConnectionT](ABC):
    """Abstract base class for managing a single connection.

    Manages the lifecycle of a single connection, providing context management and cleanup
    capabilities. This is not a pool; only one connection is active at a time.
    """

    def __init__(
        self,
        connection_uri: str | None = None,
        connection_factory: Callable[[], ConnectionT] | None = None,
        pool_name: str | None = None,
    ) -> None:
        """Initialise the connection manager."""
        if not connection_uri and not connection_factory:
            raise ConfigurationError(
                "Either connection_uri or connection_factory must be provided. "
                "Cannot initialise ConnectionManager without a connection source."
            )

        self._connection_uri = connection_uri
        self._connection_factory = connection_factory
        self._pool_name = pool_name or self.__class__.__name__
        self._active_connection: ConnectionT | None = None
        self._connections_acquired: int = 0
        self._total_connection_time: float = 0.0

        logger.debug(f"Initialised ConnectionManager with pool name: {self._pool_name}")

    def __enter__(self) -> "ConnectionManager[ConnectionT]":
        """Enter the context manager, returning self for use within the block."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit the context manager, ensuring all connections are closed."""
        self.cleanup()

    def __del__(self) -> None:
        """Safety net destructor to ensure cleanup of connections.

        Catches all exceptions silent; __del__ must never raise. Prefer using the context
        manager for deterministic cleanup, but this ensures resources are released even
        if the user forgets.
        """
        with contextlib.suppress(Exception):
            self.cleanup()

    def cleanup(self) -> None:
        """Cleanup resources held by the connection manager."""
        if self._active_connection:
            self._close_active_connection()
            self._release_pool_resources()
            logger.debug("Cleaned up resources for pool", pool_name=self._pool_name)

    @contextmanager
    def get_connection(self) -> Iterator[ConnectionT]:
        """Context manager to create a connection."""
        connection = None
        start_time = time.time()

        try:
            connection = self._create_connection()
        except AdapterError as exc:
            logger.exception(
                "Failed to create connection. ",
                "Verify service availability and credentials are correct.",
                pool_name=self._pool_name,
            )
            raise AdapterError(
                f"Failed to create connection for pool {self._pool_name}. "
                "Verify service availability and credentials are correct."
            ) from exc

        self._active_connection = connection
        self._connections_acquired += 1
        elapsed_time = time.time() - start_time
        self._total_connection_time += elapsed_time
        logger.debug(
            "Acquired connection from pool",
            elapsed_time_ms=round(elapsed_time * 1000, 2),
        )

        try:
            yield connection
        finally:
            if connection:
                self._close_connection(connection)

    @abstractmethod
    def _create_connection(self) -> ConnectionT:
        """Acquire a connection or create a new one."""
        ...

    @abstractmethod
    def _close_connection(self, connection: ConnectionT) -> None:
        """Close a connection, returning it to the pool or releasing resources."""
        ...

    @abstractmethod
    def _release_pool_resources(self) -> None:
        """Release any resources associated with the connection pool."""
        ...

    def _close_active_connection(self) -> None:
        """Close the active connection if it exists."""
        if self._active_connection:
            try:
                self._close_connection(self._active_connection)
                logger.debug(
                    "Closed active connection for pool", pool_name=self._pool_name
                )
            except Exception as exc:
                logger.warning(
                    "Failed to close active connection during cleanup. "
                    "This may indicate a resource leak.",
                    pool_name=self._pool_name,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                # Continue cleanup despite the error

            finally:
                self._active_connection = None

    def get_connection_metrics(self) -> dict[str, Any]:
        """Get metrics about the connection usage."""
        return {
            "connections_acquired": self._connections_acquired,
            "average_connection_time_ms": (
                self._total_connection_time / self._connections_acquired * 1000
                if self._connections_acquired > 0
                else 0
            ),
        }
