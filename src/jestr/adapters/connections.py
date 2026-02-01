"""Connection pool management abstraction."""

import gc
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Generic, TypeVar

import structlog

logger = structlog.get_logger()

ConnectionT = TypeVar("ConnectionT")


class ConnectionPoolManager(ABC, Generic[ConnectionT]):
    """Abstract base class for database connection pooling."""

    def __init__(
        self,
        connection_uri: str | None = None,
        connection_factory: Callable[[], ConnectionT] | None = None,
        pool_name: str | None = None,
    ) -> None:
        """Initialise the connection pool manager."""
        self.connection_uri = connection_uri
        self.connection_factory = connection_factory
        self.pool_name = pool_name

        if not self.connection_uri and not self.connection_factory:
            raise ValueError(
                "Either connection_uri or connection_factory must be provided."
            )

        self._connection_uri = connection_uri
        self._connection_factory = connection_factory
        self._pool_name = pool_name or self.__class__.__name__
        self._active_connection: ConnectionT | None = None
        self._connection_created_at: float | None = None
        self._connections_acquired = 0
        self._total_connection_time = 0.0

        logger.debug(f"Initialised connection pool manager: {self._pool_name}")

    def __enter__(self) -> "ConnectionPoolManager[ConnectionT]":
        """Enter the connection pool context."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit the connection pool context."""
        self.cleanup()

    def cleanup(self) -> None:
        """Clean up resources held by the connection pool."""
        if self._active_connection:
            self._close_active_connection()
            self._release_pool_resources()
            self._trigger_garbage_collection()

            logger.debug(f"Cleaned up connection pool: {self._pool_name}")

    @contextmanager
    def get_connection(self) -> Iterator[ConnectionT]:
        """Context manager to acquire a connection from the pool."""
        connection = None
        start_time = time.time()

        try:
            connection = self._acquire_connection()
            self._active_connection = connection
            self._connections_acquired += 1

            elapsed_time = time.time() - start_time
            self._total_connection_time += elapsed_time
            logger.debug(f"Acquired connection in {elapsed_time:.4f} seconds.")

            yield connection
        except Exception as exc:
            logger.exception(
                "Failed to acquire connections from %s: %s", self._pool_name, exc
            )
            raise
        finally:
            if connection:
                self._release_connection(connection)

    # ABSTRACT METHODS (to be implemented by subclasses
    @abstractmethod
    def _acquire_connection(self) -> ConnectionT:
        """Acquire a connection from the pool or create a new one."""
        ...

    @abstractmethod
    def _release_connection(self, connection: ConnectionT) -> None:
        """Release a connection back to the pool."""
        ...

    @abstractmethod
    def _release_pool_resources(self) -> None:
        """Release any resources held by the connection pool."""
        ...

    # HELPER METHODS
    def _close_active_connection(self) -> None:
        """Close the active connection if it exists."""
        if self._active_connection:
            try:
                self._release_connection(self._active_connection)
                logger.debug("Closed active connection for %s", self._pool_name)

            except Exception as exc:
                logger.warning(
                    "Error closing active connection on $s: %s", self._pool_name, exc
                )

            finally:
                self._active_connection = None
                self._connection_created_at = None

    def _trigger_garbage_collection(self) -> None:
        """Trigger garbage collection to free up resources."""
        gc.collect()
        logger.debug("Triggered garbage collection on %s", self._pool_name)

    def get_connection_metrics(self) -> dict[str, Any]:
        """Get metrics related to connection usage."""
        return {
            "connections_acquired": self._connections_acquired,
            "avg_connection_time_ms": (
                self._total_connection_time / self._connections_acquired * 1000
                if self._connections_acquired > 0
                else 0,
            ),
        }
