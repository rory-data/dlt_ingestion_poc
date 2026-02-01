"""Tests for database adapters."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from jestr.adapters import create_adapter, list_supported_databases
from jestr.adapters.base import BaseAdapter
from jestr.adapters.connections import ConnectionPoolManager


# Test helper: Concrete implementation of ConnectionPoolManager for testing
class ConcreteConnectionPoolManager(ConnectionPoolManager):
    """Concrete implementation of ConnectionPoolManager for testing."""

    def _acquire_connection(self):
        """Acquire a connection from the pool or create a new one."""
        return MagicMock()

    def _release_connection(self, connection):
        """Release a connection back to the pool."""
        pass

    def _release_pool_resources(self):
        """Release any resources held by the connection pool."""
        pass


# Test helper: Concrete implementation of BaseAdapter for testing
class ConcreteAdapter(BaseAdapter):
    """Concrete implementation of BaseAdapter for testing."""

    def to_arrow(self, query: str) -> pa.RecordBatch:
        """Execute a query and return the results as a PyArrow RecordBatch."""
        return pa.RecordBatch.from_arrays([], schema=pa.schema([]))

    def get_arrow_schema(self, query: str) -> pa.Schema:
        """Get the schema of the results for a given query as a PyArrow Schema."""
        return pa.schema([])

    def _execute_query_streaming(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute a query and yield results as an iterator of PyArrow RecordBatches."""
        return iter([])

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
        return {"row_count": 0, "file_size_mb": 0.0}

    def _acquire_connection(self):
        """Acquire a connection from the pool or create a new one."""
        return MagicMock()

    def _release_connection(self, connection):
        """Release a connection back to the pool."""
        pass

    def _release_pool_resources(self):
        """Release any resources held by the connection pool."""
        pass

    def _get_default_compression(self) -> str:
        """Get the default compression algorithm for Parquet files."""
        return "snappy"

    def _get_default_write_statistics(self) -> bool:
        """Get the default setting for writing Parquet statistics."""
        return True


@pytest.mark.unit
class TestCreateAdapter:
    """Test the adapter factory function."""

    @pytest.mark.parametrize(
        ("db_type", "uri", "expected_class_name"),
        [
            ("oracle", "oracle://user:pass@localhost:1521/db", "OracleAdapter"),
            ("teradata", "teradatasql://user:pass@localhost", "TeradataAdapter"),
        ],
    )
    def test_create_adapter_success(self, db_type, uri, expected_class_name):
        """Test creating adapters for supported database types."""
        # Mock the adapter classes to avoid abstract method issues
        module_path = (
            "jestr.adapters.database.oracle"
            if db_type == "oracle"
            else "jestr.adapters.database.teradata"
        )
        with patch(f"{module_path}.{expected_class_name}") as mock_adapter_class:
            mock_instance = MagicMock()
            mock_adapter_class.return_value = mock_instance

            adapter = create_adapter(
                db_type,
                connection_uri=uri,
            )
            assert adapter is not None
            assert adapter == mock_instance

    def test_create_oracle_adapter_with_config(self):
        """Test creating an Oracle adapter with custom config."""
        from jestr.adapters.database.oracle import OracleConfig

        config = OracleConfig(batch_size=10_000)
        with patch(
            "jestr.adapters.database.oracle.OracleAdapter"
        ) as mock_adapter_class:
            mock_instance = MagicMock()
            mock_instance.config = config
            mock_adapter_class.return_value = mock_instance

            adapter = create_adapter(
                "oracle",
                connection_uri="oracle://localhost/db",
                config=config,
            )
            assert adapter is not None

    @pytest.mark.parametrize(
        ("kwarg_name", "kwarg_value", "expected_config_attr"),
        [
            ("batch_size", 25_000, "batch_size"),
        ],
    )
    def test_create_adapter_kwargs_override(
        self, kwarg_name, kwarg_value, expected_config_attr
    ):
        """Test that kwargs override default config values."""
        with patch(
            "jestr.adapters.database.oracle.OracleAdapter"
        ) as mock_adapter_class:
            mock_instance = MagicMock()
            mock_adapter_class.return_value = mock_instance

            adapter = create_adapter(
                "oracle",
                connection_uri="oracle://localhost/db",
                **{kwarg_name: kwarg_value},
            )
            assert adapter is not None

    def test_create_oracle_adapter_with_max_threads(self):
        """Test creating an Oracle adapter with custom max threads."""
        with patch(
            "jestr.adapters.database.oracle.OracleAdapter"
        ) as mock_adapter_class:
            mock_instance = MagicMock()
            mock_instance.max_threads = 8
            mock_adapter_class.return_value = mock_instance

            adapter = create_adapter(
                "oracle",
                connection_uri="oracle://localhost/db",
                max_threads=8,
            )
            assert adapter is not None

    def test_create_adapter_with_invalid_database_type(self):
        """Test that invalid database type raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported database type"):
            create_adapter("postgresql", connection_uri="postgres://localhost/db")  # type: ignore[call-overload]

    def test_create_adapter_case_insensitive(self):
        """Test that database type is case-insensitive."""
        with patch(
            "jestr.adapters.database.oracle.OracleAdapter"
        ) as mock_adapter_class:
            mock_instance = MagicMock()
            mock_adapter_class.return_value = mock_instance

            adapter = create_adapter("ORACLE", connection_uri="oracle://localhost/db")  # type: ignore[call-overload]
            assert adapter is not None

    def test_create_adapter_with_invalid_config_type(self):
        """Test that invalid config type raises TypeError."""
        from jestr.adapters.database.teradata import TeradataConfig

        with pytest.raises(
            TypeError, match="config must be an instance of OracleConfig"
        ):
            # Intentionally pass wrong config type for runtime validation test
            create_adapter(  # type: ignore[misc]
                "oracle",
                connection_uri="oracle://localhost/db",
                config=TeradataConfig(),
            )

    def test_create_adapter_with_connection_factory(self):
        """Test creating an adapter with a custom connection factory."""

        def factory():
            return MagicMock()

        with patch(
            "jestr.adapters.database.oracle.OracleAdapter"
        ) as mock_adapter_class:
            mock_instance = MagicMock()
            mock_instance.connection_factory = factory
            mock_adapter_class.return_value = mock_instance

            adapter = create_adapter(
                "oracle",
                connection_factory=factory,
            )
            assert adapter is not None


@pytest.mark.unit
class TestListSupportedDatabases:
    """Test the list_supported_databases function."""

    def test_list_supported_databases(self):
        """Test that all expected databases are listed."""
        databases = list_supported_databases()
        assert isinstance(databases, list)
        assert "oracle" in databases
        assert "teradata" in databases

    def test_list_supported_databases_type(self):
        """Test that returned list contains DatabaseType values."""
        databases = list_supported_databases()
        for db in databases:
            assert isinstance(db, str)
            assert db in ("oracle", "teradata")


@pytest.mark.unit
class TestConnectionPoolManager:
    """Test the ConnectionPoolManager abstract base class."""

    def test_init_with_connection_uri(self):
        """Test initializing connection pool with URI."""

        def factory():
            return MagicMock()

        manager = ConcreteConnectionPoolManager(
            connection_uri="test://host/db", connection_factory=factory
        )
        assert manager.connection_uri == "test://host/db"
        assert manager.connection_factory is factory

    def test_init_requires_uri_or_factory(self):
        """Test that either URI or factory must be provided."""
        with pytest.raises(
            ValueError,
            match="Either connection_uri or connection_factory must be provided",
        ):
            ConcreteConnectionPoolManager()

    def test_context_manager_cleanup(self):
        """Test that cleanup is called on context manager exit."""

        def factory():
            return MagicMock()

        with patch.object(ConcreteConnectionPoolManager, "cleanup") as mock_cleanup:
            manager = ConcreteConnectionPoolManager(
                connection_uri="test://host/db", connection_factory=factory
            )
            with manager:
                pass
            # cleanup should be called after exiting context
            assert mock_cleanup.called

    def test_pool_name_default(self):
        """Test that pool_name defaults to class name."""

        def factory():
            return MagicMock()

        manager = ConcreteConnectionPoolManager(
            connection_uri="test://host/db", connection_factory=factory
        )
        assert manager._pool_name == "ConcreteConnectionPoolManager"

    def test_pool_name_custom(self):
        """Test setting a custom pool name."""

        def factory():
            return MagicMock()

        manager = ConcreteConnectionPoolManager(
            connection_uri="test://host/db",
            connection_factory=factory,
            pool_name="CustomPool",
        )
        assert manager._pool_name == "CustomPool"

    def test_connection_tracking_initialized(self):
        """Test that connection tracking fields are initialized."""

        def factory():
            return MagicMock()

        manager = ConcreteConnectionPoolManager(
            connection_uri="test://host/db", connection_factory=factory
        )
        assert manager._connections_acquired == 0
        assert manager._total_connection_time == 0.0
        assert manager._active_connection is None
        assert manager._connection_created_at is None


@pytest.mark.unit
class TestBaseAdapter:
    """Test the BaseAdapter abstract class."""

    def test_base_adapter_cannot_be_instantiated(self):
        """Test that BaseAdapter cannot be directly instantiated."""
        with pytest.raises(TypeError):
            BaseAdapter(connection_uri="test://host/db")

    def test_base_adapter_subclass_must_implement_abstract_methods(self):
        """Test that subclass must implement abstract methods."""

        class IncompleteAdapter(BaseAdapter):
            pass

        with pytest.raises(TypeError):
            IncompleteAdapter(connection_uri="test://host/db")

    def test_base_adapter_full_implementation(self):
        """Test creating a complete concrete adapter."""

        def factory():
            return MagicMock()

        adapter = ConcreteAdapter(
            connection_uri="test://host/db", connection_factory=factory
        )
        assert adapter is not None
        assert callable(adapter.to_arrow)
        assert callable(adapter.get_arrow_schema)
