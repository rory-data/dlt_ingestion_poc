"""Protocol definitions for duck typing support in jestr.

This module defines Protocol interfaces using typing.Protocol to enable structural
subtyping (duck typing) instead of nominal subtyping (isinstance checks). This allows
components to work with any object implementing the required interface, improving
extensibility and reducing coupling.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    import pyarrow as pa

    from jestr.orchestration.context import IngestionContext

T = TypeVar(name="T")
K = TypeVar(name="K")
V = TypeVar(name="V")


@runtime_checkable
class DltSource(Protocol):
    """Protocol for DLT data sources.

    Any object that implements this protocol can be used as a DLT source without needing to
    inherit from a base class. This enables pluggable source implementations and simplifies
    testing.

    Used by: SourceFactory, PipelineOrchestrator, dlt engines
    """

    def extract(self) -> Any:
        """Execute the extraction logic for this source.

        Returns:
            DLT source/resource object that can be consumed by dlt pipelines.
        """
        ...

    def validate(self) -> dict[str, Any]:
        """Validate extracted data against contract requirements.

        Returns:
            Dictionary with validation results including:
            - "valid": bool indicating if data passes validation
            - "errors": list of validation errors (if any)
            - "metrics": dict with validation metrics
        """
        ...

    def get_schema_requirements(self) -> dict[str, Any]:
        """Get schema requirements from the source's contract.

        Returns:
        Dictionary with schema definition including tables, columns, types, and constraints.
        """
        ...


@runtime_checkable
class DictLike(Protocol):
    """Protocol for dict-like objects.

    Any object supporting get) with default value can be used interchangeably with dicts,
    enabling flexible configuration handling.

    Used by: Config resolution, parameter handling
    """

    def get(self, key: K, default: V | None = None) -> V | None:
        """Get value for key, returning default if not found.

        Args:
            key: The key to look up.
            default: Value to return if key is not found.

        Returns:
            The value associated with key, or default if not found.
        """
        ...


@runtime_checkable
class ContractLoader(Protocol):
    """Protocol for loading data contracts.

    Any contract loader implementing this protocol can be swapped in, support different
    contract formats (ODCS, JSON Schema, etc.) or storage backends (file, database, HTTP).

    Used by: PipelineOrchestrator, contract resolution
    """

    def load(self, path: str | Path) -> Any:
        """Load and parse a contract from the given path.

        Args:
            path: Path to contract file (local, HTTP, etc.)

        Returns:
            Contract object (e.g. ODCSContract) with schema and metadata.

        Raises:
            ContractLoadError: If contract cannot be loaded or parsed.
            FileNotFoundError: If path does not exist.
        """
        ...


@runtime_checkable
class ConfigResolver(Protocol):
    """Protocol for resolving configuration values.

    Any resolver implementing this protocol can provide configurations from different
    sources (environment, files, vaults, databases, etc.) or apply transformations (secret
    resolution, substitution, etc.).

    Used by: PipelineOrchestrator, source factory
    """

    def resolve(self, **kwargs: Any) -> dict[str, Any]:
        """Resolve configuration from provided arguments.

        Args:
            **kwargs: Input parameters to resolve (source_params, secrets, etc.)

        Returns:
            Dictionary with resolved configuration Including:
            - "storage_adapter": Storage backend for dit
            - "endpoint_url": Optional endpoint for s3, etc.
            - Other resolved parameters
        """
        ...


@runtime_checkable
class StorageAdapter(Protocol):
    """Protocol for storage adapters.

    Any storage adapter implementing this protocol can be used to write data to different
    backends (local filesystem, S3, GCS, etc.) without changing the core logic.

    Used by: ConfigResolver, PipelineOrchestrator, dlt engines
    """

    @property
    def backend_type(self) -> str:
        """Get the type of storage backend (e.g. "local", "s3", "gcs")."""
        ...

    @property
    def support_cloud(self) -> bool:
        """Returns True if this is a cloud storage backend."""
        ...

    def get_path(self) -> str:
        """Get the normalised storage path for this backend."""
        ...

    def get_dlt_destination(self, **kwargs: Any) -> dict[str, Any]:
        """Get DLT destination configuration for this storage adapter.

        Args:
            **kwargs: Additional parameters for generating the destination config.

        Returns:
            Dictionary with DLT destination configuration (bucket, credentials, etc.)

        Raises:
            ConfigurationError: If required parameters are missing or invalid.
        """
        ...

    def validate(self) -> None:
        """Validate the storage backend is accessible and credentials valid.

        Raises:
            ConfigurationError: If storage backend is not accessible or credentials are invalid.
        """
        ...


@runtime_checkable
class Adapter(Protocol):
    """Protocol for data services adapters.

    Any adapter implementing this protocol can be used to extract or load data between
    different data services without changing consumer code.

    Used by: SourceFactory, data pipelines
    """

    def connect(self, **kwargs: Any) -> None:
        """Establish connection to the data service.

        Args:
            **kwargs: Connection parameters (host, port, credentials, etc.)

        Raises:
            AdapterError: If connection cannot be established with provided parameters.
        """
        ...

    def execute(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute a query and return a PyArrow RecordBatch iterator.

        Args:
            query: The query to execute against the data service.

        Returns:
            An iterator yielding PyArrow RecordBatch objects with the query results.

        Raises:
            AdapterError: If query execution fails.
        """
        ...

    def close(self) -> None:
        """Close the connection to the data service and release resources.

        Called when adapter is no longer required. Should be safe to call multiple times.
        """
        ...


@runtime_checkable
class ContractProvider(Protocol):
    """Protocol for providing data contracts by name.

    Any contract provider implementing this protocol can be used to supply contracts from
    different sources (local files, HTTP, databases, etc.) without changing consumer code.

    Used by: Pipeline orchestration, contract resolution
    """

    def get_contract(self, name: str) -> Any:
        """Get a data contract by name.

        Args:
            name: Contract identifier or name.

        Returns:
            Contract object (e.g. ODCSContract) with schema and metadata.

        Raises:
            ContractLoadError: If contract cannot be loaded or parsed.
        """
        ...


class PipelineStage(Protocol):
    """Protocol for a single pipeline execution stage."""

    def execute(self, ctx: "IngestionContext") -> None:
        """Execute this stage against the shared context."""
        ...


class DatabaseAdapter(Protocol):
    """Protocol for database adapters."""

    def to_arrow(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute a query and yield results as Arrow RecordBatches."""
        ...

    def get_arrow_schema(self, query: str) -> pa.Schema:
        """Get the Arrow schema for the result set of a query."""
        ...

    def to_parquet(self, query: str, output_path: Path, **kwargs) -> dict[str, Any]:
        """Execute a query and write results to Parquet at the specified path."""
        ...
