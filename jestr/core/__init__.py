"""Core jestr components and protocols for duck typing."""

from jestr.core.protocols import (
    Adapter,
    ConfigResolver,
    ContractLoader,
    ContractProvider,
    DictLike,
    DltSource,
    StorageAdapter,
)
from jestr.core.registry import (
    AdapterRegistry,
    DatabaseAdapterRegistry,
    SourceAdapterRegistry,
    StorageAdapterRegistry,
)

__all__ = [
    "Adapter",
    "AdapterRegistry",
    "ConfigResolver",
    "ContractLoader",
    "ContractProvider",
    "DatabaseAdapterRegistry",
    "DictLike",
    "DltSource",
    "SourceAdapterRegistry",
    "StorageAdapter",
    "StorageAdapterRegistry",
]
