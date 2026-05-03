"""Core jestr components and protocols for duck typing."""

from jestr.common.protocols import (
    Adapter,
    ConfigResolver,
    ContractLoader,
    ContractProvider,
    DictLike,
    DltSource,
    StorageAdapter,
)
from jestr.common.registry import (
    AdapterRegistry,
    DataServiceAdapterRegistry,
    SourceAdapterRegistry,
    StorageAdapterRegistry,
)
from jestr.common.settings import ApplicationSettings

__all__ = [
    "Adapter",
    "AdapterRegistry",
    "ApplicationSettings",
    "ConfigResolver",
    "ContractLoader",
    "ContractProvider",
    "DataServiceAdapterRegistry",
    "DictLike",
    "DltSource",
    "SourceAdapterRegistry",
    "StorageAdapter",
    "StorageAdapterRegistry",
]
