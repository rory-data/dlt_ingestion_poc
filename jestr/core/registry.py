"""Extensible adapter registry with plugin discovery.

This module provides the AdapterRegistry base class with auto-registration support via
__init_subclass__, module-level singleton registries, and a load_plugins() function that
discovers installed jestr adapter packages via entry points.
"""

from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class AdapterRegistry:
    """Base registry class for managing adapter registrations."""

    def __init__(self) -> None:
        """Initialise the adapter registry."""
        self._registry: dict[str, Any] = {}

    def register(self, adapter_type: str, adapter_class: type) -> None:
        """Register an adapter class with a specific type."""
        if not adapter_type or not adapter_type.strip():
            raise ValueError("Adapter type must be a non-empty string.")
        self._registry[adapter_type.lower()] = adapter_class

    def get_class(self, adapter_type: str) -> type:
        """Get the adapter class for a given type."""
        if not adapter_type or not adapter_type.strip():
            raise ValueError("Adapter type must be a non-empty string.")
        try:
            return self._registry[adapter_type.lower()]
        except KeyError:
            raise KeyError(
                f"Adapter type '{adapter_type}' is not registered."
                f"Supported types: {', '.join(self.list_supported() if self.list_supported() else '(none registered)')}"
            ) from None

    def create(self, adapter_type: str, **kwargs: Any) -> Any:
        """Create an instance of the adapter for a given type."""
        return self.get_class(adapter_type)(**kwargs)

    def list_supported(self) -> list[str]:
        """List all registered adapter types."""
        return sorted(self._registry.keys())

    def is_registered(self, adapter_type: str) -> bool:
        """Check if an adapter type is registered."""
        return adapter_type.lower() in self._registry

    def get_registration(self, adapter_type: str) -> type | Any:
        """Get the registered adapter class or instance for a given type, or None if not registered."""
        return self._registry.get(adapter_type.lower())

    def reset(self) -> None:
        """Reset the registry by clearing all registered adapters."""
        self._registry.clear()
        global _plugins_loaded
        _plugins_loaded = False


class DatabaseAdapterRegistry(AdapterRegistry):
    """Registry for database adapters."""


class StorageAdapterRegistry(AdapterRegistry):
    """Registry for storage adapters."""


class SourceAdapterRegistry(AdapterRegistry):
    """Registry for dlt source adapters."""


# Module-level singleton registries
_database_registry: DatabaseAdapterRegistry = DatabaseAdapterRegistry()
_source_ragistry: SourceAdapterRegistry = SourceAdapterRegistry()

_plugins_loaded: bool = False


def load_plugins() -> None:
    """Discover and load adapter plugins from installed packages.

    This function uses entry points to find and load adapter plugins from installed
    packages. It ensures that plugins are only loaded once per session.
    """
    import importlib.metadata

    global _plugins_loaded
    if _plugins_loaded:
        logger.debug("Plugins have already been loaded, skipping.")
        return
    _plugins_loaded = True

    for point in importlib.metadata.entry_points(group="jestr.adapters"):
        try:
            plugin_class = point.load()
            plugin_class.register()
            logger.info("Loaded jestr plugin %s from %s.", point.name, point.module)
        except Exception:
            logger.exception(
                "Failed to load jestr plugin %s from %s.",
                point.name,
                point.module,
                exc_info=True,
            )
