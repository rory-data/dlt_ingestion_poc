"""Simple factory for creating adapter instances based on configuration."""

from typing import Any

from jestr.common.exceptions import ConfigurationError
from jestr.common.registry import (
    DataServiceAdapterRegistry,
    _data_service_registry,
    load_plugins,
)


def create_adapter(
    adapter_type: str,
    connection_uri: str | None = None,
    config: Any | None = None,
    network_prefetch_size: int | None = None,
    db_fetch_size: int | None = None,
    arrow_batch_size: int | None = None,
    connection_factory: Any = None,
    registry: DataServiceAdapterRegistry | None = None,
    **kwargs: Any,
) -> Any:
    """Factory function to create a service adapter instance.

    Looks up the adapter class from the registry, populated via __init_subclass__ when adapter
    packages are installed and loaded via load_plugins().

    Args:
        adapter_type: The type of adapter to create (e.g. 'postgres', 'snowflake').
        connection_uri: Optional connection URI for the data service.
        config: Optional adapter configuration parameters. Can be a dict or a BaseAdapterConfig instance.
        network_prefetch_size: Optional override for network prefetch batch size (rows).
        db_fetch_size: Optional override for database fetch batch size (rows).
        arrow_batch_size: Optional override for Arrow RecordBatch size (rows).
        connection_factory: Optional custom connection factory to use instead of the default.
        registry: Optional custom registry to use instead of the default data service registry.
        **kwargs: Additional keyword arguments to pass to the adapter constructor.

    Returns:
        An instance of the requested adapter type.

    Raises:
        ConfigurationError: If the adapter type is not registered or if there are issues with the provided configuration.
    """
    registry_ = registry or _data_service_registry
    connection_factory = connection_factory or registry_.get_registration(adapter_type)
    load_plugins()  # Ensure plugins are loaded before looking up the adapter class
    adapter_type = adapter_type.lower()

    if not registry_.is_registered(adapter_type):
        raise ConfigurationError(
            f"Adapter type '{adapter_type}' is not registered."
            f"Supported types: {', '.join(registry_.list_supported() if registry_.list_supported() else '(none registered)')}"
        )

    adapter_class = registry_.get_class(adapter_type)
    config_class = getattr(adapter_class, "config_model", None)

    if config_class and config and not isinstance(config, config_class):
        raise ConfigurationError(
            f"Config must be an instance of {config_class.__name__}. "
            f"Got {type(config).__name__} instead."
        )

    cfg = config or (config_class() if config_class else None)
    if cfg is not None:
        if network_prefetch_size is not None:
            cfg.network_prefetch_size = network_prefetch_size
        if db_fetch_size is not None:
            cfg.db_fetch_size = db_fetch_size
        if arrow_batch_size is not None:
            cfg.arrow_batch_size = arrow_batch_size

    return adapter_class(
        connection_uri=connection_uri,
        connection_factory=connection_factory,
        config=cfg,
        **kwargs,
    )


def list_supported_adapters() -> list[str]:
    """List all registered adapter types."""
    load_plugins()  # Ensure plugins are loaded before listing supported adapters
    return _data_service_registry.list_supported()
