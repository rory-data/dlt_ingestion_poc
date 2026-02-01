"""Simple factory for creating database adapter instances."""

from typing import TYPE_CHECKING, Any, Literal, overload

if TYPE_CHECKING:
    from .database.oracle import OracleAdapter, OracleConfig
    from .database.teradata import TeradataAdapter, TeradataConfig

DatabaseType = Literal["oracle", "teradata"]


@overload
def create_adapter(
    database_type: Literal["oracle"],
    connection_uri: str | None = None,
    config: "OracleConfig | None" = None,
    **kwargs: Any,
) -> "OracleAdapter": ...


@overload
def create_adapter(
    database_type: Literal["teradata"],
    connection_uri: str | None = None,
    config: "TeradataConfig | None" = None,
    **kwargs: Any,
) -> "TeradataAdapter": ...


def create_adapter(
    database_type: DatabaseType,
    connection_uri: str | None = None,
    config: Any | None = None,
    **kwargs: Any,
) -> Any:
    """Factory function to create database adapter instances."""
    database_type = database_type.lower()
    connection_factory = kwargs.pop("connection_factory", None)
    batch_size = kwargs.pop("batch_size", 50_000)
    max_threads = kwargs.pop("max_threads", 4)

    match database_type:
        case "oracle":
            from .database.oracle import OracleAdapter, OracleConfig

            if config and not isinstance(config, OracleConfig):
                raise TypeError("config must be an instance of OracleConfig")

            cfg = config or OracleConfig()
            if batch_size:
                cfg.batch_size = int(batch_size)

            return OracleAdapter(
                connection_uri=connection_uri,
                connection_factory=connection_factory,
                config=cfg,
                **kwargs,
            )
        case "teradata":
            from .database.teradata import TeradataAdapter, TeradataConfig

            if config and not isinstance(config, TeradataConfig):
                raise TypeError("config must be an instance of TeradataConfig")

            cfg = config or TeradataConfig()
            if batch_size:
                cfg.batch_size = int(batch_size)

            return TeradataAdapter(
                connection_uri=connection_uri,
                connection_factory=connection_factory,
                config=cfg,
                **kwargs,
            )
        case _:
            raise ValueError(f"Unsupported database type: {database_type}")


def list_supported_databases() -> list[DatabaseType]:
    """List all supported database types."""
    return ["oracle", "teradata"]
