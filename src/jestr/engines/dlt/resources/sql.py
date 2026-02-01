"""Create dlt resources for SQL database extraction."""

import structlog
from dlt.extract import DltResource, DltResourceException

from jestr.contracts import ODCSContract

from .base import create_adapter_resource

logger = structlog.get_logger()


def create_custom_sql_resource(
    database_type: str,
    connection_uri: str,
    table_name: str,
    contract: ODCSContract,
    query: str | None = None,
    batch_size: int = 50_000,
    database_schema_name: str | None = None,
) -> DltResource:
    """Create a custom dlt resource for extracting data from a SQL database."""
    # Validate inputs
    if not connection_uri:
        raise ValueError("Connection URI must be provided.")
    if not table_name:
        raise ValueError("Table name must be provided.")

        logger.info("Creating custom SQL resource via adapter for %s.", table_name)

    try:
        resource = create_adapter_resource(
            adapter_type=database_type,
            connection_uri=connection_uri,
            resource_name=table_name,
            contract=contract,
            query=query,
            batch_size=batch_size,
            source_schema_name=database_schema_name,
        )
        logger.info("Custom SQL resource created successfully for %s.", table_name)
        return resource
    except DltResourceException as exc:
        logger.exception(
            "Failed to create custom SQL resource.",
            table_name=table_name,
            error_details=exc,
        )
        raise
