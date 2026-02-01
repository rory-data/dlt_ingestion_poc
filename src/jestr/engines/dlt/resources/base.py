"""Base utilities for dlt resources."""

import dlt
import structlog
from dlt.extract import DltResource

from jestr.adapters import create_adapter
from jestr.contracts import ODCSContract

logger = structlog.get_logger()


def create_adapter_resource(
    adapter_type: str,
    connection_uri: str,
    resource_name: str,
    contract: ODCSContract,
    query: str | None = None,
    batch_size: int = 50_000,
    source_schema_name: str | None = None,
) -> DltResource:
    """Create a dlt resource backed by adapter-driven extraction."""
    columns = contract.get_dlt_columns(resource_name) or {}
    primary_keys = contract.get_primary_keys(resource_name) or []
    schema_contract = contract.to_dlt_schema_contract()

    adapter = create_adapter(adapter_type, connection_uri)

    def data_generator():
        """Lazy generator for resource data."""
        yield from adapter.to_arrow(query)

    resource = dlt.resource(
        data_generator,
        name=f"raw__{resource_name}",
        columns=columns,
        primary_key=primary_keys,
        schema_contract=schema_contract,
        write_disposition="replace",
    )

    logger.info("Extract resource created for %s.", resource_name)

    resource.apply_hints(
        columns=columns,
        primary_keys=primary_keys if primary_keys else None,
        schema_contract=schema_contract,
        # Store ODCS properties as `x-odcs-*` metadata hints in the dlt schema
        additional_resource_hints={
            "x-odcs-contract-id": contract.contract_id,
            "x-odcs-contract-version": contract.version,
            "x-odcs-contract-status": contract.status,
        },
    )

    logger.debug("Extraction resource hints applied for %s.", resource_name)

    return resource
