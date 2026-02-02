"""A custom dlt source for Oracle extraction with validation."""

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dlt.extract import DltResource, DltSource


from jestr.contracts import ODCSContract


class OracleSource:
    """A custom dlt source for Oracle extraction with validation."""

    def __init__(
        self,
        resource_name: str,
        contract: ODCSContract,
        connection_uri: str,
        database_schema_name: str,
        database_table_name: str,
        query: str | None = None,
        batch_date: str = "",
        batch_size: int = 50_000,
    ) -> None:
        """Initialize OracleSource with configuration."""
        self.resource_name = resource_name
        self.contract = contract
        self.connection_uri = connection_uri
        self.database_schema_name = database_schema_name
        self.database_table_name = database_table_name
        self.full_table_name = (
            f"{self.database_schema_name}.{self.database_table_name}"
            if self.database_schema_name
            else self.database_table_name
        )
        self.query = query
        self.batch_date = batch_date
        self.batch_size = batch_size

    @classmethod
    def create(
        cls,
        resource_name: str,
        contract: ODCSContract,
        connection_uri: str,
        database_schema_name: str,
        database_table_name: str,
        query: str | None = None,
        batch_date: str = "",
        batch_size: int = 50_000,
    ) -> "OracleSource":
        """Factory method to create an OracleSource instance."""
        return cls(
            resource_name=resource_name,
            contract=contract,
            connection_uri=connection_uri,
            database_schema_name=database_schema_name,
            database_table_name=database_table_name,
            query=query,
            batch_date=batch_date,
            batch_size=batch_size,
        )

    def build_pipeline_flow(self) -> "DltSource":
        """Convert to a dlt source."""
        import dlt

        from ..resources.sql import create_custom_sql_resource
        from ..transformers import create_validation_transformers

        @dlt.source(name=f"ingest__{self.resource_name}")
        def ingest_oracle_source() -> tuple[Callable, ...]:
            def get_resource() -> "DltResource":
                return create_custom_sql_resource(
                    database_type="oracle",
                    connection_uri=self.connection_uri,
                    table_name=self.full_table_name,
                    contract=self.contract,
                    query=self.query,
                    batch_size=self.batch_size,
                    database_schema_name=self.database_schema_name,
                )

            def get_transformers() -> tuple[Callable, Callable, Callable]:
                from jestr.validators import IngestionValidator

                validator = IngestionValidator(
                    contract=self.contract,
                    schema_metadata=None,
                )
                resource = get_resource()
                return create_validation_transformers(
                    extract_resource=resource,
                    resource_name=self.resource_name,
                    validator=validator,
                    batch_date=self.batch_date,
                )

            return get_resource, *get_transformers

        return ingest_oracle_source
