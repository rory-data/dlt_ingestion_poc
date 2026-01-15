"""ODCS contract adapter for schema mapping and data contract handling.

Follows the datacontract-cli exporter pattern to provide dlt-compatible
schema generation from ODCS data contracts.
"""

from typing import Any, TypeAlias

from datacontract.export.exporter import Exporter
from datacontract.lint.resolve import resolve_data_contract
from dlt.common.data_types import py_type_to_sc_type
from dlt.common.schema.typing import TColumnSchema, TTableSchema
from dlt.common.schema.utils import new_column
from open_data_contract_standard.model import (
    Description,
    OpenDataContractStandard,
    SchemaProperty,
)

from .mapping import get_python_type

DltTableSchemas: TypeAlias = dict[str, TTableSchema]


def _get_description_str(description: str | Description | None) -> str | None:
    """Extract description string from either a string or Description object.

    Follows the pattern used in datacontract-cli exporters.
    """
    if description is None:
        return None
    if isinstance(description, str):
        return description.strip().replace("\n", " ")
    # Description object - use purpose field
    if hasattr(description, "purpose") and description.purpose:
        return description.purpose.strip().replace("\n", " ")
    return None


def _get_logical_type_option(prop: SchemaProperty, key: str) -> Any:
    """Get a logical type option value from schema property."""
    if prop.logicalTypeOptions is None:
        return None
    return prop.logicalTypeOptions.get(key)


class DltExporter(Exporter):
    """Exporter for converting ODCS data contracts to dlt-compatible schemas.

    Follows the datacontract-cli exporter pattern for consistency with the
    wider data contract ecosystem.
    """

    def export(
        self,
        data_contract: OpenDataContractStandard,
        schema_name: str,
        server: str,
        sql_server_type: str,
        export_args: dict[str, Any],
    ) -> DltTableSchemas:
        """Export ODCS contract to dlt table schemas.

        Args:
            data_contract: The ODCS data contract specification.
            schema_name: The name of the schema to export, or 'all' for all schemas.
            server: Not used in this implementation.
            sql_server_type: Not used in this implementation.
            export_args: Additional export arguments (not used).

        Returns:
            Dictionary mapping table names to dlt TTableSchema definitions.
        """
        return to_dlt(data_contract)


def _get_type(prop: SchemaProperty) -> str | None:
    """Get logical type from schema property, preferring physicalType for accuracy."""
    if prop.physicalType:
        return prop.physicalType
    return prop.logicalType


def get_dlt_type_from_odcs_prop(
    prop: SchemaProperty,
) -> tuple[str, dict[str, int | None]]:
    """Map ODCS property to dlt data type with precision/scale if needed.

    Returns:
        Tuple of (dlt_type, extras) where extras contains precision, scale, etc.
    """
    logical_type = _get_type(prop) or "string"

    # Get base Python type
    py_type = get_python_type(logical_type)

    # Convert to dlt type string - cast to str for TypedDict compatibility
    dlt_type_raw = py_type_to_sc_type(py_type) if isinstance(py_type, type) else py_type
    dlt_type = str(dlt_type_raw)

    # Extract logical type options (precision, scale, etc.)
    extras: dict[str, int | None] = {}
    precision = _get_logical_type_option(prop, "precision")
    if precision is not None:
        extras["precision"] = precision
    scale = _get_logical_type_option(prop, "scale")
    if scale is not None:
        extras["scale"] = scale

    return dlt_type, extras


def _build_column_schema(prop: SchemaProperty) -> TColumnSchema:
    """Build a dlt column schema from an ODCS property.

    Uses dlt.common.schema.utils.new_column() for standard construction,
    adding hints for primary key, unique, and description as needed.
    """
    dlt_type, type_extras = get_dlt_type_from_odcs_prop(prop)

    # Use dlt's standard column constructor
    precision = type_extras.get("precision")
    scale = type_extras.get("scale")
    col = new_column(
        column_name=prop.name or "",
        data_type=dlt_type,  # type: ignore
        nullable=not prop.required,
        precision=precision,  # type: ignore
        scale=scale,  # type: ignore
    )

    # Add dlt column hints
    if prop.primaryKey:
        col["primary_key"] = True

    if prop.unique:
        col["unique"] = True

    # Add description if present
    desc_str = _get_description_str(prop.description)
    if desc_str:
        col["description"] = desc_str

    return col


def to_dlt(
    data_contract: OpenDataContractStandard,
) -> DltTableSchemas:
    """Convert ODCS data contract to dlt table schemas dictionary.

    Maps all ODCS schema objects and properties to dlt-compatible table schemas,
    preserving metadata, constraints, and type information.

    Args:
        data_contract: The resolved ODCS data contract.

    Returns:
        Dictionary mapping table names to complete dlt table schemas.
        Each table schema includes columns, references, and metadata.
    """
    schemas: dict[str, TTableSchema] = {}

    if not data_contract.schema_:
        return schemas

    for table in data_contract.schema_:
        if not table.name or not table.properties:
            continue

        columns: dict[str, TColumnSchema] = {}
        for prop in table.properties:
            if prop.name:
                columns[prop.name] = _build_column_schema(prop)

        table_schema: TTableSchema = {  # type: ignore
            "name": table.name,
            "columns": columns,
        }

        desc_str = _get_description_str(table.description)
        if desc_str:
            table_schema["description"] = desc_str

        # Extract table-level references (foreign keys) if present
        if hasattr(table, "references") and table.references:
            refs = table.references if isinstance(table.references, list) else []
            processed_refs: list[dict[str, Any]] = []
            for ref in refs:
                ref_dict: dict[str, Any] = ref if isinstance(ref, dict) else {}  # type: ignore[assignment]
                processed_refs.append(
                    {
                        "columns": (
                            ref_dict.get("columns", [])
                            if isinstance(ref_dict.get("columns"), list)
                            else [ref_dict.get("columns")]
                        ),
                        "referenced_table": ref_dict.get("referencedTable"),
                        "referenced_columns": (
                            ref_dict.get("referencedColumns", [])
                            if isinstance(ref_dict.get("referencedColumns"), list)
                            else [ref_dict.get("referencedColumns")]
                        ),
                    }
                )
            table_schema["references"] = processed_refs  # type: ignore[typeddict-unknown-key]

        schemas[table.name] = table_schema

    return schemas


def get_dlt_schemas(contract_path: str) -> DltTableSchemas:
    """Load and map dlt table schemas from ODCS data contract.

    This function:
    - Resolves the ODCS contract from file path
    - Maps all ODCS properties to dlt column schemas
    - Extracts table-level metadata (descriptions)
    - Captures constraint and hint information
    - Handles logical type options (precision, scale)

    Args:
        contract_path: Path to the OpenDataContractStandard YAML file.

    Returns:
        Dictionary mapping table names to complete dlt table schemas.
        Each table schema includes columns, references, and metadata.
    """
    contract = resolve_data_contract(data_contract_location=contract_path)
    return to_dlt(contract)


def get_table_requirements(schemas: dict[str, Any], table_name: str) -> dict[str, Any]:
    """Extract dlt-ready column definitions, keys, and references for a table.

    Args:
        schemas: Dictionary of dlt table schemas (e.g., from get_dlt_schemas).
        table_name: Name of the table to extract requirements for.

    Returns:
        Dictionary containing:
        - columns: List of dlt column definitions (TColumnSchema list).
        - primary_keys: List of primary key column names.
        - references: Table-level references/foreign keys.
        - column_names: List of all column names in order.
    """
    table_schema = schemas.get(table_name, {})

    # Extract columns dict
    if isinstance(table_schema, dict) and "columns" in table_schema:
        columns_def = table_schema["columns"]
        # Convert dict values to list for dlt transformer columns argument
        table_schema_cols = list(columns_def.values())
    else:
        table_schema_cols = table_schema if isinstance(table_schema, list) else []

    column_names = [c["name"] for c in table_schema_cols]
    primary_keys = [c["name"] for c in table_schema_cols if c.get("primary_key")]
    references = (
        table_schema.get("references") if isinstance(table_schema, dict) else None
    )

    return {
        "columns": table_schema_cols,
        "primary_keys": primary_keys,
        "references": references,
        "column_names": column_names,
    }
