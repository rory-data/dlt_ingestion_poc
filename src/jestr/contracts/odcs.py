"""ODCS contract adapter for schema mapping and data contract handling."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import structlog
from dlt.common.data_types import py_type_to_sc_type
from dlt.common.schema.typing import TColumnSchema, TTableSchema
from dlt.common.schema.utils import new_column
from open_data_contract_standard.model import (
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from jestr.config.mapping import LOGICAL_TO_PY_TYPE_MAP

logger = structlog.get_logger(__name__)

# Set the options for how schema evolution and contract validation are handled
EvolutionMode = Literal["strict", "notify", "auto_evolve"]
ValidationMode = Literal["reject", "quarantine"]


@dataclass
class ODCSContract:
    """Wrapper class for OpenDataContractStandard contracts."""

    odcs: OpenDataContractStandard
    contract_path: Path
    schema_objects: list[SchemaObject] | None = None

    @classmethod
    def from_file(cls, contract_path: str) -> "ODCSContract":
        """Load and resolve an ODCS contract from a file path."""
        try:
            if not Path(contract_path).exists():
                raise FileNotFoundError(
                    "Contract file not found: %s", str(contract_path)
                )

            contract = OpenDataContractStandard.from_file(
                data_contract_location=str(contract_path)
            )
            return cls(
                odcs=contract,
                contract_path=contract_path,
                schema_objects=contract.schema_,
            )
        except Exception as exc:
            logger.exception(
                "Failed to load ODCS contract from %s: %s", contract_path, exc
            )
            raise

    @property
    def id(self) -> str:
        """Get the ID of the ODCS contract."""
        return self.odcs.id or ""

    @property
    def version(self) -> str:
        """Get the version of the ODCS contract."""
        return self.odcs.version or "1.0.0"

    @property
    def status(self) -> str:
        """Get the status of the ODCS contract."""
        return self.odcs.status or "draft"

    @property
    def custom_properties(self) -> dict[str, Any]:
        """Get custom properties from the ODCS contract."""
        val = getattr(self.odcs, "customProperties", None)
        return val if isinstance(val, dict) else {}

    @property
    def evolution_mode(self) -> EvolutionMode:
        """Get the schema evolution mode of the ODCS contract."""
        mode = self.custom_properties.get("evolutionMode", "notify")
        if mode not in ("strict", "notify", "auto_evolve"):
            logger.warning(
                "Invalid schema evolution mode '%s' in contract, defaulting to 'notify'",
                mode,
            )
            return "notify"
        return cast(EvolutionMode, mode)

    @property
    def validation_mode(self) -> ValidationMode:
        """Get the contract validation mode of the ODCS contract."""
        mode = self.custom_properties.get("validationMode", "reject")
        if mode not in ("reject", "quarantine"):
            logger.warning(
                "Invalid contract validation mode '%s' in contract, defaulting to 'reject'",
                mode,
            )
            return "reject"
        return cast(ValidationMode, mode)

    def to_dlt_schemas(self) -> dict[str, TTableSchema]:
        """Convert the ODCS contract to dlt table schemas dictionary."""
        dlt_schemas: dict[str, TTableSchema] = {}

        odcs_schemas = self.odcs.schema_ or []

        if not odcs_schemas:
            return dlt_schemas

        for table in odcs_schemas:
            if not table.name or not table.properties:
                continue

            columns: dict[str, TColumnSchema] = {}
            for prop in table.properties:
                if prop.name:
                    columns[prop.name] = self._build_column_schema(prop)

            table_schema: TTableSchema = {  # type: ignore
                "name": table.name,
                "columns": columns,
            }

            desc_str = self._get_description_str(table.description)
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

            dlt_schemas[table.name] = table_schema

        return dlt_schemas

    def _build_column_schema(self, prop: SchemaProperty) -> TColumnSchema:
        """Build a dlt column schema from an ODCS property."""
        dlt_type, type_extras = self.get_dlt_type_from_odcs_prop(prop)

        col = new_column(
            column_name=prop.name or "",
            data_type=dlt_type,  # type: ignore
            nullable=not prop.required,
            precision=type_extras.get("precision"),
            scale=type_extras.get("scale"),
        )

        # Add dlt column hints
        if prop.primaryKey:
            col["primary_key"] = True

        if prop.unique:
            col["unique"] = True

        # Add description if present
        desc_str = self._get_description_str(prop.description)
        if desc_str:
            col["description"] = desc_str

        return self._add_odcs_hints(col, prop)

    def _add_odcs_hints(
        self, column: TColumnSchema, prop: SchemaProperty
    ) -> TColumnSchema:
        """Add ODCS metadata hints to the dlt column schema."""
        col_dict: dict[str, Any] = cast(dict[str, Any], column)

        if prop.logicalType:
            col_dict["x-odcs-logical-type"] = prop.logicalType
        if prop.physicalType:
            col_dict["x-odcs-physical-type"] = prop.physicalType
        if prop.tags and "PII" in prop.tags:
            col_dict["x-odcs-pii"] = True
        if prop.classification:
            col_dict["x-odcs-classification"] = prop.classification

        if prop.logicalTypeOptions:
            if (min_length := prop.logicalTypeOptions.get("minLength")) is not None:
                col_dict["x-odcs-min-length"] = min_length
            if (max_length := prop.logicalTypeOptions.get("maxLength")) is not None:
                col_dict["x-odcs-max-length"] = max_length

        if prop.quality:
            col_dict["x-odcs-quality-rules"] = prop.quality

        logger.debug(
            "Added ODCS hints to column %s: %s",
            prop.name,
            {k: v for k, v in col_dict.items() if k.startswith("x-odcs-")},
        )

        return column

    def get_dlt_columns(self, table_name: str) -> dict[str, TColumnSchema]:
        """Get dlt column schemas for a specific table in the ODCS contract."""
        dlt_schemas = self.to_dlt_schemas()
        logger.debug("Table schemas in contract: %s", dlt_schemas)
        return dlt_schemas.get(table_name, {}).get("columns", {})

    def get_primary_keys(self, table_name: str) -> list[str]:
        """Get primary key column names for a specific table in the ODCS contract."""
        columns = self.get_dlt_columns(table_name)
        return [
            col_name
            for col_name, col_schema in columns.items()
            if col_schema.get("primary_key")
        ]

    @staticmethod
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

    def get_dlt_type_from_odcs_prop(
        self,
        prop: SchemaProperty,
    ) -> tuple[str, dict[str, int | None]]:
        """Map ODCS property to dlt data type with precision/scale if needed.

        Returns:
            Tuple of (dlt_type, extras) where extras contains precision, scale, etc.
        """
        logical_type = self._get_type(prop) or "string"
        py_type = LOGICAL_TO_PY_TYPE_MAP.get(logical_type, str)

        # Convert to dlt type string
        dlt_type_raw = (
            py_type_to_sc_type(py_type) if isinstance(py_type, type) else py_type
        )
        dlt_type = str(dlt_type_raw)

        # Extract logical type options (precision, scale, etc)
        extras: dict[str, int | None] = {}
        precision = self._get_logical_type_option(prop, "precision")
        if precision is not None:
            extras["precision"] = precision
        scale = self._get_logical_type_option(prop, "scale")
        if scale is not None:
            extras["scale"] = scale

        return dlt_type, extras

    @staticmethod
    def _get_logical_type_option(prop: SchemaProperty, key: str) -> Any:
        """Get a logical type option value from schema property."""
        if prop.logicalTypeOptions is None:
            return None
        return prop.logicalTypeOptions.get(key)

    @staticmethod
    def _get_type(prop: SchemaProperty) -> str | None:
        """Get logical type from schema property, preferring logicalType for interoperability."""
        return prop.logicalType or prop.physicalType
