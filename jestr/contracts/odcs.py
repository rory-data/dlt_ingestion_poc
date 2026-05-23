"""ODCS contract adapter for schema mapping and data contract handling."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import structlog
import yaml
from dlt.common.data_types import TDataType, py_type_to_sc_type
from dlt.common.schema.typing import (
    TColumnSchema,
    TSchemaContract,
    TTableReferenceInline,
    TTableSchema,
)
from dlt.common.schema.utils import new_column
from open_data_contract_standard.model import (
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)

from jestr.common.constants import (
    FILE_BASED_SERVER_TYPES,
    EvolutionMode,
    ValidationMode,
)
from jestr.common.exceptions import ContractLoadError, YamlLocation
from jestr.common.maps import LOGICAL_TO_PY_TYPE_MAP

logger = structlog.get_logger(__name__)


@dataclass
class ODCSContract:
    """Wrapper class representing the ODCS contract for a data pipeline run.

    This class serves as an adapter to map the ODCS contract schema to the internal
    representations used within jestr. It includes fields for configuration parameters,
    validation modes, and evolution modes, along with methods for handling contract logic.
    """

    odcs: OpenDataContractStandard
    contract_path: Path

    @classmethod
    def from_string(
        cls, content: str, source_label: str = "<string>"
    ) -> "ODCSContract":
        """Factory method to create an ODCSContract instance from a string representation of the contract.

        Args:
            content: The string content of the ODCS contract, expected to be in YAML format.
            source_label: Descriptive label for the source of the contract, used for logging and error messages

        Returns:
            An instance of ODCSContract initialised with the parsed ODCS contract.

        Raises:
            ContractLoadError: If the content cannot be parsed into a valid ODCS contract.
        """
        try:
            contract = OpenDataContractStandard.from_string(content)
            return cls(
                odcs=contract,
                contract_path=Path(source_label),
            )
        except yaml.YAMLError as exc:
            location = None
            mark = getattr(exc, "problem_mark", None)
            if mark is not None:
                location = YamlLocation(
                    file_path=source_label,
                    line=mark.line + 1,  # line numbers are 0-indexed in PyYAML
                    column=mark.column + 1,
                )
            raise ContractLoadError(
                source_label,
                f"Failed to parse ODCS contract from {source_label}: {exc}",
                location=location,
            ) from exc
        except Exception as exc:
            raise ContractLoadError(
                source_label,
                f"Unexpected error while loading ODCS contract from {source_label}: {exc}",
            ) from exc

    @classmethod
    def from_file(cls, contract_path: Path) -> "ODCSContract":
        """Factory method to create an ODCSContract instance from a file path.

        Args:
            contract_path: The file path to the ODCS contract, expected to be in YAML format.

        Returns:
            An instance of ODCSContract initialised with the parsed ODCS contract from the file.

        Raises:
            ContractLoadError: If the file cannot be read or parsed into a valid ODCS contract.
        """
        try:
            contract = OpenDataContractStandard.from_file(file_path=str(contract_path))
            return cls(
                odcs=contract,
                contract_path=contract_path,
            )
        except ContractLoadError:
            raise
        except yaml.YAMLError as exc:
            location = None
            mark = getattr(exc, "problem_mark", None)
            if mark is not None:
                location = YamlLocation(
                    file_path=str(contract_path),
                    line=mark.line + 1,  # line numbers are 0-indexed in PyYAML
                    column=mark.column + 1,
                )
            logger.exception(
                "Failed to parse ODCS contract from file ",
                contract_path=str(contract_path),
                error=str(exc),
                exc_info=True,
            )
            raise ContractLoadError(
                str(contract_path),
                f"Failed to parse ODCS contract from file {contract_path}: {exc}",
                location=location,
            ) from exc
        except FileNotFoundError as exc:
            logger.exception(
                f"ODCS contract file not found: {contract_path}",
                contract_path=str(contract_path),
                error=str(exc),
                exc_info=True,
            )
            raise ContractLoadError(
                str(contract_path),
                f"ODCS contract file not found: {contract_path}",
            ) from exc
        except Exception as exc:
            logger.exception(
                "Unexpected error while loading ODCS contract from file ",
                contract_path=str(contract_path),
                error=str(exc),
                exc_info=True,
            )
            raise ContractLoadError(
                str(contract_path),
                f"Unexpected error while loading ODCS contract from file {contract_path}: {exc}",
            ) from exc

    @property
    def id(self) -> str:
        """Returns the unique ID of the ODCS contract, or an empty string if not set."""
        return self.odcs.id or ""

    @property
    def version(self) -> str:
        """Returns the version of the ODCS contract, or a default value if not set."""
        return self.odcs.version or "1.0.0"

    @property
    def status(self) -> str:
        """Returns the status of the ODCS contract, or a default value if not set."""
        return self.odcs.status or "draft"

    def get_server(self, server_name: str | None = None) -> Any:
        """Retrieves a server definition from the ODCS contract by name, or returns the first server if no name is provided.

        Args:
            server_name: The name of the server to retrieve.

        Returns:
            The server definition matching the provided name, or the first server if no name is provided. Returns None if no servers are defined in the contract.
        """
        servers: list[Server] | None = self.odcs.servers
        if not servers:
            return None
        if server_name is None:
            return servers[0]  # Return the first server if no name is provided
        for server_ in servers:
            if server_.server and server_.server.upper() == server_name.upper():
                return server_
        return None

    def get_source_type(self, server_name: str | None = None) -> str | None:
        """Determines the source type for a given server name based on the ODCS contract.

        If the server is file-based, returns the format; otherwise, returns the server type.

        Args:
            server_name: The name of the server to determine the source type for.

        Returns:
            The source type for the specified server, or None if the server is not found or does not have a type defined.
        """
        server_ = self.get_server(server_name)
        if not server_ or not server_.server:
            return None
        raw_type = (server_.type or "").lower()
        if not raw_type:
            return None
        if raw_type in FILE_BASED_SERVER_TYPES:
            return server_.format
        return raw_type

    @property
    def _primary_schema(self) -> SchemaObject | None:
        """Helper property to access the first schema object defined in the ODCS contract, if available."""
        schema = self.odcs.schema_
        return schema[0] if schema else None

    @property
    def resource_name(self) -> str | None:
        """Derive the dlt resource name from the first schema object's ''logicalName''.

        Returns:
            The ''logicalName'' of the first schema object in the contract, which can be used as the dlt resource name. Returns None if there are no schema objects defined or if the logicalName field is not set.
        """
        return self._primary_schema.name if self._primary_schema else None

    @property
    def source_schema_name(self) -> str | None:
        """Derive the source schema name from the first schema object's ''physicalName''.

        Returns:
            The schema name portion of the ''physicalName'' of the first schema object in the contract, which can be used as the source schema name. If the physicalName is in the format "schema.table", it returns "schema". If there is no dot, it returns the entire physicalName. Returns None if there are no schema objects defined or if the physicalName field is not set.
        """
        if self._primary_schema:
            physical_name = self._primary_schema.physicalName or ""
            return (
                physical_name.split(".", 1)[0]
                if "." in physical_name
                else physical_name
            )
        return None

    @property
    def source_table_name(self) -> str | None:
        """Derive the source table name from the first schema object's ''physicalName''.

        Returns:
            The table name portion of the ''physicalName'' of the first schema object in the contract, which can be used as the source table name. If the physicalName is in the format "schema.table", it returns "table". If there is no dot, it returns the entire physicalName. Returns None if there are no schema objects defined or if the physicalName field is not set.
        """
        if self._primary_schema:
            physical_name: str = self._primary_schema.physicalName or ""
            return (
                physical_name.split(".", 1)[1]
                if "." in physical_name
                else physical_name
            )
        return None

    def get_resource_path(self, server_name: str | None = None) -> str | None:
        """Determines the resource path for a given server name based on the ODCS contract.

        Args:
            server_name: The name of the server to determine the resource path for.

        Returns:
            The resource path for the specified server if it is file-based, or None if the server is not found, does not have a type defined, or is not file-based.
        """
        server_ = self.get_server(server_name)
        if server_ is not None:
            server_type = (server_.type or "").lower()
            if server_type == "local":
                return server_.path or None
            elif server_type == "s3":
                return server_.location or None
            else:
                return None
        else:
            return None

    @property
    def custom_properties(self) -> dict[str, Any]:
        """Returns the custom properties defined in the ODCS contract, or an empty dictionary if not set.

        Returns:
            A dictionary of custom properties from the ODCS contract's customProperties field, or an empty dictionary if the field is not set or is not a valid dictionary.
        """
        val = getattr(self.odcs, "customProperties", None)
        return val if isinstance(val, dict) else {}

    @property
    def validation_mode(self) -> ValidationMode:
        """Determines the validation mode for the contract based on the customProperties.validationMode field.

        Returns:
            The ValidationMode specified in the contract's custom properties, or a default of ValidationMode.REJECT if not specified or invalid.
        """
        mode = self.custom_properties.get("validationMode", "reject").lower()
        if mode not in ValidationMode._value2member_map_:
            logger.warning(
                f"Invalid validation mode '{mode}' specified in contract {self.id}, defaulting to 'reject'."
            )
            return ValidationMode.REJECT
        return ValidationMode(mode)

    @property
    def evolution_mode(self) -> EvolutionMode:
        """Determines the evolution mode for the contract based on the customProperties.evolutionMode field.

        Returns:
            The EvolutionMode specified in the contract's custom properties, or a default of EvolutionMode.STRICT if not specified or invalid.
        """
        mode = self.custom_properties.get("evolutionMode", "strict").lower()
        if mode not in EvolutionMode._value2member_map_:
            logger.warning(
                f"Invalid evolution mode '{mode}' specified in contract {self.id}, defaulting to 'strict'."
            )
            return EvolutionMode.STRICT
        return EvolutionMode(mode)

    @property
    def ol_source_namespace(self) -> str | None:
        """Derive OpenLineage namespace from the first ODCS server entry.

        Follows OL naming conventions:
        - S3 servers: ''s3://<netloc>'' derived from ''location'' or ''endpointUrl'' fields, or "s3" if neither is available.
        - Database/other servers: ''<type>://<host>:<port>/<service>''

        Returns:
            The derived OpenLineage namespace based on the first server defined in the ODCS contract, or None if no servers are defined or if the server does not have sufficient information to construct a namespace.
        """
        from urllib.parse import urlparse

        servers = self.odcs.servers
        if not servers:
            return None
        server_ = servers[0]  # Use the first server for OL source namespace
        server_type = (server_.type or "").lower()

        if server_type == "s3":
            if server_.location:
                parsed = urlparse(server_.location)
                if parsed.netloc:
                    return f"s3://{parsed.netloc}"
            if server_.endpointUrl:
                return f"s3://{server_.endpointUrl}"
            return "s3"

        host = server_.host or ""
        if not host:
            return None

        namespace = f"{server_type}://{host}"
        if server_.port:
            namespace += f":{server_.port}"

        service = server_.serviceName or server_.database or server_.schema_ or ""
        if service:
            namespace += f"/{service}"

        return namespace

    @property
    def ol_source_dataset_name(self) -> str | None:
        """Derive OpenLineage dataset name from the first schema object's ''physicalName''.

        Returns:
            The derived OpenLineage dataset name based on the ''physicalName'' of the first schema object in the ODCS contract, or None if there are no schema objects defined or if the physicalName field is not set.
        """
        return self._primary_schema.physicalName if self._primary_schema else None

    def to_dlt_schemas(self) -> dict[str, TTableSchema]:
        """Convert the ODCS contract's schema objects into a dictionary of dlt table schemas.

        Returns:
            A dictionary mapping table names to their corresponding dlt table schemas, constructed from the ODCS contract's schema objects. If no schema objects are defined in the contract, returns an empty dictionary.
        """
        dlt_schemas: dict[str, TTableSchema] = {}
        odcs_schema = self.odcs.schema_ or []

        if not odcs_schema:
            logger.warning(f"No schema objects defined in contract {self.id}.")
            return dlt_schemas

        for table in odcs_schema:
            if not table.name or not table.properties:
                continue

            columns: dict[str, TColumnSchema] = {}
            for prop in table.properties:
                if prop.name:
                    columns[prop.name] = self._build_column_schema(prop)

            table_schema: TTableSchema = {
                "name": table.name,
                "columns": columns,
            }

            desc_str = self._get_description_str(table.description)
            if desc_str:
                table_schema["description"] = desc_str

            # Extract table-level references (foreign keys) if defined in the ODCS schema
            if hasattr(table, "references") and table.references:
                refs = table.references if isinstance(table.references, list) else []
                processed_refs: list[dict[str, Any]] = []

                for ref in refs:
                    # Use duck typing to process references, accepting any dict-like object
                    try:
                        ref_dict = cast("dict[str, Any]", ref)
                        processed_ref = {
                            "columns": self._process_reference_field(
                                ref_dict, "columns", []
                            ),
                            "referenced_table": ref_dict.get("referencedTable"),
                            "referenced_columns": self._process_reference_field(
                                ref_dict, "referencedColumns", []
                            ),
                        }
                        processed_refs.append(processed_ref)
                    except (AttributeError, TypeError):
                        # if ref is not a dict-like object, skip it
                        continue

                table_schema["references"] = cast(
                    list[TTableReferenceInline], processed_refs
                )

            dlt_schemas[table.name] = table_schema

        return dlt_schemas

    def _build_column_schema(self, prop: SchemaProperty) -> TColumnSchema:
        """Build a dlt column schema from an ODCS SchemaProperty.

        Args:
            prop: The ODCS SchemaProperty to convert into a dlt column schema.

        Returns:
            A dlt column schema dictionary constructed from the provided ODCS SchemaProperty, including data type mapping, nullability, and any relevant hints or metadata extracted from the ODCS property.
        """
        dlt_type, type_extras = self.get_dlt_type_from_odcs_prop(prop)

        col = new_column(
            column_name=prop.name or "",
            data_type=cast(TDataType, dlt_type),
            nullable=not prop.required,
        )
        if (precision := type_extras.get("precision")) is not None:
            col["precision"] = precision
        if (scale := type_extras.get("scale")) is not None:
            col["scale"] = scale

        # Add dlt column hints
        if prop.primaryKey:
            col["primary_key"] = True
        if prop.unique:
            col["unique"] = True

        # Add description if available
        desc_str = self._get_description_str(prop.description)
        if desc_str:
            col["description"] = desc_str

        return self._add_odcs_hints(col, prop)

    def _add_odcs_hints(
        self, column: TColumnSchema, prop: SchemaProperty
    ) -> TColumnSchema:
        """Add any relevant hints from the ODCS property to the dlt column schema.

        This includes logical and physical type hints, PII tags, classification, and quality rules.

        Args:
            column: The dlt column schema to which hints should be added.
            prop: The ODCS SchemaProperty containing potential hints to be added to the column schema.

        Returns:
            The updated dlt column schema with any applicable ODCS hints added as metadata.
        """
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
                col_dict["x-odcs-logical-min-length"] = min_length
            if (max_length := prop.logicalTypeOptions.get("maxLength")) is not None:
                col_dict["x-odcs-logical-max-length"] = max_length

        if prop.quality:
            col_dict["x-odcs-quality-rules"] = prop.quality

        logger.debug(
            "ODCS columns hints added.",
            column_name=prop.name,
            hints={k: v for k, v in col_dict.items() if k.startswith("x-odcs-")},
        )

        return column

    def get_dlt_columns(self, table_name: str) -> dict[str, TColumnSchema]:
        """Extract column schemas for a given table from the ODCS contract.

        Args:
            table_name: The name of the table to extract column schemas for.

        Returns:
            A dictionary mapping column names to their corresponding dlt column schemas for the specified table in the ODCS contract. If the table is not defined in the contract, returns an empty dictionary.
        """
        dlt_schemas = self.to_dlt_schemas()
        logger.debug(
            "dlt schemas retrieved.",
            num_tables=len(dlt_schemas),
        )
        return dlt_schemas.get(table_name, {}).get("columns", {})

    def get_primary_keys(self, table_name: str) -> list[str]:
        """Extract primary key column names for a given table from the ODCS contract.

        Args:
            table_name: The name of the table to extract primary key columns for.

        Returns:
            A list of column names that are designated as primary keys for the specified table in the ODCS contract. If the table is not defined in the contract or has no primary key columns, returns an empty list.
        """
        columns = self.get_dlt_columns(table_name)
        return [
            col_name
            for col_name, col_schema in columns.items()
            if col_schema.get("primary_key")
        ]

    @staticmethod
    def _get_description_str(description: str | Description | None) -> str | None:
        """Extract a plain string description from an ODCS Description field.

        Args:
            description: The ODCS description field, which can be a string, a Description object, or None.

        Returns:
            A plain string extracted from the description, with whitespace normalised, or None if the description is not provided or cannot be interpreted as a string.
        """
        if description is None:
            return None

        # Try duck typing Description-like object with purpose attribute first
        try:
            if hasattr(description, "purpose") and description.purpose:
                return str(description.purpose).strip().replace("\n", " ")
        except AttributeError:
            pass  # Purpose exists but it not str-like

        # Fall back to using as a plain string
        if isinstance(description, str):
            desc_str = description.strip().replace("\n", " ")
            if desc_str:
                return desc_str

        return None

    @staticmethod
    def _ensure_list(value: Any) -> list[Any]:
        """Ensure that the provided value is returned as a list."""
        return (
            value if isinstance(value, list) else [value] if value is not None else []
        )

    @staticmethod
    def _process_reference_field(
        ref: dict[str, Any], field_name: str, default: Any
    ) -> list[Any]:
        """Extract and normalise a reference field from the ODCS schema.

         This method handles the case where the field can be either a single value or a list of values, ensuring that the result is always a list.

        Args:
            ref: The reference dictionary containing the field to extract.
            field_name: The name of the field to extract from the reference.
            default: The default value to use if the field is not present in the reference.

        Returns:
            A list of values extracted from the specified field in the reference, or a list containing the default value if the field is not present or cannot be accessed.
        """
        try:
            value = ref.get(field_name, default)
            return ODCSContract._ensure_list(value)
        except (AttributeError, TypeError):
            return ODCSContract._ensure_list(default)

    def get_dlt_type_from_odcs_prop(
        self, prop: SchemaProperty
    ) -> tuple[str, dict[str, int | None]]:
        """Map an ODCS SchemaProperty to a dlt data type, including handling of logical types and any relevant type options.

        Args:
            prop: The ODCS SchemaProperty to map to a dlt data type.

        Returns:
            A tuple containing the dlt data type as a string and a dictionary of any relevant type options (such as precision and scale) extracted from the ODCS property. The data type is determined based on the logical type if available, falling back to the physical type, and then mapped to a dlt-compatible type using the LOGICAL_TO_PY_TYPE_MAP.
        """
        logical_type = self._get_type(prop) or "string"
        py_type = LOGICAL_TO_PY_TYPE_MAP.get(logical_type, str)

        dlt_type = str(
            py_type_to_sc_type(py_type) if isinstance(py_type, type) else py_type
        )

        extras: dict[str, int | Any] = {}
        precision = self._get_logical_type_option(prop, "precision")
        if precision is not None:
            extras["precision"] = precision

        scale = self._get_logical_type_option(prop, "scale")
        if scale is not None:
            extras["scale"] = scale

        return dlt_type, extras

    @staticmethod
    def _get_logical_type_option(prop: SchemaProperty, key: str) -> Any:
        """Helper method to safely extract a logical type option from an ODCS SchemaProperty."""
        return prop.logicalTypeOptions.get(key) if prop.logicalTypeOptions else None

    @staticmethod
    def _get_type(prop: SchemaProperty) -> str | None:
        """Helper method to determine the logical type of an ODCS SchemaProperty, falling back to physical type if logical type is not set."""
        return prop.logicalType or prop.physicalType

    def to_dlt_schema_contract(self) -> TSchemaContract:
        """Convert the ODCS contract's schema and evolution settings into a dlt schema contract format.

        This method maps the ODCS contract's schema objects to dlt table schemas and determines the appropriate evolution settings based on the contract's status and specified evolution mode.

        Returns:
            A dlt schema contract dictionary containing the mapped table schemas and evolution settings derived from the ODCS contract.
        """
        # If contract is deprecated/retired, freeze everything
        if self.status.lower() in {"deprecated", "retired"}:
            return {
                "tables": "freeze",
                "columns": "freeze",
                "data_type": "freeze",
            }

        mode_mapping: dict[EvolutionMode.value, TSchemaContract] = {
            "strict": {"tables": "freeze", "columns": "freeze", "data_type": "freeze"},
            "notify": {"tables": "evolve", "columns": "evolve", "data_type": "evolve"},
            "auto_evolve": {
                "tables": "evolve",
                "columns": "evolve",
                "data_type": "evolve",
            },
        }

        return mode_mapping[self.evolution_mode]
