"""Customer exception hierarchy for jestr.

This module defines a structure exception hierarchy for jestr, enabling precise error handling
and improved observability.

Exception Hierarchy:
    JestrError (base class for all jestr exceptions)
    ├⎯ AdapterError
    ├⎯ ConfigurationError
    ├⎯ ValidationError
    ├⎯ PipelineError
    ├⎯ IngestionError
    ├⎯ StorageError
    ├⎯ ContractLoadError
    ⏐   └⎯ SchemaEvolutionError
    └⎯ RetryableError
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from jestr.validators.core.issues import DataQualityIssue


@dataclass(frozen=True)
class YamlLocation:
    """Represents the location of an issue in a YAML file for error reporting."""

    file_path: str | None = None
    line: int | None = None
    column: int | None = None

    def __str__(self) -> str:
        """Format as 'path[line,col]', 'path', '[line,col', or ''."""
        pos = ""
        if self.line is not None and self.column is not None:
            pos = f"[{self.line},{self.column}]"
        elif self.line is not None:
            pos = f"[{self.line}]"

        if self.file_path and pos:
            return f"{self.file_path}{pos}"
        if self.file_path:
            return self.file_path
        if pos:
            return pos
        return ""


class JestrError(Exception):
    """Base exception for all jestr errors.

    All custom jestr exceptions inherit from this base class, enabling catch-all exception
    handling when needed.
    """


class AdapterError(JestrError):
    """Errors related to adapter operations.

    Raised when service connectivity, query execution, or schema reflection fails.

    Examples:
        - Oracle connection failed due to network timeout
        - Query execution execceded memory limit
    """


class ValidationError(JestrError):
    """Errors that occur during validation of data against contracts.

    Attributes:
        resource_name: The name of the resource that failed validation.
        issues: A list of data quality issues identified during validation.
        message: An optional custom error message. If not provided, a default message summarising the issues will be used.

    Examples:
        - NULL values in non-nullable column
        - String values exceeding defined length limits
        - Datatype mismatches (e.g. string in numeric column)
    """

    def __init__(
        self,
        resource_name: str,
        issues: list["DataQualityIssue"],
        message: str | None = None,
    ) -> None:
        """Initialise the ValidationError with the resource name and list of issues.

        Args:
            resource_name: The name of the resource that failed validation.
            issues: A list of data quality issues identified during validation.
            message: An optional custom error message (auto-generated if not provided).
        """
        self.resource_name = resource_name
        self.issues = issues

        if message is None:
            message = (
                f"Validation failed for resource '{resource_name}': "
                f"{len(issues)} issue(s) detected. "
            )

        super().__init__(message)


class ContractLoadError(JestrError):
    """Errors that occur while loading or parsing contract definitions.

    Raised when loading or parsing ODCS contracts fails. This includes YAML parsing errors,
    missing required fields, or file I/O issues

    Attributes:
        - contract_path: The file path of the contract that failed to load (if applicable).
        - reason: A detailed reason for the failure.
        - location: Optional YAML Location where the error occurred.

    Examples:
        - YAML syntax error in contract file
        - Missing required 'resources' section in contract
        - Contract file not found at specified path
    """

    def __init__(
        self,
        contract_path: str,
        reason: str,
        location: "YamlLocation | None" = None,
    ) -> None:
        """Initialise the ContractLoadError with details about the failure.

        Args:
            contract_path: The file path of the contract that failed to load (if applicable).
            reason: A detailed reason for the failure.
            location: Optional YAML Location where the error occurred.
        """
        self.contract_path = contract_path
        self.reason = reason
        self.location = location

        if location:
            message = f"Failed to load contract from '{contract_path}': {reason} at {location}"
        else:
            message = f"Failed to load contract from '{contract_path}': {reason}"
        super().__init__(message)


class ConfigurationError(JestrError):
    """Errors related to configuration issues.

    Raised when there are problems with the configuration of jestr, such as missing required
    settings, invalid values, or conflicts between settings.

    Examples:
        - Missing required 'adapter' configuration
        - Invalid value for 'batch_size' (e.g. negative number)
        - Conflicting settings (e.g. both 'vault' and 'hardcoded' credentials provided)
    """


class PipelineError(JestrError):
    """Errors that occur during pipeline execution.

    Raised when there are issues during the execution of a jestr pipeline, such as failures in
    data transformations, resource processing, or unexpected exceptions.

    Examples:
        - Data transformation failed due to unexpected value
        - Resource processing failed due to missing required field
        - Unhandled exception during pipeline execution
    """


class IngestionError(JestrError):
    """Errors that occur during data ingestion.

    Raised when there are issues during the ingestion of data into the target system, such as
    connectivity issues, data format problems, or failures in writing to the target.

    Examples:
        - Failed to connect to target database
        - Data format not supported for ingestion
        - Write operation failed due to constraint violation
    """


class StorageError(JestrError):
    """Errors related to storage operations.

    Raised when there are issues with storage operations, such as reading/writing from/to
    storage systems, file I/O errors, or issues with temporary storage during processing.

    Examples:
        - Failed to read from temporary storage location
        - File I/O error when writing to local disk
        - Storage system connectivity issue during read/write operation
    """


class SchemaEvolutionError(JestrError):
    """Errors that occur due to schema evolutipon beyond contract expectations.

    Raised when there are issues related to schema evolution, such as new columns added without
    updating the contract, or changes in data types that violate contract definitions.

    Examples:
        - New column added to source table without corresponding contract update
        - Data type change in source that violates contract definition
        - Unexpected nullability change in source schema
    """


class RetryableError(JestrError):
    """Errors that are considered transient and may succeed if retried.

    Raised for errors that are likely to be resolved on a subsequent attempt, such as temporary
    network issues, timeouts, or transient service unavailability.

    Examples:
        - Network timeout when connecting to source system
        - Temporary unavailability of target database
        - Transient error from external API during enrichment step
    """
