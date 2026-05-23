"""Idiomatic Python enums and constants for jestr pipelines."""

from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

UNICODE_REPLACEMENT_CHAR: str = "\ufffd"
FILE_BASED_SERVER_TYPES: frozenset[str] = frozenset({"local", "s3"})


class AdapterType(StrEnum):
    """Enum for supported adapter types.

    Each member represents a data service that jestr can interact with. Members are usable
    as strings, compatible with URI schemes and configuration strings.
    """

    ORACLE = "oracle"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    TERADATA = "teradata"


class SourceType(StrEnum):
    """Enum for supported pipeline source types.

    Broader than AdapterType, includes both data services and file-based sources that can
    be ingested by the pipeline.
    """

    CSV = "csv"
    ORACLE = "oracle"
    TERADATA = "teradata"


class RunMode(StrEnum):
    """Enum for supported pipeline execution modes.

    Governs whether the pipeline extracts fresh data from the source or replays previously
    cached data.
    """

    EXTRACT = "extract"
    """Default mode: extract fresh data from the source and process it through the pipeline."""

    REPLAY = "replay"
    """Replay mode: skip extraction and reprocess data from the most recent successful batch, using cached data if available."""


class ValidationMode(StrEnum):
    """Enum for data validation enforcement modes.

    Determines the behaviour when data quality constraints are violated during validation
    checks. Set via the ODCS contract's customProperties.validationMode field.
    """

    REJECT = "reject"
    """Fail the batch and halt processing on validation failures."""

    QUARANTINE = "quarantine"
    """Route invalid records to a separate quarantine table for later review, while allowing valid records to be processed."""


class EvolutionMode(StrEnum):
    """Enum for schema evolution handling modes.

    Determines how the pipeline handles changes in the data schema over time, such as new columns being added.
    Set via the ODCS contract's customProperties.evolutionMode field.
    """

    STRICT = "strict"
    """Fail the batch and halt processing if the incoming data schema does not exactly match the expected schema defined in the contract."""

    NOTIFY = "notify"
    """Allow the batch to proceed but log a warning if the incoming data schema has differences (e.g. new columns) compared to the expected schema, indicating potential evolution that should be reviewed."""

    AUTO_EVOLVE = "auto_evolve"
    """Automatically adapt to schema changes by allowing new columns to be added to the expected schema, while still enforcing the presence and types of existing columns. This mode enables seamless handling of schema evolution without manual intervention, while maintaining data quality checks on existing schema elements."""


class CredentialSource(StrEnum):
    """Enum of credential sources for authentication.

    Defines priority order for resolving credentials from multiple potential sources during
    pipeline initialisation.
    """

    ENV = "env"
    """Environment variables: credentials are sourced from environment variables."""

    AIRFLOW_URI = "airflow_uri"
    """Airflow connection URI: credentials are sourced from Airflow connection URIs."""

    VAULT = "vault"
    """HashiCorp Vault: credentials are sourced from HashiCorp Vault secrets."""

    CODE = "code"
    """Hardcoded: credentials are directly provided in the code or configuration (not recommended for production)."""


class ColumnSuffixes:
    """Standard column name suffixes for applying during transformations."""

    CLEANED = "_cleaned"
    """Suffix for columns that have been cleaned (e.g. replacement character removal)."""

    VALIDATED = "_validated"
    """Suffix for columns that have been validated against a contract."""

    ISSUES = "_issues"
    """Suffix for columns that contain data quality issues identified during validation."""


@dataclass(frozen=True)
class BatchDefaults:
    """Default batch sizes and limits for various operations to balance performance and resource usage."""

    SIZE: ClassVar[int] = 50_000
    MIN_SIZE: ClassVar[int] = 5_000
    MAX_SIZE: ClassVar[int] = 1_000_000

    DB_FETCH_SIZE: ClassVar[int] = 10_000
    MIN_DB_FETCH_SIZE: ClassVar[int] = 100
    MAX_DB_FETCH_SIZE: ClassVar[int] = 100_000

    NETWORK_PREFETCH_SIZE: ClassVar[int] = 100_000
    MIN_NETWORK_PREFETCH_SIZE: ClassVar[int] = 1_000
    MAX_NETWORK_PREFETCH_SIZE: ClassVar[int] = 1_000_000

    CLEANUP_INTERVAL: ClassVar[int] = 10
    SAMPLE_VALUES: ClassVar[int] = 10
    MAX_FAILED_ROW_SAMPLES: ClassVar[int] = 100


@dataclass(frozen=True)
class ArrowDefaults:
    """Default settings for PyArrow to optimise performance and memory usage."""

    MAX_MEMORY_BYTES: ClassVar[int] = 2 * 1024**3


@dataclass(frozen=True)
class DecimalDefaults:
    """Default precision and scale for decimal types to ensure consistency across adapters."""

    PRECISION: ClassVar[int] = 38
    SCALE: ClassVar[int] = 10


@dataclass(frozen=True)
class RetryDefaults:
    """Default settings for retrying failed operations to balance resilience and performance."""

    MAX_ATTEMPTS: ClassVar[int] = 3
    BACKOFF_MULTIPLIER: ClassVar[float] = 10.0
    INITIAL_WAIT_SECONDS: ClassVar[float] = 2.0
    MAX_WAIT_SECONDS: ClassVar[float] = 10.0
