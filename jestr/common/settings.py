"""Centralised application settings for consolidsting pipeline execution configuration."""

from pydantic_settings import BaseSettings, SettingsConfigDict

from jestr.common.constants import BatchDefaults


class ApplicationSettings(BaseSettings):
    """Application settings for consolidating pipeline execution configuration."""

    model_config = SettingsConfigDict(
        env_prefix="JESTR_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ===== Pipeline Execution =====
    arrow_batch_size: int = BatchDefaults.SIZE
    """Rows per Arrow RecordBatch for streaming ingestion.

    Controls the size of Arrow batches yielded to consumers. Larger batches reduce overhead
    but increase memory usage. Adjust based on workload and resource constraints.

    Environment variable: JESTR_ARROW_BATCH_SIZE
    """

    db_fetch_size = BatchDefaults.DB_FETCH_SIZE
    """Database curose fectchmany() row count (server-side cursor size)

    Controls the number of rows fetched from the database per round trip when using
    server-side cursors. Larger values can improve performance by reducing round trips,
    but may increase memory usage.

    Environment variable: JESTR_DB_FETCH_SIZE
    """

    network_prefetch_size: int = BatchDefaults.NETWORK_PREFETCH_SIZE

    run_more: str = "extract"

    sample_rows: int | None = None

    cleanup_interval_seconds: int = 86400

    # ===== Logging and Telemetry =====
    log_level: str = "INFO"

    json_logs: bool = False

    correlation_id: str | None = None

    # ===== dlt Storage Configuration =====
    destination_path: str | None = None

    storage_backend: str = "filesystem"

    # ===== Validation and Contract Handling =====
    validation_mode: str = "reject"

    # ===== OpenLineage Configuration =====
    openlineage_url: str | None = None

    openlineage_api_key: str | None = None

    openlineage_namespace: str = "jestr"
