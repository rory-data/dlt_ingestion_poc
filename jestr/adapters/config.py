"""Local configuration models for adapters."""

from pydantic import BaseModel, Field

from jestr.common.constants import BatchDefaults, DecimalDefaults, RetryDefaults


class RetryConfig(BaseModel):
    """Configuration for retry logic with exponential backoff."""

    max_attempts: int = Field(
        default=RetryDefaults.MAX_ATTEMPTS,
        description="Maximum number of retry attempts for transient failures.",
        ge=1,
    )
    backoff_multiplier: float = Field(
        default=RetryDefaults.BACKOFF_MULTIPLIER,
        description="Exponential backoff multiplier applied between retries.",
        gt=0.1,
    )
    initial_wait_seconds: float = Field(
        default=RetryDefaults.INITIAL_WAIT_SECONDS,
        description="Initial wait time in seconds before the first retry attempt.",
        ge=0.1,
    )
    max_wait_seconds: float = Field(
        default=RetryDefaults.MAX_WAIT_SECONDS,
        description="Maximum wait time in seconds between retry attempts.",
        ge=0.1,
    )


class BaseAdapterConfig(BaseModel):
    """Base configuration for all adapters to extend."""

    arrow_batch_size: int = Field(
        default=BatchDefaults.SIZE,
        description="Number of rows per Arrow RecordBatch for streaming.",
    )

    db_fetch_size: int = Field(
        default=BatchDefaults.DB_FETCH_SIZE,
        description="Default cursor fetchmany() size (rows per round-trip).",
    )

    network_prefetch_size: int = Field(
        default=BatchDefaults.NETWORK_PREFETCH_SIZE,
        description="Driver-level networks prefetch batch size (rows).",
    )

    cleanup_interval: int = Field(
        default=BatchDefaults.CLEANUP_INTERVAL,
        description="Trigger garbage collection every N batches (0 = disabled).",
    )

    write_statistics: bool = Field(
        default=True,
        description="Whether to write detailed statistics about the adapter operations.",
    )

    retry_config: RetryConfig = Field(
        default_factory=RetryConfig,
        description="Configuration for retrying failed operations with exponential backoff.",
    )

    parquet_row_group_size: int = Field(
        default=0,
        description="Number of rows per row group when writing Parquet files (0 = use engine defaults).",
    )

    parquet_compression: str = Field(
        default="snappy",
        description="Compression algorithm to use when writing Parquet files (e.g. 'snappy', 'gzip', 'zstd', 'none').",
    )

    parquet_decimal_precision: int = Field(
        default=DecimalDefaults.PRECISION,
        description="Default precision for decimal types when writing Parquet files.",
    )

    parquet_decimal_scale: int = Field(
        default=DecimalDefaults.SCALE,
        description="Default scale for decimal types when writing Parquet files.",
    )
