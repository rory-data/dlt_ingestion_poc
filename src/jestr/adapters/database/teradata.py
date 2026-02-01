"""Teradata database adapter implementation."""

from typing import Any

import structlog
from pydantic import BaseModel, Field

from ..base import BaseAdapter

logger = structlog.get_logger()


# CONFIGURATION MODEL
class TeradataConfig(BaseModel):
    """Configuration for TeradataAdapter."""

    batch_size: int = Field(
        default=50_000,
        description="Number of rows to process in each batch during data operations.",
    )

    cleanup_interval: int = Field(
        default=10,
        description="Trigger garbage collection every N batches (0 = disabled).",
        ge=0,
    )

    write_statistics: bool = Field(
        default=True,
        description="Write column statistics for query optimisation.",
    )

    row_group_size: int | None = Field(
        default=None,
        description="Number of rows per row group in Parquet files (None = use engine defaults).",
    )

    compression: str = "snappy"

    decimal_precision: int = Field(
        default=38,
        description="Precision for decimal types when writing to Parquet files.",
    )

    decimal_scale: int = Field(
        default=10,
        description="Scale for decimal types when writing to Parquet files.",
    )


# MAIN ADAPTER
class TeradataAdapter(BaseAdapter):
    """Teradata database adapter."""

    config_model = TeradataConfig

    def __init__(
        self,
        connection_uri: str | None = None,
        connection_factory: Any | None = None,
        config: TeradataConfig | None = None,
    ) -> None:
        super().__init__(
            connection_uri=connection_uri,
            connection_factory=connection_factory,
            pool_name="TeradataAdapter",
        )
        self.config = config or TeradataConfig()
        logger.info("TeradataAdapter initialised with connection pool manager")
