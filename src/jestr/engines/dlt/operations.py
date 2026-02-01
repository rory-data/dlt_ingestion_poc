"""Pipeline initialization and execution utilities for dlt ingestion scripts."""

import sys
from collections.abc import Callable
from typing import Any

import structlog

from jestr.validators import get_validation_summary, log_validation_summary

logger = structlog.get_logger()


def run_pipeline_with_summary(
    pipeline: Any,
    source_callable: Callable,
    pipeline_description: str = "pipeline",
) -> None:
    """Run dlt pipeline and display validation summary."""
    try:
        logger.info("Starting data load...")
        load_info = pipeline.run(source_callable)
        logger.info("Pipeline completed successfully")
        logger.info(load_info)

        # Extract and display validation summary
        validation_summary = get_validation_summary(pipeline)
        log_validation_summary(validation_summary)

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        sys.exit(1)
