"""Pipeline initialization and execution utilities for dlt ingestion scripts."""

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import dlt
from loguru import logger

from ingestion.core.reporting.state import (
    get_validation_summary,
    log_validation_summary,
)
from ingestion.core.validate.validator import ArrowValidator
from ingestion.schema.odcs import get_dlt_schemas


def validate_source_file(file_path: Path | str) -> Path:
    """Validate that source file exists and return Path object.

    Args:
        file_path: Path to the source file.

    Returns:
        Path object if file exists.

    Raises:
        SystemExit: If file does not exist.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        logger.error(f"Source file not found: {file_path}")
        sys.exit(1)
    return file_path


def load_odcs_schemas(contract_path: Path | str) -> dict:
    """Load ODCS contract and generate dlt schemas.

    Args:
        contract_path: Path to the ODCS contract YAML file.

    Returns:
        Dictionary of dlt table schemas.

    Raises:
        SystemExit: If contract loading fails.
    """
    logger.info(f"Loading ODCS contract from {contract_path}")
    try:
        schemas = get_dlt_schemas(str(contract_path))
        logger.info(f"Loaded schemas for {len(schemas)} tables: {list(schemas.keys())}")
        return schemas
    except Exception as e:
        logger.error(f"Failed to load ODCS contract: {e}", exc_info=True)
        sys.exit(1)


def setup_validator(
    issue_action: Literal["reject", "quarantine"] = "quarantine",
) -> ArrowValidator:
    """Initialize and configure data quality validator.

    Args:
        issue_action: Action for bad records ("quarantine" or "reject").

    Returns:
        Configured ArrowValidator instance.
    """
    validator = ArrowValidator(issue_action=issue_action)
    logger.info(f"Validator configured with action: {validator.issue_action}")
    return validator


def run_pipeline_with_summary(
    pipeline: Any,
    source_callable: Callable,
    pipeline_description: str = "pipeline",
) -> None:
    """Run dlt pipeline and display validation summary.

    Handles pipeline execution, logs results, and displays validation summary.

    Args:
        pipeline: The dlt pipeline instance to run.
        source_callable: The dlt source or resource callable to load.
        pipeline_description: Description of the pipeline for logging.

    Raises:
        SystemExit: If pipeline execution fails.
    """
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
