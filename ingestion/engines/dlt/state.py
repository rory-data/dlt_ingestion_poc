"""dlt-specific state management for pipeline validation metrics."""

from typing import Any

import dlt
from loguru import logger

from ingestion.core.reporting.validation import ValidationSummary


def update_validation_state(
    issues: list,
    total_rows: int,
    bad_rows: int,
    expected_record_counts: dict[str, int] | None = None,
) -> None:
    """Update dlt pipeline state with validation metrics.

    This function should be called from within dlt decorated functions
    to persist validation data across batches.

    Args:
        issues: List of DataQualityIssue objects from validation.
        total_rows: Total rows processed in this batch.
        bad_rows: Number of rows that failed validation.
        expected_record_counts: Optional dict of expected record counts from trailers.
    """
    state = dlt.current.state()
    summary_dict = state.setdefault("validation_summary", {})
    summary = ValidationSummary.from_dict(summary_dict)

    # Update counts
    summary.update(issues, total_rows, bad_rows)

    # Update expected record counts if provided
    if expected_record_counts:
        summary.set_expected_counts(expected_record_counts)

    state["validation_summary"] = summary.to_dict()


def add_record_counts(counts: dict[str, int]) -> None:
    """Add record type counts to the validation state.

    Args:
        counts: Dictionary mapping record type to count.
    """
    state = dlt.current.state()
    summary_dict = state.setdefault("validation_summary", {})
    summary = ValidationSummary.from_dict(summary_dict)
    summary.add_record_counts(counts)
    state["validation_summary"] = summary.to_dict()


def get_validation_summary(pipeline: Any) -> dict[str, Any]:
    """Extract validation summary from a completed pipeline.

    Note: This function must be called outside of dlt decorated functions.

    Args:
        pipeline: Completed dlt.Pipeline instance.

    Returns:
        Dictionary containing validation summary, or empty dict if unavailable.
    """
    try:
        # Check source state first (preferred for dlt 1.x)
        for source_state in pipeline.state.get("sources", {}).values():
            if "validation_summary" in source_state:
                return source_state["validation_summary"]

        # Fallback to general pipeline state
        return pipeline.state.get("validation_summary", {})
    except Exception as e:
        logger.warning(f"Could not retrieve validation summary: {e}")
        return {}
