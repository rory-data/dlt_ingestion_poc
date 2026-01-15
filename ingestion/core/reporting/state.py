"""Validation state management utilities for dlt pipelines."""

from typing import Any

import dlt
from loguru import logger

from .validation import ValidationSummary


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
        logger.warning("Could not retrieve validation summary: {e}", e=e)
        return {}


def log_validation_summary(
    summary_dict: dict[str, Any],
    table_name: str | None = None,
) -> None:
    """Log a formatted validation summary.

    Args:
        summary_dict: Dictionary containing validation summary data.
        table_name: Optional table name to include in output.
    """
    if not summary_dict:
        logger.info("No validation summary available")
        return

    summary = ValidationSummary.from_dict(summary_dict)

    logger.info("=" * 20)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 20)

    if table_name:
        logger.info("Table: {table}", table=table_name)

    logger.info("Total rows processed: {rows:,}", rows=summary.total_rows)
    logger.info("Total bad rows: {bad_rows:,}", bad_rows=summary.total_bad_rows)

    if summary.total_rows > 0:
        bad_pct = (summary.total_bad_rows / summary.total_rows) * 100
        logger.info("Bad row percentage: {pct:.2f}%", pct=bad_pct)

    if summary.record_counts:
        logger.info("\nRecord Type Counts:")
        for rt, count in sorted(summary.record_counts.items()):
            expected = summary.expected_record_counts.get(rt)
            if expected is not None:
                match = "✓" if count == expected else "✗"
                logger.info(
                    "  {rt}: {count:,} (expected: {expected:,}) {match}",
                    rt=rt,
                    count=count,
                    expected=expected,
                    match=match,
                )
            else:
                logger.info("  {rt}: {count:,}", rt=rt, count=count)

    if summary.issue_counts:
        logger.info("\nData Quality Issues:")
        # Sort issues by severity then count
        for key, count in sorted(
            summary.issue_counts.items(), key=lambda x: x[1], reverse=True
        ):
            issue_type, col, sev = key.split("|")
            samples = summary.samples.get(key, [])
            sample_str = f" (samples: {samples[:3]})" if samples else ""
            logger.info(
                "  [{sev}] {issue_type} in {col}: {count:,} rows{samples}",
                sev=sev,
                issue_type=issue_type,
                col=col,
                count=count,
                samples=sample_str,
            )

    logger.info("=" * 20)
