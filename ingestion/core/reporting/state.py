"""Validation summary reporting utilities."""

from typing import Any

from loguru import logger

from .validation import ValidationSummary


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
