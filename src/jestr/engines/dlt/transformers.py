"""dlt transformers for data validation and bifurcation into clean and bad datasets."""

from collections.abc import Callable, Iterator
from typing import Any

import dlt
import pyarrow as pa
import pyarrow.compute as pc
import structlog

from jestr.validators import IngestionValidator

logger = structlog.get_logger()


def _extract_failed_row_details(
    batch: pa.RecordBatch, bad_mask: pa.Array, issues: list
) -> list[dict]:
    """Extract details of rows that failed validation.

    Returns:
        List of dicts containing row index, failed columns, and check details
    """
    bad_batch = batch.filter(bad_mask)
    failed_rows = []

    if bad_batch.num_rows == 0:
        return failed_rows

    # Get the indices of bad rows in the original batch
    bad_indices = pc.indices_nonzero(bad_mask).to_pylist()

    # Group issues by column and type for lookup
    issues_by_column = {}
    for issue in issues:
        col = issue.column
        if col not in issues_by_column:
            issues_by_column[col] = []
        issues_by_column[col].append(
            {
                "type": issue.issue_type,
                "severity": issue.severity.value,
                "count": issue.issue_count,
            }
        )

    # Extract failed row information
    for idx, row_num in enumerate(bad_indices):
        row_data = {
            "row_index": row_num,
            "failed_checks": [],
        }

        # Identify which columns failed for this row
        for col_name, col_issues in issues_by_column.items():
            col_idx = batch.schema.get_field_index(col_name)
            if col_idx >= 0:
                value = bad_batch.column(col_name)[idx].as_py()
                row_data["failed_checks"].append(
                    {
                        "column": col_name,
                        "value": str(value)[:100] if value is not None else None,
                        "checks": col_issues,
                    }
                )

        failed_rows.append(row_data)

    return failed_rows[:10]  # Limit to first 10 failed rows


def _create_bad_mask(batch: pa.RecordBatch, bad_data: pa.RecordBatch) -> list[bool]:
    """Create boolean mask identifying which rows in batch are in bad_data.

    Args:
        batch: Original batch
        bad_data: Bad records from validation

    Returns:
        List of booleans, True where row is in bad_data
    """
    if bad_data.num_rows == 0:
        return [False] * batch.num_rows

    bad_mask = []
    for i in range(batch.num_rows):
        is_bad = any(
            all(
                batch.column(k)[i].as_py() == bad_data.column(k)[j].as_py()
                for k in range(batch.num_columns)
            )
            for j in range(bad_data.num_rows)
        )
        bad_mask.append(is_bad)
    return bad_mask


def create_validation_transformers(
    extract_resource: Any,
    resource_name: str,
    validator: IngestionValidator,
    batch_date: str = "",
) -> tuple[Callable, Callable, Callable, Callable]:
    """Factory method for validation and bifurcation transformers with resource state tracking.

    Returns:
        Tuple of (validate_data, extract_clean_data, extract_bad_data, validation_report)
    """

    @dlt.transformer(
        name=f"validate__{resource_name}", data_from=extract_resource, selected=False
    )
    def validate_data(batch: pa.RecordBatch) -> Iterator[dict]:
        """Validate data in batches and yield clean/bad as dict with state tracking."""
        clean_data, bad_data, issues = validator.validate(batch)

        logger.info(
            "Validation completed for %s. Clean rows: %d, Bad rows: %d, Issues found: %d",
            resource_name,
            clean_data.num_rows,
            bad_data.num_rows,
            len(issues),
        )

        # Use RESOURCE state for per-resource isolation (not source state)
        resource_state = dlt.current.resource_state()

        # Initialize metrics accumulator
        metrics = resource_state.setdefault(
            "validation_metrics",
            {
                "total_rows": 0,
                "clean_rows": 0,
                "bad_rows": 0,
                "batches_processed": 0,
                "issues": [],
                "failed_row_locations": [],
            },
        )

        # Update cumulative metrics
        metrics["total_rows"] += batch.num_rows
        metrics["clean_rows"] += clean_data.num_rows
        metrics["bad_rows"] += bad_data.num_rows
        metrics["batches_processed"] += 1

        # Store issue summaries (not all rows - for performance)
        for issue in issues:
            metrics["issues"].append(
                {
                    "batch_date": batch_date,
                    "issue_type": issue.issue_type,
                    "column": issue.column,
                    "severity": issue.severity.value,
                    "count": issue.issue_count,
                    "samples": issue.sample_values[:3],  # Limit samples
                }
            )

        # Extract and store failed row locations with details
        if bad_data.num_rows > 0:
            bad_mask = _create_bad_mask(batch, bad_data)
            failed_rows = _extract_failed_row_details(batch, pa.array(bad_mask), issues)
            metrics["failed_row_locations"].extend(failed_rows)

        yield {"clean": clean_data, "bad": bad_data}

    @dlt.transformer(
        name=resource_name, data_from=validate_data, write_disposition="append"
    )
    def extract_clean_data(validation_result: dict) -> Iterator[pa.RecordBatch]:
        """Extract clean data from validation result."""
        clean_batch: pa.RecordBatch = validation_result["clean"]
        if clean_batch.num_rows > 0:
            logger.debug(
                "Yielding %d clean rows for %s.", clean_batch.num_rows, resource_name
            )
            yield clean_batch

    @dlt.transformer(
        name=f"bad__{resource_name}",
        data_from=validate_data,
        write_disposition="append",
    )
    def extract_bad_data(validation_result: dict) -> Iterator[pa.RecordBatch]:
        """Extract bad data from validation result with location metadata."""
        bad_batch: pa.RecordBatch = validation_result["bad"]

        if bad_batch.num_rows > 0:
            logger.debug(
                "Yielding %d bad rows for %s.", bad_batch.num_rows, resource_name
            )

            # Track bad data location in resource state for remediation
            resource_state = dlt.current.resource_state()
            bad_batch_idx = resource_state.get("bad_batch_count", 0)
            resource_state["bad_batch_count"] = bad_batch_idx + 1

            # Store location info for post-pipeline reporting
            locations = resource_state.setdefault("bad_data_locations", [])
            locations.append(
                {
                    "batch_index": bad_batch_idx,
                    "row_count": bad_batch.num_rows,
                    "destination_path": f"bad__{resource_name}/batch_{bad_batch_idx}",
                }
            )

            yield bad_batch

    @dlt.resource(
        name=f"validation_report__{resource_name}",
        write_disposition="replace",
    )
    def validation_report() -> Iterator[dict]:
        """Emit validation summary as a loadable resource.

        This resource contains aggregated validation metrics from the entire pipeline run.
        """
        resource_state = dlt.current.resource_state()
        metrics = resource_state.get("validation_metrics", {})

        if metrics:
            # Calculate pass rate
            total = metrics.get("total_rows", 0)
            pass_rate = (metrics.get("clean_rows", 0) / total * 100) if total > 0 else 0

            yield {
                "batch_date": batch_date,
                "resource_name": resource_name,
                "total_rows": metrics.get("total_rows", 0),
                "clean_rows": metrics.get("clean_rows", 0),
                "bad_rows": metrics.get("bad_rows", 0),
                "batches_processed": metrics.get("batches_processed", 0),
                "pass_rate": round(pass_rate, 2),
                "issues_found": len(metrics.get("issues", [])),
                "failed_row_count": len(metrics.get("failed_row_locations", [])),
            }

    return validate_data, extract_clean_data, extract_bad_data, validation_report
