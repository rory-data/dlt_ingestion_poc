"""Pipeline initialization and execution utilities for dlt ingestion scripts."""

import sys
from collections.abc import Callable
from typing import Any

import dlt
import structlog

logger = structlog.get_logger()


def get_validation_metrics_from_state(pipeline: dlt.Pipeline) -> dict[str, Any]:
    """Retrieve validation metrics from pipeline state post-pipeline execution.

    This function extracts validation metrics accumulated during transformer execution,
    including metrics, issues, and failed row locations per resource.

    Args:
        pipeline: Executed dlt pipeline instance

    Returns:
        Dictionary mapping resource_name to validation metrics with structure:
        {
            "resource_name": {
                "validation_metrics": {
                    "total_rows": int,
                    "clean_rows": int,
                    "bad_rows": int,
                    "batches_processed": int,
                    "pass_rate": float,
                    "issues": [
                        {
                            "batch_date": str,
                            "issue_type": str,
                            "column": str,
                            "severity": str,
                            "count": int,
                            "samples": list[str],
                        }
                    ],
                    "failed_row_locations": [
                        {
                            "row_index": int,
                            "failed_checks": [
                                {
                                    "column": str,
                                    "value": str,
                                    "checks": list[dict],
                                }
                            ],
                        }
                    ],
                    "bad_data_locations": [
                        {
                            "batch_index": int,
                            "row_count": int,
                            "destination_path": str,
                        }
                    ],
                }
            }
        }
    """
    state = pipeline.state
    sources_state = state.get("sources", {})

    summaries = {}
    for source_name, source_state in sources_state.items():
        resources = source_state.get("resources", {})
        for resource_name, resource_state in resources.items():
            if "validation_metrics" in resource_state:
                metrics = resource_state["validation_metrics"]

                # Calculate pass rate if not already present
                if "pass_rate" not in metrics:
                    total = metrics.get("total_rows", 0)
                    clean = metrics.get("clean_rows", 0)
                    metrics["pass_rate"] = (
                        round(clean / total * 100, 2) if total > 0 else 0
                    )

                summaries[resource_name] = {
                    "source_name": source_name,
                    "validation_metrics": metrics,
                }

    return summaries


def run_pipeline_with_summary(
    pipeline: Any,
    source_callable: Callable,
    pipeline_description: str = "pipeline",
    contract: Any = None,
) -> dict[str, Any]:
    """Run dlt pipeline and extract validation metrics.

    Args:
        pipeline: dlt.Pipeline instance
        source_callable: Callable returning dlt source
        pipeline_description: Human-readable description of pipeline
        contract: Optional ODCS contract for reference

    Returns:
        Dictionary containing validation metrics from pipeline execution
    """
    try:
        logger.info("Starting data load...")
        logger.info(f"Pipeline: {pipeline_description}")

        load_info = pipeline.run(source_callable)
        logger.info("Pipeline completed successfully")
        logger.info(load_info)

        # Extract validation metrics from pipeline state
        validation_metrics = get_validation_metrics_from_state(pipeline)

        # Log summary for each resource
        for resource_name, metrics_info in validation_metrics.items():
            metrics = metrics_info["validation_metrics"]
            logger.info(
                "Validation summary for %s",
                resource_name,
                total_rows=metrics.get("total_rows", 0),
                clean_rows=metrics.get("clean_rows", 0),
                bad_rows=metrics.get("bad_rows", 0),
                pass_rate=metrics.get("pass_rate", 0),
                batches=metrics.get("batches_processed", 0),
            )

            # Log any critical issues
            issues = metrics.get("issues", [])
            if issues:
                critical_issues = [i for i in issues if i.get("severity") == "critical"]
                if critical_issues:
                    logger.warning(
                        "Critical validation issues detected in %s",
                        resource_name,
                        issue_count=len(critical_issues),
                    )

        return validation_metrics

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        sys.exit(1)
