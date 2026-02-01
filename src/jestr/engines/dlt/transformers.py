"""dlt transformers for data validation and bifurcation into clean and bad datasets."""

from collections.abc import Callable, Iterator
from dataclasses import asdict
from typing import Any

import dlt
import pyarrow as pa
import structlog

from jestr.validators import IngestionValidator

logger = structlog.get_logger()


def create_validation_transformers(
    extract_resource: Any,
    resource_name: str,
    validator: IngestionValidator,
    batch_date: str = "",
) -> tuple[Callable, Callable, Callable]:
    """Factory method for validation and bifurcation transformers."""

    @dlt.transformer(
        name=f"validate__{resource_name}", data_from=extract_resource, selected="False"
    )
    def validate_data(batch: pa.RecordBatch) -> Iterator[dict]:
        """Validate data in batches and yield clean/bad as dict."""
        clean_data, bad_data, issues = validator.validate(batch)

        logger.info(
            "Validation completed for %s. Clean rows: %d, Bad rows: %d, Issues found: %d",
            resource_name,
            clean_data.num_rows,
            bad_data.num_rows,
            len(issues),
        )

        state = dlt.current.source_state()
        metrics_list = state.setdefault("validation_metrics", [])
        metrics_list.append(
            {
                "batch_date": batch_date,
                "total_rows": batch.num_rows,
                "clean_rows": clean_data.num_rows,
                "bad_rows": bad_data.num_rows,
                "issues_count": len(issues),
                "issues": [asdict(issue) for issue in issues] if issues else [],
            }
        )

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
        """Extract bad data from validation result."""
        bad_batch: pa.RecordBatch = validation_result["bad"]
        if bad_batch.num_rows > 0:
            logger.debug(
                "Yielding %d bad rows for %s.", bad_batch.num_rows, resource_name
            )
            yield bad_batch

    return validate_data, extract_clean_data, extract_bad_data
