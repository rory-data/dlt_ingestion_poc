"""Data quality validation of ingested data structures."""

import pyarrow as pa
import structlog
from dlt.common.schema.typing import TTableSchema

from jestr.contracts import ODCSContract

from .checks import structural
from .core import DataQualityIssue, ValidationResult, Validator, _partition_by_validity

logger = structlog.get_logger()


class IngestionValidator(Validator):
    """Validator for ingest data structures."""

    def __init__(
        self, contract: ODCSContract, schema_metadata: TTableSchema | None = None
    ) -> None:
        """Initialise IngestionValidator."""
        self.contract = contract
        self.schema_metadata = schema_metadata

    def validate(self, batch: pa.RecordBatch) -> ValidationResult:
        """Validate a PyArrow RecordBatch."""
        if self._is_valid_arrow_structure(batch):
            bad_mask = pa.repeat(pa.scalar(False, type=pa.bool_()), batch.num_rows)
            issues: list[DataQualityIssue] = []

            for field in batch.schema:
                column = batch[field.name]

                # Only validate string columns (extend this for other types as needed)
                if not (
                    pa.types.is_string(column.type)
                    or pa.types.is_large_string(column.type)
                ):
                    continue

                # Get mask and issues from validator in single pass
                column_mask, column_issues = structural.validate_string_column(
                    column, field.name
                )
                bad_mask = pa.compute.or_(bad_mask, column_mask)
                issues.extend(column_issues)

            if isinstance(bad_mask, pa.ChunkedArray):
                bad_mask = bad_mask.combine_chunks()

            clean_data, bad_data = _partition_by_validity(batch, bad_mask)

            return ValidationResult(
                clean_data=clean_data,
                bad_data=bad_data,
                issues=issues,
                schema_metadata=self.schema_metadata,
            )

    @staticmethod
    def _is_valid_arrow_structure(batch: pa.RecordBatch) -> bool:
        """Check if the Arrow RecordBatch structure is valid."""
        try:
            batch.validate(full=True)
            return True
        except pa.ArrowInvalid as exc:
            logger.exception(
                "Arrow batch structure validation failed.",
                error_details=str(exc),
            )
            return False
