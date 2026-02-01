"""Data quality validation of ingested data structures."""

import pyarrow as pa
import structlog
from dlt.common.schema.typing import TTableSchema

from jestr.contracts import ODCSContract

from .check import structural
from .core import DataQualityIssue, ValidationResult, Validator, _partition_by_validity
from .core.base import _get_bad_row_mask

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
        issues: list[DataQualityIssue] = []

        # First validate the Arrow record structures. Abends if invalid.
        if self._validate_arrow_structures(batch):
            for field in batch.schema:
                column = batch[field.name]
                column_name = field.name

                # Validate string columns for encoding issues (single pass).
                if pa.types.is_string(column.type) or pa.types.is_large_string(
                    column.type
                ):
                    issues.extend(self._validate_string_column(column, column_name))

            # Bifurcate the batch into clean and bad data based on issues found.
            bad_mask = _get_bad_row_mask(batch)
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

    @staticmethod
    def _validate_string_column(
        column: pa.ChunkedArray, column_name: str
    ) -> list[DataQualityIssue]:
        """Validate string column for encoding issues in a single pass."""
        issues: list[DataQualityIssue] = []

        issues.extend(structural.contains_no_replacement_char(column, column_name))
        issues.extend(structural.contains_no_invalid_chars(column, column_name))

        return issues
