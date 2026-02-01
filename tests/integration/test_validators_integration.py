"""Integration tests for data validation modules."""

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from jestr.validators.checks import structural
from jestr.validators.core import (
    DataQualityIssue,
    ValidationResult,
    _partition_by_validity,
)
from jestr.validators.core.issues import Severity


@pytest.mark.integration
class TestValidatorIntegration:
    """Integration tests for validators."""

    def test_validation_pipeline_chain(self):
        """Test chaining multiple validators."""

        class Validator1:
            def validate(self, batch):
                clean = pa.table({"id": [1, 2]})
                bad = pa.table({"id": []})
                return ValidationResult(clean_data=clean, bad_data=bad)

        class Validator2:
            def validate(self, batch):
                clean = pa.table({"id": [1]})
                bad = pa.table({"id": [2]})
                issues = [
                    DataQualityIssue(
                        severity=Severity.WARNING,
                        issue_type="Minor Issue",
                        column="id",
                        issue_count=1,
                        sample_values=["2"],
                    )
                ]
                return ValidationResult(clean_data=clean, bad_data=bad, issues=issues)

        # First validator
        v1 = Validator1()
        batch = pa.record_batch({"id": [1, 2]})
        result1 = v1.validate(batch)
        assert result1.is_valid

        # Second validator on clean data
        v2 = Validator2()
        result2 = v2.validate(result1.clean_data.to_batches()[0])
        assert not result2.is_valid
        assert len(result2.issues) == 1

    def test_validation_accumulation(self):
        """Test accumulating validation results across batches."""
        batches = [
            pa.record_batch({"id": [1, 2], "text": ["a", "b"]}),
            pa.record_batch({"id": [3, 4], "text": ["c", "d\ufffd"]}),
            pa.record_batch({"id": [5, 6], "text": ["e", "f"]}),
        ]

        all_clean_rows = 0
        all_bad_rows = 0

        for batch in batches:
            # Create bad row mask by validating string columns
            bad_mask = pa.repeat(pa.scalar(False, type=pa.bool_()), batch.num_rows)

            for field in batch.schema:
                column = batch[field.name]
                if pa.types.is_string(column.type) or pa.types.is_large_string(
                    column.type
                ):
                    column_mask, _ = structural.validate_string_column(
                        column, field.name
                    )
                    bad_mask = pc.or_(bad_mask, column_mask)

            if isinstance(bad_mask, pa.ChunkedArray):
                bad_mask = bad_mask.combine_chunks()

            clean, bad = _partition_by_validity(batch, bad_mask)
            all_clean_rows += clean.num_rows
            all_bad_rows += bad.num_rows

        assert all_clean_rows == 5  # Rows with clean data
        assert all_bad_rows == 1  # Row with replacement char
