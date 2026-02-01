"""Integration tests for data validation modules."""

import pyarrow as pa
import pytest

from jestr.validators.core import (
    DataQualityIssue,
    ValidationResult,
    _get_bad_row_mask,
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
        """Test accumulating validation results."""
        batches = [
            pa.record_batch({"id": [1, 2], "text": ["a", "b"]}),
            pa.record_batch({"id": [3, 4], "text": ["c", "d\ufffd"]}),
            pa.record_batch({"id": [5, 6], "text": ["e", "f"]}),
        ]

        all_clean_rows = 0
        all_bad_rows = 0

        for batch in batches:
            mask = _get_bad_row_mask(batch)
            clean, bad = _partition_by_validity(batch, mask)
            all_clean_rows += clean.num_rows
            all_bad_rows += bad.num_rows

        assert all_clean_rows == 5  # Rows with clean data
        assert all_bad_rows == 1  # Row with replacement char
