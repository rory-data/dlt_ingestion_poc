"""Examples demonstrating proper use of the Validator protocol.

This module shows:
1. Using validators with dependency injection
2. Creating custom validators
3. Composing validators in a chain
4. Testing validators with mocks
"""

import pyarrow as pa

from ingestion.core.validate.base import ValidationResult, Validator
from ingestion.core.validate.validator import UTF8Validator

# ============================================================================
# EXAMPLE 1: Using Validators with Dependency Injection
# ============================================================================


def process_data_pipeline(
    data: pa.Table,
    validator: Validator,  # Injected, not hard-coded
) -> ValidationResult:
    """Pipeline that accepts validator as dependency.

    This design enables:
    - Easy testing (inject mock validator)
    - Easy swapping (different validators for different cases)
    - Clear what the pipeline depends on

    Args:
        data: PyArrow Table to process
        validator: Validator implementation to use

    Returns:
        ValidationResult with clean/bad data
    """
    # Use injected validator
    result = validator.validate(data)

    if result.is_valid:
        print(f"✓ All {result.clean_row_count} rows are valid")
    else:
        print(f"✗ Found {len(result.issues)} issues in {result.bad_row_count} rows")

    return result


# Usage
if __name__ == "__main__":
    # Create sample data
    data = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

    # Production: Use real validator
    validator = UTF8Validator(issue_action="quarantine")
    result = process_data_pipeline(data, validator)

    # Testing: Use mock validator (see example 4)
    # mock_validator = MockValidator(return_valid=True)
    # result = process_data_pipeline(data, mock_validator)


# ============================================================================
# EXAMPLE 2: Creating a Custom Validator (Implementing the Protocol)
# ============================================================================


class SchemaValidator:
    """Custom validator that checks schema constraints.

    Implements the Validator protocol to be used interchangeably with UTF8Validator.
    """

    def __init__(self, min_columns: int = 1, max_columns: int = 100):
        """Initialize schema validator.

        Args:
            min_columns: Minimum number of columns required
            max_columns: Maximum number of columns allowed
        """
        self.min_columns = min_columns
        self.max_columns = max_columns

    def validate(self, data: pa.Table | pa.RecordBatch) -> ValidationResult:
        """Validate schema constraints.

        Args:
            data: PyArrow Table or RecordBatch

        Returns:
            ValidationResult (all rows clean if schema valid, all bad if not)
        """
        # Convert to Table if needed
        table = (
            pa.Table.from_batches([data]) if isinstance(data, pa.RecordBatch) else data
        )

        # Check constraints
        num_cols = table.num_columns
        if not (self.min_columns <= num_cols <= self.max_columns):
            # Schema invalid - mark all rows as bad
            bad_rows = table
            clean_rows = table.schema.empty_table()
            return ValidationResult(
                clean_data=clean_rows,
                bad_data=bad_rows,
                issues=[],
            )

        # Schema valid - all rows clean
        empty_rows = table.schema.empty_table()
        return ValidationResult(
            clean_data=table,
            bad_data=empty_rows,
            issues=[],
        )


# ============================================================================
# EXAMPLE 3: Composing Validators with a ValidationChain
# ============================================================================


class ValidationChain:
    """Chain multiple validators together.

    Runs validators sequentially. If one fails (has issues), subsequent
    validators only see the clean data.

    This implements the Chain of Responsibility pattern.
    """

    def __init__(
        self, validators: list[Validator], stop_on_first_failure: bool = False
    ):
        """Initialize validation chain.

        Args:
            validators: List of validators to run in order
            stop_on_first_failure: Stop running if a validator finds issues
        """
        self.validators = validators
        self.stop_on_first_failure = stop_on_first_failure

    def validate(self, data: pa.Table | pa.RecordBatch) -> ValidationResult:
        """Run all validators in sequence.

        Args:
            data: PyArrow Table or RecordBatch

        Returns:
            Final ValidationResult (combined issues from all validators)
        """
        current_data = data
        all_issues = []
        all_bad_rows = None

        for validator in self.validators:
            result = validator.validate(current_data)
            all_issues.extend(result.issues)

            # Track bad rows
            if all_bad_rows is None:
                all_bad_rows = result.bad_data
            else:
                # Combine bad rows from multiple validators
                # (In practice, you'd use more sophisticated union logic)
                pass

            # Move to next validator with clean data only
            current_data = result.clean_data

            # Stop early if configured and issues found
            if self.stop_on_first_failure and result.issues:
                break

        # Return final result
        return ValidationResult(
            clean_data=current_data,
            bad_data=all_bad_rows
            or (
                pa.Table.from_batches([data]).schema.empty_table()
                if isinstance(data, pa.RecordBatch)
                else data
            ),
            issues=all_issues,
        )


# Usage
if __name__ == "__main__":
    data = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})

    # Chain multiple validators
    chain = ValidationChain(
        [
            SchemaValidator(min_columns=2),
            UTF8Validator(issue_action="quarantine"),
        ]
    )

    result = chain.validate(data)
    print(
        f"Chain validation: {result.clean_row_count} clean, {result.bad_row_count} bad"
    )


# ============================================================================
# EXAMPLE 4: Testing with Mock Validators
# ============================================================================


class MockValidator:
    """Mock validator for testing.

    Use in unit tests to verify pipeline behavior without real validation logic.
    """

    def __init__(self, return_valid: bool = True, issues_to_return: list = None):
        """Initialize mock validator.

        Args:
            return_valid: If True, mark all rows as clean
            issues_to_return: List of issues to return (if not valid)
        """
        self.return_valid = return_valid
        self.issues_to_return = issues_to_return or []
        self.validate_called_with = []  # Track calls for assertions

    def validate(self, data: pa.Table | pa.RecordBatch) -> ValidationResult:
        """Mock validation - returns pre-configured result."""
        # Track the call
        self.validate_called_with.append(data)

        if self.return_valid:
            # All rows clean
            empty = (
                pa.RecordBatch.from_arrays(
                    [pa.array([], type=field.type) for field in data.schema],
                    schema=data.schema,
                )
                if isinstance(data, pa.RecordBatch)
                else data.schema.empty_table()
            )
            return ValidationResult(clean_data=data, bad_data=empty, issues=[])
        else:
            # All rows bad
            empty = (
                pa.RecordBatch.from_arrays(
                    [pa.array([], type=field.type) for field in data.schema],
                    schema=data.schema,
                )
                if isinstance(data, pa.RecordBatch)
                else data.schema.empty_table()
            )
            return ValidationResult(
                clean_data=empty, bad_data=data, issues=self.issues_to_return
            )


# Usage in tests
def test_pipeline_with_valid_data():
    """Test that pipeline handles valid data correctly."""
    data = pa.table({"id": [1, 2]})
    mock_validator = MockValidator(return_valid=True)

    result = process_data_pipeline(data, mock_validator)

    # Assertions
    assert result.is_valid
    assert result.clean_row_count == 2
    assert len(mock_validator.validate_called_with) == 1


def test_pipeline_with_invalid_data():
    """Test that pipeline handles invalid data correctly."""
    data = pa.table({"id": [1, 2]})
    mock_validator = MockValidator(return_valid=False)

    result = process_data_pipeline(data, mock_validator)

    # Assertions
    assert not result.is_valid
    assert result.bad_row_count == 2


# ============================================================================
# EXAMPLE 5: Builder Pattern for Complex Validation
# ============================================================================


class ValidationBuilder:
    """Builder for constructing complex validation pipelines."""

    def __init__(self):
        self._validators = []
        self._stop_on_failure = False

    def with_utf8_validation(self, action: str = "quarantine") -> "ValidationBuilder":
        """Add UTF-8 validation to the pipeline.

        Args:
            action: 'reject' or 'quarantine'

        Returns:
            Self for chaining
        """
        self._validators.append(UTF8Validator(issue_action=action))
        return self

    def with_schema_validation(
        self, min_cols: int = 1, max_cols: int = 100
    ) -> "ValidationBuilder":
        """Add schema validation to the pipeline.

        Args:
            min_cols: Minimum columns required
            max_cols: Maximum columns allowed

        Returns:
            Self for chaining
        """
        self._validators.append(
            SchemaValidator(min_columns=min_cols, max_columns=max_cols)
        )
        return self

    def stop_on_first_failure(self) -> "ValidationBuilder":
        """Stop validation chain on first failure.

        Returns:
            Self for chaining
        """
        self._stop_on_failure = True
        return self

    def build(self) -> ValidationChain:
        """Build the validation chain.

        Returns:
            Configured ValidationChain
        """
        return ValidationChain(
            self._validators,
            stop_on_first_failure=self._stop_on_failure,
        )


# Usage
if __name__ == "__main__":
    validator = (
        ValidationBuilder()
        .with_schema_validation(min_cols=2)
        .with_utf8_validation(action="quarantine")
        .stop_on_first_failure()
        .build()
    )

    data = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
    result = validator.validate(data)
    print(
        f"Built validation: {result.clean_row_count} clean, {result.bad_row_count} bad"
    )
