import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from loguru import logger

# Configure loguru to show structured fields
logger.remove()
logger.add(
    sys.stderr,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | <level>{level: <8}</level> | <level>{message}</level> | {extra}",
    colorize=True,
)


def get_csv(filepath: str) -> pa.Table:
    """Read CSV and validate UTF-8, raising error if invalid encoding found."""
    import pyarrow.csv as csv

    options = csv.ConvertOptions(check_utf8=True)

    table = csv.read_csv(
        filepath,
        read_options=csv.ReadOptions(encoding="utf-8"),
        convert_options=options,
    )

    # Check if any string columns were downgraded to binary
    for field in table.schema:
        if pa.types.is_binary(field.type):
            raise ValueError(
                f"Column '{field.name}' contains invalid UTF-8 and was converted to binary. "
                f"This indicates encoding issues in the source data."
            )

    return table


from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class ValidationRule:
    """Configuration for a single validation check."""

    name: str
    description: str
    pattern: str
    is_error: bool  # True for ERROR, False for WARNING
    matcher: str = "regex"  # 'regex' or 'substring'


VALIDATION_RULES = [
    ValidationRule(
        name="replacement_char",
        description="Replacement character (U+FFFD) - DATA CORRUPTION",
        pattern="\ufffd",
        is_error=True,
        matcher="substring",
    ),
    ValidationRule(
        name="control_char",
        description="Control character",
        pattern="[\x00-\x08\x0b\x0c\x0e-\x1f]",
        is_error=False,
        matcher="regex",
    ),
    ValidationRule(
        name="non_char",
        description="Unicode non-character (not for interchange)",
        pattern="[\ufdd0-\ufdef\ufffe\uffff]",
        is_error=False,
        matcher="regex",
    ),
]


def _check_pattern(
    col: pa.Array,
    col_name: str,
    rule: ValidationRule,
    max_log: int,
) -> tuple[list[str], int]:
    """Check column for pattern violations and log results.

    Args:
        col: PyArrow column to check.
        col_name: Column name for logging.
        rule: Validation rule to apply.
        max_log: Maximum number of violations to log individually.

    Returns:
        Tuple of (violation messages, total violation count).
    """
    # Match pattern using appropriate function
    if rule.matcher == "substring":
        matches = pc.match_substring(col, rule.pattern)  # type: ignore
    else:
        matches = pc.match_substring_regex(col, rule.pattern)  # type: ignore

    if not pc.any(matches).as_py():  # type: ignore
        return [], 0

    # Get indices and count (avoid materializing full list if not needed)
    bad_indices_array = pc.indices_nonzero(matches)  # type: ignore
    count = len(bad_indices_array)
    log_func = logger.error if rule.is_error else logger.warning

    # Log summary
    log_func(
        f"Column '{col_name}': Found {count} rows with {rule.description}",
        column=col_name,
        count=count,
    )

    # Batch extract violations for logging (single Arrow operation)
    log_limit = min(max_log, count)
    if log_limit > 0:
        bad_indices_slice = bad_indices_array.slice(0, log_limit)
        bad_indices_list = bad_indices_slice.to_pylist()
        bad_values = col.take(bad_indices_slice).to_pylist()  # Batch extraction

        # Log individual violations
        violations = []
        for idx, value in zip(bad_indices_list, bad_values, strict=False):
            value_preview = value[:100] if value else None
            log_func(
                f"  Row {idx}: {rule.description}",
                column=col_name,
                row=idx,
                value=repr(value_preview),
            )
            violations.append(
                f"Column '{col_name}', row {idx}: {rule.description} - {value_preview!r}"
            )

        # Log overflow message if needed
        if count > max_log:
            log_func(
                f"  ... and {count - max_log} more rows with {rule.name} (not shown)"
            )
    else:
        violations = []

    return violations, count


def validate_arrow_table_utf8(
    table: pa.Table,
    raise_on_error: bool = True,
    check_binary: bool = True,
    max_errors_to_log: int = 100,
    max_total_errors: int | None = None,
):
    """Validate all string columns in an Arrow table for UTF-8 validity using PyArrow Compute.

    Industry best practices for data quality:
    - ERRORS (abort pipeline): Replacement characters (U+FFFD) indicate data corruption
    - WARNINGS (log but continue): Non-characters (U+FDD0-FDEF, U+FFFE, U+FFFF), control characters

    Args:
        table: Arrow table to validate.
        raise_on_error: Whether to raise ValueError on validation errors.
        check_binary: Whether to check for binary/control characters in text fields.
        max_errors_to_log: Maximum number of individual error rows to log per check (default 100).
        max_total_errors: Fast-fail threshold - stop validation after N total errors found.
                         None = validate all columns. Recommended: 100-1000 for large tables.

    Returns:
        List of error messages if raise_on_error=False, otherwise None.

    Raises:
        ValueError: If validation errors found and raise_on_error=True.
    """
    errors = []
    warnings = []
    total_error_count = 0
    columns_checked = 0
    fast_fail_triggered = False

    # Filter rules based on check_binary flag
    rules = [
        rule
        for rule in VALIDATION_RULES
        if rule.name == "replacement_char" or check_binary
    ]

    # Validate each string column
    for field in table.schema:
        if not pa.types.is_string(field.type):
            continue

        col = table[field.name]
        columns_checked += 1

        # Apply each validation rule
        for rule in rules:
            violations, violation_count = _check_pattern(
                col, field.name, rule, max_errors_to_log
            )

            if rule.is_error:
                errors.extend(violations)
                total_error_count += violation_count

                # Fast-fail check: stop validation if error threshold exceeded
                if max_total_errors and total_error_count >= max_total_errors:
                    logger.warning(
                        f"Fast-fail triggered: {total_error_count} errors found (threshold: {max_total_errors}). "
                        f"Stopped validation after {columns_checked} columns.",
                        total_errors=total_error_count,
                        threshold=max_total_errors,
                        columns_checked=columns_checked,
                    )
                    fast_fail_triggered = True
                    break
            else:
                warnings.extend(violations)

        if fast_fail_triggered:
            break

    # Log summary
    if errors or warnings:
        summary_msg = f"Validation {'STOPPED EARLY' if fast_fail_triggered else 'complete'}: {len(errors)} errors, {len(warnings)} warnings"
        if fast_fail_triggered:
            summary_msg += f" ({columns_checked} of {sum(1 for f in table.schema if pa.types.is_string(f.type))} string columns checked)"
        logger.info(
            summary_msg,
            errors=len(errors),
            warnings=len(warnings),
            fast_fail=fast_fail_triggered,
        )

    # Raise if errors found and requested
    if errors and raise_on_error:
        error_summary = (
            f"Found {len(errors)} validation error(s) - data quality check FAILED:"
            + (" (fast-fail mode)" if fast_fail_triggered else "")
            + "\n"
            + "\n".join(f"  - {err}" for err in errors[:10])
        )
        if len(errors) > 10:
            error_summary += f"\n  ... and {len(errors) - 10} more errors (see logs)"
        raise ValueError(error_summary)

    return errors if not raise_on_error else None


def validate_arrow_string_arrays(table: pa.Table, raise_on_error: bool = True):
    """Validate all string columns in an Arrow table using PyArrow's StringArray.validate(full=True).

    This method reads each string column as a PyArrow StringArray and runs full validation
    against them to check for UTF-8 validity and structural integrity.

    Args:
        table: Arrow table to validate.
        raise_on_error: Whether to raise ValueError on validation errors.

    Returns:
        List of error messages if raise_on_error=False, otherwise None.

    Raises:
        ValueError: If validation errors found and raise_on_error=True.
    """
    errors = []
    string_columns_checked = 0

    # Validate each string column using PyArrow's native validate method
    for field in table.schema:
        if not pa.types.is_string(field.type):
            continue

        col = table[field.name]
        string_columns_checked += 1

        try:
            # Cast to StringArray and run full validation
            string_array = col.cast(pa.string())
            validation_result = string_array.validate(full=True)

            if validation_result is None:
                logger.info(
                    f"Column '{field.name}': StringArray validation passed",
                    column=field.name,
                    rows=len(string_array),
                )
            else:
                error_msg = f"Column '{field.name}': StringArray validation failed - {validation_result}"
                logger.error(error_msg, column=field.name)
                errors.append(error_msg)

        except Exception as e:
            error_msg = f"Column '{field.name}': StringArray validation error - {e!s}"
            logger.error(error_msg, column=field.name, error=str(e))
            errors.append(error_msg)

    # Log summary
    logger.info(
        "StringArray validation complete",
        columns_checked=string_columns_checked,
        errors=len(errors),
    )

    # Raise if errors found and requested
    if errors and raise_on_error:
        error_summary = (
            f"Found {len(errors)} StringArray validation error(s):\n"
            + "\n".join(f"  - {err}" for err in errors)
        )
        raise ValueError(error_summary)

    return errors if not raise_on_error else None


def validate_parquet_text_columns_with_duckdb(parquet_file: str):
    """Validate parquet string columns using DuckDB's read_text function for UTF-8 validation.

    DuckDB's read_text function validates that file content is valid UTF-8 and throws an error
    if invalid UTF-8 encoding is detected. This provides strict UTF-8 validity checking at the
    engine level.

    Args:
        parquet_file: Path to the parquet file to validate.

    Raises:
        ValueError: If UTF-8 validation errors are found in string column content.
    """
    import os
    import tempfile

    import duckdb

    try:
        conn = duckdb.connect(":memory:")

        # Load parquet file and get string columns
        schema = conn.execute(f"DESCRIBE '{parquet_file}'").fetchall()
        string_columns = [
            col for col, dtype, *_ in schema if dtype.startswith("VARCHAR")
        ]

        if not string_columns:
            logger.info("No string columns found in parquet file")
            return

        # For each string column, extract values to temporary text files and validate with read_text
        total_rows = conn.execute(f"SELECT COUNT(*) FROM '{parquet_file}'").fetchone()[
            0
        ]
        logger.info(
            "Starting UTF-8 validation using read_text",
            filepath=parquet_file,
            total_rows=total_rows,
            string_columns=len(string_columns),
        )

        violations = []
        temp_files = []

        try:
            for col_name in string_columns:
                # Create a temporary file to store column values (one per line)
                with tempfile.NamedTemporaryFile(
                    mode="w", delete=False, suffix=".txt", encoding="utf-8"
                ) as tmp:
                    temp_file = tmp.name
                    temp_files.append(temp_file)

                    # Export column values to temp file
                    export_query = f"""
                        COPY (
                            SELECT COALESCE({col_name}, '') as value
                            FROM '{parquet_file}'
                            WHERE {col_name} IS NOT NULL
                        )
                        TO '{temp_file}' (FORMAT CSV, DELIMITER E'\\n', QUOTE '');
                    """
                    conn.execute(export_query)

                # Now validate the temp file using read_text (strict UTF-8 validation)
                try:
                    validation_query = (
                        f"SELECT COUNT(*) as row_count FROM read_text('{temp_file}')"
                    )
                    result = conn.execute(validation_query).fetchone()
                    row_count = result[0] if result else 0

                    logger.info(
                        f"Column '{col_name}': UTF-8 validation passed",
                        column=col_name,
                        rows_validated=row_count,
                    )
                except Exception as utf8_error:
                    error_msg = str(utf8_error)
                    logger.error(
                        f"Column '{col_name}': UTF-8 validation FAILED - invalid encoding detected",
                        column=col_name,
                        error=error_msg,
                    )
                    violations.append(
                        f"Column '{col_name}': Invalid UTF-8 encoding detected - {error_msg}"
                    )

        finally:
            # Clean up temporary files
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                    except OSError:
                        pass

        if violations:
            error_msg = (
                "DuckDB read_text UTF-8 validation found invalid encoding:\n"
                + "\n".join(f"  - {v}" for v in violations)
            )
            raise ValueError(error_msg)

        logger.info(
            "DuckDB read_text validation complete: all string columns have valid UTF-8",
            columns_checked=len(string_columns),
        )

    except ValueError:
        raise
    except Exception as e:
        logger.error(f"DuckDB read_text validation failed: {e}", error=str(e))
        raise


def write_parquet_with_validation(table: pa.Table, parquet_file: str):
    """Write Parquet file with strict UTF-8 validation."""
    # Use StringArray validation instead of validate_arrow_table_utf8
    validate_arrow_string_arrays(table, raise_on_error=True)

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("city", pa.string()),
        ]
    )

    with pq.ParquetWriter(parquet_file, schema) as writer:
        writer.write_table(table)
    logger.info("Written Parquet file", filepath=parquet_file, rows=len(table))


if __name__ == "__main__":
    csv_file = "input_bad.csv"
    parquet_file = "output/test_invalid_utf8.parquet"

    table = get_csv("input_bad.csv")
    logger.info(
        "Loaded CSV table",
        filepath=csv_file,
        rows=len(table),
        columns=len(table.schema),
        schema=str(table.schema),
    )
    logger.debug(f"Table preview:\n{table}")

    try:
        write_parquet_with_validation(table, parquet_file)
        # Read the Parquet file back into a Polars dataframe and print it to logger
        table_read = pq.read_table(parquet_file)
        logger.info(
            "Read Parquet file",
            filepath=parquet_file,
            rows=len(table_read),
            columns=len(table_read.schema),
        )
        logger.debug(f"Table preview:\n{table_read}")
    except ValueError as e:
        logger.warning(f"PyArrow validation errors found: {e}")

    # Validate using DuckDB's read_text function (run regardless of PyArrow results)
    logger.info("Running DuckDB validation on parquet file...")
    try:
        validate_parquet_text_columns_with_duckdb(parquet_file)
    except ValueError as e:
        logger.error(f"DuckDB validation errors found: {e}")
        sys.exit(1)
