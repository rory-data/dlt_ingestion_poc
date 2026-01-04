"""Demonstration of cleaner PyArrow validation using compute functions."""

import sys

import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger

# Configure logger
logger.remove()
logger.add(sys.stderr, format="<level>{message}</level>")


def validate_arrow_cleaner(table: pa.Table):
    """Demonstrates cleaner validation using PyArrow Compute."""
    logger.info(f"Validating table with {len(table)} rows...")

    for field in table.schema:
        if pa.types.is_string(field.type):
            col = table[field.name]

            # 1. Check for Replacement Characters (U+FFFD)
            # This is much faster than iterating rows
            has_replacement = pc.match_substring(col, "\ufffd")  # type: ignore
            if pc.any(has_replacement).as_py():  # type: ignore
                # Get indices of bad rows
                bad_indices = pc.indices_nonzero(has_replacement)  # type: ignore
                logger.warning(
                    f"Column '{field.name}': Found {len(bad_indices)} rows with replacement characters."
                )
                # Example: print first 3 bad values
                bad_values = col.take(bad_indices).slice(0, 3)
                logger.info(f"  Sample: {bad_values.to_pylist()}")

            # 2. Check for Control Characters and Non-characters
            # Combined regex for performance
            # Control: [\x00-\x08\x0b\x0c\x0e-\x1f]
            # Non-char: [\ufdd0-\ufdef\ufffe\uffff]
            # Note: We use Python string for regex pattern to avoid escaping issues in C++ engine if possible,
            # but PyArrow uses RE2.
            # We use a normal string (not raw) so Python handles the escapes and passes actual chars/bytes to RE2.
            bad_chars_pattern = "[\x00-\x08\x0b\x0c\x0e-\x1f\ufdd0-\ufdef\ufffe\uffff]"

            has_bad_chars = pc.match_substring_regex(col, bad_chars_pattern)  # type: ignore
            if pc.any(has_bad_chars).as_py():  # type: ignore
                bad_indices = pc.indices_nonzero(has_bad_chars)  # type: ignore
                logger.warning(
                    f"Column '{field.name}': Found {len(bad_indices)} rows with control/non-characters."
                )
                bad_values = col.take(bad_indices).slice(0, 3)
                logger.info(f"  Sample: {bad_values.to_pylist()}")


if __name__ == "__main__":
    # Create sample data
    data = [
        "Normal string",
        "With replacement \ufffd",
        "With control \x01",
        "With non-char \ufdd0",
        "Another normal one",
    ]
    table = pa.Table.from_pydict({"text": data})

    logger.info("--- PyArrow Approach ---")
    validate_arrow_cleaner(table)
