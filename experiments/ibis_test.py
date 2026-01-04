import re

import ibis
import ibis.selectors as s
import pyarrow as pa
from loguru import logger


# Helper function in Ibis to identify string columns and rows that match r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]")
def _detect_control_characters(col):
    """Detect control characters in a given column."""

    @ibis.udf.scalar.python
    def has_control_chars(s: str) -> bool:
        if s is None:
            return False
        return bool(re.search(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]", s))

    control_char_col = has_control_chars(col).name("has_control_chars")
    return control_char_col


def _validate_arrow_table(pyarrow_table: pa.Table):
    issues = []
    check_results = pyarrow_table.validate()
    if not check_results["is_valid"]:
        issues.append({"issues": check_results["issues"]})
    return issues


def validate_string_columns(table_name: str, pyarrow_table: pa.Table):
    issues = []
    con = ibis.duckdb.connect()
    ibis_table = con.create_table(table_name, pyarrow_table)
    string_cols = ibis_table.select(s.of_type("string")).columns
    for col_name in string_cols:
        control_char_col = _detect_control_characters(ibis_table[col_name])
        result = (
            ibis_table.select(
                [
                    ibis_table.id,
                    ibis_table[col_name],
                    control_char_col,
                ]
            )
            .filter(control_char_col)
            .execute()
        )
        for row in result.values.tolist():
            issues.append({"row": row[0], "text": row[1]})

    return issues


if __name__ == "__main__":
    # Connect to DuckDB backend (best for text operations)

    # Create pyarrow table

    t = pa.table(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "text": [
                "Kia ora",  # Valid Māori with macron
                "Hello\x00World",  # Null byte
                "Café",  # Valid diacritic
                "Tē̲st\u200b",  # Zero-width space
                "​​​Test",  # Non-breaking spaces
                "Tëst",  # Different normalization form
            ],
        },
    )

    logger.info("Table created successfully.")

    validate_string_columns("text_data", t)
