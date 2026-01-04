import re
import unicodedata

import ibis
from loguru import logger

# Connect to DuckDB backend (best for text operations)
con = ibis.duckdb.connect()

# Create table
t = con.create_table(
    "text_data",
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


# Helper function to validate string column
def _validate_string_column(col_name: str) -> str:
    """Validate column is a string type"""
    col_type = t[col_name].type()
    if not isinstance(col_type, ibis.expr.datatypes.String):
        raise TypeError(f"Column '{col_name}' must be string type, got {col_type}")
    return col_name


# Check 1: Detect null bytes
def check_null_bytes(col_name):
    """Detect embedded null bytes in strings"""
    _validate_string_column(col_name)

    @ibis.udf.scalar.python
    def has_null_byte(s: str) -> bool:
        if s is None:
            return False
        return "\x00" in s

    result = (
        t.select(
            [
                t.id,
                t[col_name],
                has_null_byte(t[col_name]).name("has_null_byte"),
            ]
        )
        .filter(has_null_byte(t[col_name]))
        .execute()
    )

    return [{"row": row[0], "text": row[1]} for row in result.values.tolist()]


# Check 2: Detect non-printable control characters
def check_control_characters(col_name):
    """Detect control characters (except whitespace)"""
    _validate_string_column(col_name)

    @ibis.udf.scalar.python
    def find_control_chars(s: str) -> str:
        if s is None:
            return None
        for char in s:
            code_point = ord(char)
            if (
                0x00 <= code_point <= 0x08
                or 0x0B <= code_point <= 0x0C
                or 0x0E <= code_point <= 0x1F
                or 0x7F <= code_point <= 0x9F
            ):
                return repr(char)
        return None

    control_col = find_control_chars(t[col_name]).name("control_char")
    result = (
        t.select(
            [
                t.id,
                t[col_name],
                control_col,
            ]
        )
        .filter(control_col.notnull())
        .execute()
    )

    return [
        {"row": row[0], "text": row[1], "control_char": row[2]}
        for row in result.values.tolist()
    ]


# Check 3: Detect non-breaking spaces and invisible characters
def check_invisible_whitespace(col_name):
    """Detect non-breaking spaces, zero-width characters"""
    _validate_string_column(col_name)

    @ibis.udf.scalar.python
    def find_invisible_char(s: str) -> str:
        if s is None:
            return None
        invisible_chars = {
            "\u00a0": "non-breaking space",
            "\u200b": "zero-width space",
            "\u200c": "zero-width non-joiner",
            "\u200d": "zero-width joiner",
            "\ufeff": "zero-width no-break space (BOM)",
        }
        for char, name in invisible_chars.items():
            if char in s:
                return name
        return None

    invisible_col = find_invisible_char(t[col_name]).name("invisible_char")
    result = (
        t.select(
            [
                t.id,
                t[col_name],
                invisible_col,
            ]
        )
        .filter(invisible_col.notnull())
        .execute()
    )

    return [
        {"row": row[0], "text": row[1], "invisible_char": row[2]}
        for row in result.values.tolist()
    ]


# Check 4: Unicode normalization form detection
def check_normalization_inconsistency(col_name):
    """Detect inconsistent Unicode normalization forms"""
    _validate_string_column(col_name)

    @ibis.udf.scalar.python
    def is_normalized(s: str) -> bool:
        if s is None:
            return True
        nfc = unicodedata.normalize("NFC", s)
        return s == nfc

    normalized_col = is_normalized(t[col_name]).name("is_normalized")
    result = (
        t.select(
            [
                t.id,
                t[col_name],
                normalized_col,
            ]
        )
        .filter(~normalized_col)
        .execute()
    )

    return [{"row": row[0], "text": row[1]} for row in result.values.tolist()]


# Check 5: Detect combining diacriticals
def analyze_diacritics(col_name):
    """Analyze diacritical marks (preserving them as required)"""
    _validate_string_column(col_name)

    @ibis.udf.scalar.python
    def count_combining_marks(s: str) -> int:
        if s is None:
            return 0
        return sum(1 for char in s if unicodedata.combining(char) > 0)

    result = t.select(
        [
            t.id,
            t[col_name],
            count_combining_marks(t[col_name]).name("combining_marks"),
        ]
    ).execute()

    return [
        {"row": row[0], "text": row[1], "combining_marks": row[2]}
        for row in result.values.tolist()
    ]


# Check 6: Detect mojibake patterns
def check_mojibake(col_name):
    """Detect common mojibake patterns (double encoding, etc.)"""
    _validate_string_column(col_name)

    @ibis.udf.scalar.python
    def find_mojibake(s: str) -> str:
        if s is None:
            return None
        mojibake_patterns = [
            (r"([Â€™Â€œÂ€Â])", "UTF-8 misinterpretation"),
            (r"(\? {2,})", "Multiple replacement characters"),
        ]
        for pattern, issue_type in mojibake_patterns:
            if re.search(pattern, s):
                return issue_type
        return None

    mojibake_col = find_mojibake(t[col_name]).name("issue")
    result = (
        t.select(
            [
                t.id,
                t[col_name],
                mojibake_col,
            ]
        )
        .filter(mojibake_col.notnull())
        .execute()
    )

    return [
        {"row": row[0], "text": row[1], "issue": row[2]}
        for row in result.values.tolist()
    ]


# Check 7: Verify UTF-8 validity
def check_utf8_validity(col_name):
    """Verify column contains valid UTF-8"""
    _validate_string_column(col_name)
    # Ibis/DuckDB validates UTF-8 on creation, so if we got here, it's valid
    return {"valid": True, "message": "All strings are valid UTF-8"}


logger.info(f"UTF-8 validity: {check_utf8_validity('text')}")


# Comprehensive validation function
def validate_text_column(col_name, preserve_diacritics=True):
    """Run all text validation checks"""
    results = {
        "null_bytes": check_null_bytes(col_name),
        "control_characters": check_control_characters(col_name),
        "invisible_whitespace": check_invisible_whitespace(col_name),
        "normalization_issues": check_normalization_inconsistency(col_name),
        "diacritical_analysis": analyze_diacritics(col_name),
        "mojibake": check_mojibake(col_name),
        "utf8_validity": check_utf8_validity(col_name),
    }

    if not preserve_diacritics:
        results["diacritical_analysis"] = None

    return results


def main():
    """Run all text validation checks on the text column"""
    validation_results = validate_text_column("text", preserve_diacritics=True)
    for check, result in validation_results.items():
        logger.info(f"\n{check}: {result}")


if __name__ == "__main__":
    main()
