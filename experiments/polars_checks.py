import sys
import unicodedata
from pathlib import Path
from typing import Literal

import chardet
import polars as pl
from loguru import logger

logger.add(
    sys.stderr,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | <level>{level: <8}</level> | <level>{message}</level> | {extra}",
    colorize=True,
)

# Create Polars DataFrame with UTF-8 strings
df = pl.DataFrame(
    {
        "id": [1, 2, 3, 4, 5, 6],
        "text": ["Kia ora", "Hello\x00World", "Café", "Tēst\u200b", "​Test", "Tëst"],
    }
)


# Check 1: Detect null bytes
def has_null_bytes(s: str) -> bool:
    return "\x00" in s if s is not None else False


df_with_checks = df.with_columns(
    pl.col("text")
    .map_elements(has_null_bytes, return_dtype=pl.Boolean)
    .alias("has_null_bytes")
)


# Check 2: Detect invisible whitespace
def has_invisible_chars(s: str) -> bool:
    invisible = {"\u00a0", "\u200b", "\u200c", "\u200d", "\ufeff"}
    return any(char in s for char in invisible) if s is not None else False


df_with_checks = df_with_checks.with_columns(
    pl.col("text")
    .map_elements(has_invisible_chars, return_dtype=pl.Boolean)
    .alias("has_invisible_chars")
)


# Check 3: Count diacritical marks
def count_diacritics(s: str) -> int:
    return (
        sum(1 for char in s if unicodedata.combining(char) > 0) if s is not None else 0
    )


df_with_checks = df_with_checks.with_columns(
    pl.col("text")
    .map_elements(count_diacritics, return_dtype=pl.UInt32)
    .alias("diacritic_count")
)


# Check 4: Check normalization form
def is_nfc_normalized(s: str) -> bool:
    if s is None:
        return True
    return s == unicodedata.normalize("NFC", s)


df_with_checks = df_with_checks.with_columns(
    pl.col("text")
    .map_elements(is_nfc_normalized, return_dtype=pl.Boolean)
    .alias("is_nfc_normalized")
)


# Check 5: Detect control characters
def has_control_chars(s: str) -> bool:
    if s is None:
        return False
    for char in s:
        code = ord(char)
        if (
            (0x00 <= code <= 0x08)
            or (0x0B <= code <= 0x0C)
            or (0x0E <= code <= 0x1F)
            or (0x7F <= code <= 0x9F)
        ):
            return True
    return False


df_with_checks = df_with_checks.with_columns(
    pl.col("text")
    .map_elements(has_control_chars, return_dtype=pl.Boolean)
    .alias("has_control_chars")
)

# Filter to show only problematic rows
quality_issues = df_with_checks.filter(
    (pl.col("has_null_bytes"))
    | (pl.col("has_invisible_chars"))
    | (pl.col("has_control_chars"))
    | (~pl.col("is_nfc_normalized"))
)

logger.info(quality_issues)

# Summary statistics
logger.info("\nValidation Summary:")
logger.info(
    df_with_checks.select(
        [
            pl.col("has_null_bytes").sum().alias("null_byte_count"),
            pl.col("has_invisible_chars").sum().alias("invisible_char_count"),
            pl.col("has_control_chars").sum().alias("control_char_count"),
            (~pl.col("is_nfc_normalized")).sum().alias("non_nfc_count"),
            pl.col("diacritic_count").mean().alias("avg_diacritics"),
        ]
    )
)

# STEP 1: Analyze original
logger.info("=== Before Normalization ===")
analysis_before = df.with_columns(
    pl.col("text")
    .map_elements(
        lambda s: s == unicodedata.normalize("NFC", s) if s else True,
        return_dtype=pl.Boolean,
    )
    .alias("is_nfc")
)
logger.info(analysis_before.filter(~pl.col("is_nfc")))
logger.info(f"Non-NFC rows: {(~analysis_before['is_nfc']).sum()}")


# STEP 2: Apply normalization
def normalize_nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s) if s is not None else s


df_normalized = df.with_columns(
    pl.col("text").map_elements(normalize_nfc, return_dtype=pl.Utf8).alias("text")
)

# STEP 3: Verify
logger.info("\n=== After Normalization ===")
analysis_after = df_normalized.with_columns(
    pl.col("text")
    .map_elements(
        lambda s: s == unicodedata.normalize("NFC", s) if s else True,
        return_dtype=pl.Boolean,
    )
    .alias("is_nfc")
)
logger.info(f"Non-NFC rows: {(~analysis_after['is_nfc']).sum()}")


# STEP 4: Diacritic preservation check
def count_combining(s: str) -> int:
    return sum(1 for c in s if unicodedata.combining(c) > 0) if s else 0


comparison = pl.DataFrame(
    {
        "original_text": df["text"],
        "normalized_text": df_normalized["text"],
        "original_marks": df["text"].map_elements(
            count_combining, return_dtype=pl.UInt32
        ),
        "normalized_marks": df_normalized["text"].map_elements(
            count_combining, return_dtype=pl.UInt32
        ),
    }
).with_columns(
    (pl.col("original_marks") == pl.col("normalized_marks")).alias("marks_preserved")
)

logger.info("\n=== Diacritic Preservation ===")
logger.info(comparison)
assert comparison["marks_preserved"].all(), "Diacritics were lost!"


# ============================================================================
# COMPREHENSIVE FAILED RECORDS REPORTING
# ============================================================================


def generate_failure_report(data: pl.DataFrame) -> dict:
    """Generate a detailed failure report for data quality issues.

    Args:
        data: DataFrame with validation check columns.

    Returns:
        Dictionary containing failure details and statistics.
    """
    failures: dict[str, list[dict]] = {
        "null_bytes": [],
        "invisible_chars": [],
        "control_chars": [],
        "nfc_normalization": [],
    }

    for row in data.iter_rows(named=True):
        if row["has_null_bytes"]:
            failures["null_bytes"].append(
                {
                    "id": row["id"],
                    "text": row["text"],
                    "severity": "HIGH",
                    "description": "Contains null bytes (\\x00)",
                    "remediation": "Remove null bytes using .replace('\\x00', '')",
                }
            )

        if row["has_invisible_chars"]:
            invisible_chars = []
            for char in row["text"]:
                if char in {"\u00a0", "\u200b", "\u200c", "\u200d", "\ufeff"}:
                    invisible_chars.append(
                        f"U+{ord(char):04X} ({unicodedata.name(char, 'UNKNOWN')})"
                    )

            failures["invisible_chars"].append(
                {
                    "id": row["id"],
                    "text": row["text"],
                    "severity": "MEDIUM",
                    "characters_found": ", ".join(invisible_chars),
                    "description": f"Contains invisible characters: {', '.join(invisible_chars)}",
                    "remediation": "Remove invisible characters or replace with visible equivalents",
                }
            )

        if row["has_control_chars"]:
            control_chars = []
            for char in row["text"]:
                code = ord(char)
                if (
                    (0x00 <= code <= 0x08)
                    or (0x0B <= code <= 0x0C)
                    or (0x0E <= code <= 0x1F)
                    or (0x7F <= code <= 0x9F)
                ):
                    control_chars.append(f"U+{code:04X}")

            failures["control_chars"].append(
                {
                    "id": row["id"],
                    "text": row["text"],
                    "severity": "HIGH",
                    "characters_found": ", ".join(control_chars),
                    "description": f"Contains control characters: {', '.join(control_chars)}",
                    "remediation": "Remove control characters using regex: re.sub(r'[\\x00-\\x1f\\x7f-\\x9f]', '', text)",
                }
            )

        if not row["is_nfc_normalized"]:
            failures["nfc_normalization"].append(
                {
                    "id": row["id"],
                    "text": row["text"],
                    "severity": "MEDIUM",
                    "description": "Not in NFC (Canonical Decomposition, followed by Canonical Composition)",
                    "remediation": "Normalize using unicodedata.normalize('NFC', text)",
                }
            )

    return failures


def print_failure_report(failures: dict) -> None:
    """Print a formatted failure report.

    Args:
        failures: Dictionary containing categorised failures.
    """
    logger.info("\n" + "=" * 80)
    logger.info("FAILED RECORDS REPORT")
    logger.info("=" * 80)

    total_issues = sum(len(v) for v in failures.values())

    if total_issues == 0:
        logger.info("✓ All records passed validation!")
        return

    logger.info(f"\nTotal Issues Found: {total_issues}\n")

    # Null Bytes Report
    if failures["null_bytes"]:
        logger.error(f"\n❌ NULL BYTES ({len(failures['null_bytes'])} records)")
        logger.error("-" * 80)
        for failure in failures["null_bytes"]:
            logger.error(
                f"  ID {failure['id']:3d} | Severity: {failure['severity']:6s}"
            )
            logger.error(f"           | Issue: {failure['description']}")
            logger.error(f"           | Fix:   {failure['remediation']}\n")

    # Invisible Characters Report
    if failures["invisible_chars"]:
        logger.warning(
            f"\n⚠️  INVISIBLE CHARACTERS ({len(failures['invisible_chars'])} records)"
        )
        logger.warning("-" * 80)
        for failure in failures["invisible_chars"]:
            logger.warning(
                f"  ID {failure['id']:3d} | Severity: {failure['severity']:6s}"
            )
            logger.warning(f"           | Found: {failure['characters_found']}")
            logger.warning(f"           | Issue: {failure['description']}")
            logger.warning(f"           | Fix:   {failure['remediation']}\n")

    # Control Characters Report
    if failures["control_chars"]:
        logger.error(
            f"\n❌ CONTROL CHARACTERS ({len(failures['control_chars'])} records)"
        )
        logger.error("-" * 80)
        for failure in failures["control_chars"]:
            logger.error(
                f"  ID {failure['id']:3d} | Severity: {failure['severity']:6s}"
            )
            logger.error(f"           | Found: {failure['characters_found']}")
            logger.error(f"           | Issue: {failure['description']}")
            logger.error(f"           | Fix:   {failure['remediation']}\n")

    # NFC Normalization Report
    if failures["nfc_normalization"]:
        logger.warning(
            f"\n⚠️  NFC NORMALIZATION ({len(failures['nfc_normalization'])} records)"
        )
        logger.warning("-" * 80)
        for failure in failures["nfc_normalization"]:
            logger.warning(
                f"  ID {failure['id']:3d} | Severity: {failure['severity']:6s}"
            )
            logger.warning(f"           | Issue: {failure['description']}")
            logger.warning(f"           | Fix:   {failure['remediation']}\n")

    # Summary by Severity
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY BY SEVERITY")
    logger.info("=" * 80)
    high_severity = sum(
        1 for v in failures.values() for item in v if item.get("severity") == "HIGH"
    )
    medium_severity = sum(
        1 for v in failures.values() for item in v if item.get("severity") == "MEDIUM"
    )

    logger.info(f"  HIGH:   {high_severity} issues (blocking)")
    logger.info(f"  MEDIUM: {medium_severity} issues (should address)")
    logger.info(f"  TOTAL:  {total_issues} issues\n")


# Generate and print the failure report
failures = generate_failure_report(df_with_checks)


def validate_string_columns(df: pl.DataFrame, text_cols: list[str]) -> pl.DataFrame:
    """Apply validation checks to specified string columns."""
    result = df
    for col in text_cols:
        result = (
            result.with_columns(
                pl.col(col)
                .map_elements(has_null_bytes, return_dtype=pl.Boolean)
                .alias(f"{col}_has_null_bytes")
            )
            .with_columns(
                pl.col(col)
                .map_elements(has_invisible_chars, return_dtype=pl.Boolean)
                .alias(f"{col}_has_invisible_chars")
            )
            .with_columns(
                pl.col(col)
                .map_elements(count_diacritics, return_dtype=pl.UInt32)
                .alias(f"{col}_diacritic_count")
            )
            .with_columns(
                pl.col(col)
                .map_elements(is_nfc_normalized, return_dtype=pl.Boolean)
                .alias(f"{col}_is_nfc_normalized")
            )
            .with_columns(
                pl.col(col)
                .map_elements(has_control_chars, return_dtype=pl.Boolean)
                .alias(f"{col}_has_control_chars")
            )
        )
    return result


def detect_encoding(file_path: Path) -> str:
    """Detect file encoding using chardet."""
    with open(file_path, "rb") as f:
        raw = f.read()
    result = chardet.detect(raw)
    encoding = result.get("encoding", "utf-8") or "utf-8"
    logger.info(
        f"  Detected encoding: {encoding} (confidence: {result.get('confidence', 0):.2%})"
    )
    return encoding


def identify_problematic_chars(s: str) -> dict:
    """Identify problematic characters in a string."""
    issues = {
        "null_bytes": [],
        "control_chars": [],
        "invisible_chars": [],
        "non_nfc": False,
    }

    if not s:
        return issues

    # Check for null bytes
    if "\x00" in s:
        issues["null_bytes"].append(("\\x00", "NULL BYTE"))

    # Check for control characters
    for char in s:
        code = ord(char)
        if (
            (0x00 <= code <= 0x08)
            or (0x0B <= code <= 0x0C)
            or (0x0E <= code <= 0x1F)
            or (0x7F <= code <= 0x9F)
        ):
            try:
                name = unicodedata.name(char, "UNNAMED")
            except ValueError:
                name = "UNNAMED"
            issues["control_chars"].append((f"U+{code:04X}", name))

    # Check for invisible characters
    invisible = {
        "\u00a0": "NO-BREAK SPACE",
        "\u200b": "ZERO-WIDTH SPACE",
        "\u200c": "ZERO-WIDTH NON-JOINER",
        "\u200d": "ZERO-WIDTH JOINER",
        "\ufeff": "ZERO-WIDTH NO-BREAK SPACE",
    }
    for char, name in invisible.items():
        if char in s:
            issues["invisible_chars"].append((f"U+{ord(char):04X}", name))

    # Check NFC normalization
    if s != unicodedata.normalize("NFC", s):
        issues["non_nfc"] = True

    return issues


def main() -> None:
    """Read and validate CSV files from inputs directory using PyArrow."""
    input_dir = Path("inputs")
    files = [
        input_dir / "input_bad.csv",
        input_dir / "input_cp1252.csv",
        input_dir / "input_latin1.csv",
    ]

    for file_path in files:
        logger.info(f"\n{'=' * 80}")
        logger.info(f"Processing: {file_path.name}")
        logger.info(f"{'=' * 80}")

        try:
            # Detect encoding
            encoding = detect_encoding(file_path)

            # Read CSV file using Polars with detected encoding
            df = pl.read_csv(file_path, encoding=encoding)
            logger.info(f"✓ Successfully read {file_path.name}")
            logger.info(f"  Columns: {df.columns}")
            logger.info(f"  Rows: {df.height}")

            # Validate string columns (name and city)
            string_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]
            if string_cols:
                df_validated = validate_string_columns(df, string_cols)

                # Filter for rows with any failures
                failure_cols = [
                    col
                    for col in df_validated.columns
                    if col.endswith("_has_null_bytes")
                    or col.endswith("_has_invisible_chars")
                    or col.endswith("_has_control_chars")
                    or col.endswith("_is_nfc_normalized")
                ]

                # Create a filter for any True value in failure columns
                has_issues = (
                    df_validated.select(failure_cols)
                    .to_pandas()
                    .apply(
                        lambda row: any(
                            row[col]
                            if col.endswith("_is_nfc_normalized")
                            else (not row[col])
                            for col in row.index
                        ),
                        axis=1,
                    )
                )

                # ... Actually, simpler approach - just check if any check column is True
                # except for is_nfc_normalized which is True when it's correct
                failed_rows = df_validated.with_columns(
                    pl.lit(False).alias("_has_failure")
                )

                for col in df_validated.columns:
                    if (
                        col.endswith("_has_null_bytes")
                        or col.endswith("_has_invisible_chars")
                        or col.endswith("_has_control_chars")
                    ):
                        failed_rows = failed_rows.with_columns(
                            (pl.col("_has_failure") | pl.col(col)).alias("_has_failure")
                        )
                    elif col.endswith("_is_nfc_normalized"):
                        failed_rows = failed_rows.with_columns(
                            (pl.col("_has_failure") | (~pl.col(col))).alias(
                                "_has_failure"
                            )
                        )

                failed_rows = failed_rows.filter(pl.col("_has_failure")).drop(
                    "_has_failure"
                )

                if len(failed_rows) > 0:
                    logger.info(
                        f"\n  ⚠️  Found {len(failed_rows)} rows with validation issues:"
                    )
                    logger.info(failed_rows)

                    # Show detailed character analysis for failures
                    logger.info("\n  Detailed Issue Analysis:")
                    for row in failed_rows.select(["id"] + string_cols).iter_rows(
                        named=True
                    ):
                        logger.info(f"\n    Row ID {row['id']}:")
                        for col in string_cols:
                            value = row[col]
                            if value:
                                issues = identify_problematic_chars(value)
                                has_any_issue = any(
                                    [
                                        issues["null_bytes"],
                                        issues["control_chars"],
                                        issues["invisible_chars"],
                                        issues["non_nfc"],
                                    ]
                                )
                                if has_any_issue:
                                    logger.info(f"      {col}: {value!r}")
                                    if issues["null_bytes"]:
                                        logger.info(
                                            f"        ❌ Null bytes: {issues['null_bytes']}"
                                        )
                                    if issues["control_chars"]:
                                        logger.info(
                                            f"        ❌ Control chars: {issues['control_chars']}"
                                        )
                                    if issues["invisible_chars"]:
                                        logger.info(
                                            f"        ⚠️  Invisible chars: {issues['invisible_chars']}"
                                        )
                                    if issues["non_nfc"]:
                                        logger.info("        ⚠️  Not NFC normalized")
                else:
                    logger.info("\n  ✓ All rows passed validation!")
            else:
                logger.info("  No string columns found to validate")

        except FileNotFoundError:
            logger.error(f"✗ File not found: {file_path}")
        except Exception as e:
            logger.error(f"✗ Error processing {file_path.name}: {e}")


if __name__ == "__main__":
    main()
