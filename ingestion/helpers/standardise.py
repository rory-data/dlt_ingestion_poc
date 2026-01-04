"""A collection of generic helper functions for data cleaning and processing within the dlt ingestion pipeline."""

import logging

import polars as pl
import polars.selectors as cs
import pyarrow as pa

# Configure logging
# It's generally recommended to configure logging at the application entry point,
# but placing it here for simplicity in this module context.
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def clean_data(arrow_data: pa.Table) -> pa.Table:
    """
    Cleans string columns within a PyArrow Table.

    This function performs the following cleaning steps on all string columns:
    1. Unicode Normalisation (NFC): Ensures consistent representation of characters.
    2. Control Character Removal: Removes non-printable control characters (Unicode category 'C').
    3. Whitespace Stripping: Removes leading and trailing whitespace.
    4. Casting to Utf8: Ensures the final data type is Polars Utf8.

    Args:
        arrow_data (pa.Table): The input PyArrow table containing data to be cleaned.

    Returns:
        pa.Table: A new PyArrow table with cleaned string columns.

    Raises:
        pl.PolarsError: If any Polars-specific error occurs during processing.
        Exception: For any other unexpected errors during the cleaning process.
    """
    if not isinstance(arrow_data, pa.Table):
        logger.error("Input data is not a PyArrow Table. Type: %s", type(arrow_data))
        raise TypeError("Input 'arrow_data' must be a pyarrow.Table.")

    # Check if any string columns exist to potentially skip conversion
    if not any(
        pa.types.is_string(field.type)
        or pa.types.is_large_string(field.type)
        or pa.types.is_string_view(field.type)
        for field in arrow_data.schema
    ):
        logger.info(
            "No string-like columns found in the input table. Skipping cleaning."
        )
        return arrow_data

    try:
        # Convert PyArrow Table to Polars DataFrame for efficient manipulation
        df = pl.DataFrame(arrow_data)
        logger.debug("Converted Arrow Table to Polars DataFrame. Shape: %s", df.shape)

        # Apply cleaning operations to all string columns
        df_cleaned = clean_string_columns(df)
        logger.debug(
            "String columns cleaned successfully. Shape remains: %s", df_cleaned.shape
        )

        schema = coerce_arrow_string_type(df_cleaned)

        return pa.table(df_cleaned, schema=schema)

    except pl.PolarsError as e:
        logger.error(
            "A Polars error occurred during data cleaning: %s", e, exc_info=True
        )
        # Re-raise the specific Polars error
        raise e
    except Exception as e:
        logger.error(
            "An unexpected error occurred during data cleaning: %s", e, exc_info=True
        )
        # Re-raise the caught exception
        raise e


def coerce_arrow_string_type(df: pl.DataFrame) -> pa.Schema:
    """
    Coerces the schema of a Polars DataFrame to a PyArrow schema with string types.

    This function maps Polars Utf8 columns to Arrow string type explicitly, while
    allowing Arrow to infer the types of other columns. This is used to avoid the
    Arrow string_view type, which is currently not supported by dlt.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.

    Returns:
        pa.Schema: The PyArrow schema with string types for Utf8 columns.

    Raises:
        TypeError: If the input is not a Polars DataFrame.
    """
    original_schema = pa.table(df).schema
    modified_fields = []
    needs_schema_change = False
    for field in original_schema:
        # Check if it's any Arrow string-like type that isn't already pa.string()
        if pa.types.is_large_string(field.type) or pa.types.is_string_view(field.type):
            logger.debug(
                "Modifying schema for column '%s' from %s to pa.string()",
                field.name,
                field.type,
            )
            modified_fields.append(
                pa.field(
                    field.name,
                    pa.string(),
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )
            needs_schema_change = True
        elif pa.types.is_string(field.type):
            # Already pa.string(), keep as is
            modified_fields.append(field)
        else:
            # Keep non-string fields as they are
            modified_fields.append(field)

    if needs_schema_change:
        logger.debug(
            "Created final Arrow schema enforcing pa.string() for string columns."
        )
        return pa.schema(modified_fields)
    else:
        logger.debug(
            "No schema modification needed. Returning table with original Arrow types (post-cleaning)."
        )
        return original_schema


def clean_string_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans string columns in a Polars DataFrame.

    This function performs the following cleaning steps on all string columns:
    1. Unicode Normalisation (NFC): Ensures consistent representation of characters.
    2. Control Character Removal: Removes non-printable control characters (Unicode category 'C').
    3. Whitespace Stripping: Removes leading and trailing whitespace.
    4. Casting to Utf8: Ensures the final data type is Polars Utf8.

    Args:
        df (pl.DataFrame): The input Polars DataFrame containing the columns to be cleaned.

    Returns:
        pl.DataFrame: A new Polars DataFrame with the cleaned string columns.

    Raises:
        pl.PolarsError: If any Polars-specific error occurs during processing.
    """
    # Apply cleaning operations to all string columns
    return df.with_columns(
        cs.string()
        .str.normalize("NFC")  # Normalise Unicode characters
        .str.replace_all(
            r"[\p{C}]", ""
        )  # Remove Unicode control characters (case-insensitive flag removed as it's not needed for \p{C})
        .str.strip_chars()  # Strip leading/trailing whitespace
    )
