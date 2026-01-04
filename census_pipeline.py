"""Census data ingestion pipeline using dlt.

This script defines a dlt pipeline that reads census data from parquet files,
cleans it using a transformer, and writes the cleaned data back to the filesystem.
"""

import logging
from collections.abc import Iterator

import dlt
import pyarrow as pa
from dlt.destinations import filesystem
from ingestion.helpers.duckdb import get_duckdb_data_arrow

from ingestion.helpers.standardise import clean_data

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Define chunk size for processing
CHUNK_SIZE = 100000  # Adjust as needed based on memory constraints and performance


@dlt.resource(table_name="census_source", write_disposition="replace")
def census_source() -> Iterator[pa.Table]:
    """
    Extracts census data from the specified source.

    Yields:
        Iterator[pa.Table]: An iterator yielding PyArrow tables.
    """
    logger.info("Starting to extract census data...")
    yield get_duckdb_data_arrow(
        db_path="data/input/census.ddb",
        duckdb_table_name="census",
        chunk_size=CHUNK_SIZE,
    )
    logger.info("Finished extracting census data.")


@dlt.transformer(
    data_from=census_source,
    table_name="census_clean",
    write_disposition="replace",
    file_format="parquet",
)
def census_clean(
    raw_data_chunk: pa.Table,
) -> Iterator[pa.Table]:  # Corrected input type hint
    """
    Cleans data received from the census_raw resource.

    Args:
        raw_data_chunk (pa.Table): A chunk of data (as a PyArrow Table)
                                   from the upstream resource.

    Yields:
        Iterator[pa.Table]: An iterator yielding cleaned PyArrow tables.
    """
    logger.debug(
        f"Received Arrow Table chunk with {raw_data_chunk.num_rows} records for cleaning."
    )
    if raw_data_chunk.num_rows == 0:  # Check for empty table using num_rows
        logger.debug("Received empty chunk, skipping cleaning.")
        # Optionally yield an empty table if downstream requires it, otherwise just return
        # yield pa.Table.from_pylist([]) # Keep this commented or remove if not needed
        return

    try:
        # No conversion needed, raw_data_chunk is already an Arrow Table
        logger.debug("Processing Arrow Table chunk for cleaning.")

        # Apply the cleaning function
        cleaned_table = clean_data(raw_data_chunk)  # Pass the table directly
        logger.debug("Cleaning function applied successfully.")
        yield cleaned_table
    except Exception as e:
        logger.error("Error processing chunk in census_clean: %s", e, exc_info=True)
        # Decide how to handle errors: skip chunk, raise error, yield empty table?
        # Re-raising the exception to make the pipeline fail on error.
        raise e


if __name__ == "__main__":
    # Initialise the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="census_pipeline",
        destination=filesystem(bucket_url="data/output"),
        # staging="files/staging",
        dataset_name="census",
        export_schema_path="data/schemas/export",
        progress="log",
        dev_mode=True,
    )

    # Run the pipeline
    logger.info("Starting census pipeline run...")
    info = pipeline.run(census_clean)
    logger.info("Pipeline run finished.")
    logger.info(f"Pipeline run info: {info}")
