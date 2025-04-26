"""Census data ingestion pipeline using dlt.

This script defines a dlt pipeline that reads census data from parquet files,
cleans it using a transformer, and writes the cleaned data back to the filesystem.
"""

import logging
from collections.abc import Iterator  # Changed import
from typing import Any

import dlt
import pyarrow as pa
from dlt.destinations import filesystem as fs_destination
from dlt.sources.filesystem import filesystem, read_parquet

from helpers.generic import clean_data

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Define chunk size for processing
CHUNK_SIZE = 100000  # Adjust as needed based on memory constraints and performance


pipeline = dlt.pipeline(
    pipeline_name="census_pipeline",
    destination=fs_destination(bucket_url="files/output"),
    dataset_name="census",
    export_schema_path="files/schemas/export",
    progress="log",
)

census_source = filesystem(bucket_url="files/input") | read_parquet(chunksize=CHUNK_SIZE)


@dlt.transformer(data_from=census_source, table_name="census_clean", write_disposition="replace", file_format="parquet")
def census_clean(raw_data_chunk: list[dict[str, Any]]) -> Iterator[pa.Table]:  # Corrected input type hint
    """
    Cleans data received from the census_raw resource.

    Args:
        raw_data_chunk (list[dict[str, Any]]): A chunk of data (as a list of dictionaries)
                                              from the upstream resource.

    Yields:
        Iterator[pa.Table]: An iterator yielding cleaned PyArrow tables.
    """
    logger.debug(f"Received chunk of {len(raw_data_chunk)} records for cleaning.")
    if not raw_data_chunk:
        logger.debug("Received empty chunk, skipping cleaning.")
        # Optionally yield an empty table if downstream requires it, otherwise just return
        # yield pa.Table.from_pylist([])
        return

    try:
        # Convert the list of dictionaries to a PyArrow Table
        arrow_table = pa.Table.from_pylist(raw_data_chunk)
        logger.debug("Converted chunk to Arrow Table for cleaning.")

        # Apply the cleaning function
        cleaned_table = clean_data(arrow_table)
        logger.debug("Cleaning function applied successfully.")
        yield cleaned_table
    except Exception as e:
        logger.error("Error processing chunk in census_clean: %s", e, exc_info=True)
        # Decide how to handle errors: skip chunk, raise error, yield empty table?
        # Re-raising the exception to make the pipeline fail on error.
        raise e


logger.info("Starting census pipeline run...")
info = pipeline.run(census_clean)
logger.info("Pipeline run finished.")
logger.info(f"Pipeline run info: {info}")
