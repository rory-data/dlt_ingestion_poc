"""Helper functions for interacting with DuckDB databases."""

import logging
from collections.abc import Iterator

import duckdb
import pyarrow as pa

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_duckdb_data_arrow(
    db_path: str,
    duckdb_table_name: str,
    chunk_size: int = 100000,
) -> Iterator[pa.Table]:
    """
    Fetches data from a DuckDB table in Arrow Table chunks.

    Args:
        db_path: Path to the DuckDB database file.
        duckdb_table_name: The DuckDB tabe to query against.
        chunk_size: Number of rows per Arrow Table chunk.

    Yields:
        Arrow Table chunks.
    """
    con = None  # Initialise con to None

    try:
        logger.info(f"Fetching data from DuckDB table '{duckdb_table_name}' at {db_path} in Arrow chunks...")
        con = duckdb.connect(database=db_path, read_only=True)

        # Execute a query and fetch as Arrow record batches (chunks)
        # Using con.table() for safer table referencing and fetch_arrow_reader()
        # directly on the relation object.
        relation = con.table(duckdb_table_name)
        arrow_batches = relation.fetch_arrow_reader(batch_size=chunk_size)

        # arrow_batches is an iterator of pyarrow.RecordBatch
        for batch in arrow_batches:
            # Convert each RecordBatch to an Arrow Table before yielding
            table = pa.Table.from_batches([batch])
            logger.debug(f"Yielding Arrow Table chunk with {table.num_rows} rows.")
            yield table

        logger.info("Finished fetching data from DuckDB table.")
    except Exception as e:
        logger.error(f"Error fetching data from DuckDB: {e}")
        raise
    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
