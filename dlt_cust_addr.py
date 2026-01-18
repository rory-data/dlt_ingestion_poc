"""Clean DuckDB-based pipe-delimited text file source implementation.

Efficiently reads standard pipe-delimited text files using DuckDB's streaming
Arrow integration, applies validation, and loads to dlt.

This pipeline is designed to extract raw source data during extract runs for a given batch
cycle; replay runs never re-read the source, persist it as Arrow-backed Parquet staging
data managed by dlt, and then replay the transformer from that staging layer for any
subsequent reruns without re-extracting from the source.

Execution modes:

- extract (default):
   Reads source data and appends to raw staging.
   Safe to rerun; Airflow retries will re-extract.

- replay:
   Skips source extraction entirely.
   Replays validation and loading from cached raw data.
   Intended for schema evolution fixes or data issue remediation.
"""

from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import dlt
import pyarrow as pa
import pyarrow.compute as pc
from dlt.destinations import filesystem
from loguru import logger

from ingestion.core.reporting import state
from ingestion.core.validate.validator import ArrowValidator
from ingestion.engines.dlt import RunMode, operations, transformers
from ingestion.io import duckdb
from ingestion.schema.odcs import get_dlt_schemas, get_table_requirements
from ingestion.utils import logging


@dlt.source
def dlt_customer_address(
    table_name: str,
    batch_date: str,
    file_path: str | Path,
    table_schema: dict[str, Any],
    validator: ArrowValidator,
    batch_size: int = 50_000,
    run_mode: RunMode = RunMode.EXTRACT,
):
    """Load and validate customer address data.

    Args:
        table_name: Name of the table to load.
        batch_date: Batch date identifier (YYYYMMDD format).
        file_path: Path to the source data file.
        table_schema: Schema definition including columns, keys, and references.
        validator: Data quality validator instance.
        batch_size: Number of rows to process per batch.
        run_mode: Execution mode (extract or replay).
    """

    # ---- RAW EXTRACT (IMMUTABLE CACHE) ----
    @dlt.resource(
        name=f"raw_{table_name}",
        write_disposition="append",
        columns=[
            {**col, "nullable": True}
            for col in table_schema["columns"]
            if col["name"] != "dlt_batch_date"
        ]
        + [
            {
                "name": "dlt_batch_date",
                "data_type": "text",
                "nullable": False,
            },
        ],
    )
    def extract_and_cache() -> Iterator[pa.Table]:
        if run_mode == RunMode.REPLAY:
            logger.info(f"Replay mode — skipping raw extract for {batch_date}")
            yield from ()
            return

        logger.info(f"Extracting raw data for batch {batch_date}")

        for batch in duckdb.stream_csv_to_arrow(
            file_path,
            batch_size=batch_size,
            header=True,
        ):
            table = duckdb.rename_csv_columns(
                pa.Table.from_batches([batch]),
                table_schema["column_names"],
            )

            table = table.append_column(
                "dlt_batch_date",
                pa.array([batch_date] * table.num_rows, pa.string()),
            )

            yield dlt.mark.with_hints(
                table,
                {"partition": ["dlt_batch_date"]},
            )

    @dlt.transformer(
        data_from=extract_and_cache,
        name=f"staged_{table_name}",
        write_disposition="replace",
    )
    def staged_validate(raw: pa.Table) -> Iterator[pa.Table]:
        batch = raw.filter(pc.equal(raw["dlt_batch_date"], pa.scalar(batch_date)))

        if run_mode == RunMode.REPLAY and batch.num_rows == 0:
            raise RuntimeError(
                f"Replay requested for {batch_date}, but no raw data found"
            )

        if batch.num_rows == 0:
            yield from ()
            return

        clean, bad, issues = validator.validate(batch)

        state.update_validation_state(
            issues,
            batch.num_rows,
            bad.num_rows,
        )

        # Emit quarantine if there are bad records
        if bad.num_rows > 0:
            yield dlt.mark.with_table_name(
                bad,
                f"quarantine_{table_name}",
            )

        if clean.num_rows:
            yield clean

    # ---- VALIDATION / FINAL TABLE ----
    @dlt.transformer(
        data_from=staged_validate,
        name=table_name,
        write_disposition="replace",
        primary_key=table_schema["primary_keys"],
        schema_contract="evolve",
        references=table_schema["references"],
    )
    def load_file(staged: pa.Table) -> Iterator[pa.Table]:
        casted, valid = transformers.cast_columns_to_dlt_types(
            staged,
            table_schema["columns"],
        )

        if not valid:
            yield dlt.mark.with_table_name(
                staged,
                f"quarantine_{table_name}",
            )
            return

        yield transformers.create_casted_table(
            casted,
            staged.schema.names,
        )

    # ---- QUARANTINE TABLE (DECLARATION) ----
    @dlt.resource(
        name=f"quarantine_{table_name}",
        write_disposition="append",
        columns=table_schema["columns"],
    )
    def quarantine_declaration() -> Iterator[Any]:
        """Placeholder for quarantined data schema discovery."""
        yield from ()

    return extract_and_cache, staged_validate, load_file, quarantine_declaration


# ----------------------------------------------------------------------
# Entrypoint (local / Airflow KPO compatible)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    logging.setup_logger("DEBUG")

    FILE_PATH = Path("data/input/customer_address.txt")
    CONTRACT_PATH = "data/schemas/import/customer_address_odcs.yaml"
    TABLE_NAME = "customer_address"
    BATCH_DATE = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

    FILE_PATH = operations.validate_source_file(FILE_PATH)
    schemas = get_dlt_schemas(CONTRACT_PATH)
    table_requirements = get_table_requirements(schemas, TABLE_NAME)

    # Add dlt_batch_date to requirements so it's handled by transformers
    table_requirements["columns"].append(
        {
            "name": "dlt_batch_date",
            "data_type": "text",
            "nullable": False,
        }
    )
    table_requirements["column_names"].append("dlt_batch_date")

    validator = operations.setup_validator(issue_action="quarantine")

    pipeline = dlt.pipeline(
        pipeline_name="customer_address_pipeline_fs",
        destination=filesystem(
            bucket_url=f"file://{Path.cwd()}/data/output/dlt_storage"
        ),
        dataset_name="customer_address_data",
    )

    operations.run_pipeline_with_summary(
        pipeline,
        dlt_customer_address(
            table_name=TABLE_NAME,
            batch_date=BATCH_DATE,
            file_path=FILE_PATH,
            table_schema=table_requirements,
            validator=validator,
        ),
        pipeline_description="customer address",
    )
