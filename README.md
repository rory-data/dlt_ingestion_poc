# dlt-ingestion-poc

This is a project for experimentation using dlt for data ingestion in an ETL pipeline. It uses a file of NZ Census data (comprised of 17 cols x 35.6m records) in Parquet format as the source and applies a custom cleaning function to the data before writing it back to the filesystem.

The project is structured to allow for easy modification and testing of different data ingestion and transformation strategies.

## Features

- Ingests census data from Parquet files located in the `files/input` directory.
- Cleans the ingested data using a custom cleaning function (`helpers.generic.clean_data`).
- Writes the cleaned data back to the filesystem (configured via `dlt.destinations.filesystem`).
- Exports the inferred schema to `files/schemas/export`.
- Uses `dlt` for pipeline orchestration and data loading.
- Uses `pyarrow` for efficient data handling.

## Setup

This project uses `uv` for dependency management.

1.  **Install `uv`**: If you don't have `uv` installed, follow the instructions [here](https://github.com/astral-sh/uv).
2.  **Create a virtual environment**:
    ```bash
    uv venv
    ```
3.  **Activate the virtual environment**:
    - macOS/Linux: `source .venv/bin/activate`
    - Windows: `.venv\Scripts\activate`
4.  **Install dependencies**:
    ```bash
    uv sync
    ```
    This installs the project in editable mode along with development dependencies.

## Usage

To run the main data ingestion pipeline:

```bash
python census_pipeline.py
```

The pipeline will:

1.  Read Parquet files from `files/input/`.
2.  Apply the `census_clean` transformer.
3.  Write the cleaned data (in Parquet format) to the destination configured in the pipeline (defaulting to a local filesystem location managed by `dlt`).
4.  Log progress to the console.
