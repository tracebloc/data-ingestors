"""CSV Ingestion Example.

This example demonstrates how to ingest data from a CSV file into a database
for time series classification tasks (one multivariate sequence per entity ->
one label). It includes data validation, proper error handling, and supports
various CSV formats with comprehensive configuration options.
"""

import logging

from tracebloc_ingestor import (
    Config,
    Database,
    APIClient,
    CSVIngestor,
    run_ingestion,
)
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import TaskCategory, Intent, DataFormat

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)


def main():
    """Run the time series classification data ingestion example."""

    # Initialize components
    database = Database(config)
    # Initialize API client
    api_client = APIClient(config)

    # Schema definition for the sequence data. The two semantic columns are
    # FIXED names (backend#1054): `sequence_id` groups the timestep rows of
    # one sequence (VARCHAR — it is a key, not a feature); `timestamp` orders
    # the rows WITHIN each sequence (SQL TIMESTAMP or a numeric step index).
    # Every other column is a numeric feature (nulls allowed). The label
    # column is excluded here and supplied via label_column below.
    schema = {
        "sequence_id": "VARCHAR(64)",
        "timestamp": "TIMESTAMP",
        "heart_rate": "FLOAT",
        "resp_rate": "FLOAT",
        "temperature": "FLOAT",
        "spo2": "FLOAT",
        "lactate": "FLOAT",
    }

    # CSV specific options
    csv_options = {
        "chunk_size": 1000,
        "delimiter": ",",
        "quotechar": '"',
        "escapechar": "\\",
        "encoding": "utf-8",
        "on_bad_lines": "warn",
        "skip_blank_lines": True,
        "na_values": ["", "NA", "NULL", "None"],
    }

    # Create ingestor for time series classification data with validators
    ingestor = CSVIngestor(
        database=database,
        api_client=api_client,
        table_name=config.TABLE_NAME,
        schema=schema,
        data_format=DataFormat.TABULAR,
        category=TaskCategory.TIME_SERIES_CLASSIFICATION,
        csv_options=csv_options,
        file_options={
            "number_of_columns": len(schema),
            "schema": schema,
        },
        label_column="label",  # The per-sequence outcome column
        intent=Intent.TRAIN,  # Is the data for training or testing
    )

    # Ingest data with validation
    logger.info(
        "Starting time series classification data ingestion with data validation..."
    )
    run_ingestion(
        ingestor, config.LABEL_FILE, batch_size=config.BATCH_SIZE, logger=logger
    )


if __name__ == "__main__":
    main()
