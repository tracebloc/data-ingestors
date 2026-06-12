"""CSV Ingestion Example.

This example demonstrates how to ingest data from a CSV file into a database
for time to event prediction tasks. It includes data validation, proper error handling,
and supports various CSV formats with comprehensive configuration options.
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
    """Run the time to event prediction data ingestion example."""

    # Initialize components
    database = Database(config)
    # Initialize API client
    api_client = APIClient(config)

    # Schema definition for tabular data
    schema = {
        "age": "FLOAT",
        "anaemia": "INT",
        "creatinine_phosphokinase": "FLOAT",
        "diabetes": "INT",
        "ejection_fraction": "FLOAT",
        "high_blood_pressure": "INT",
        "platelets": "FLOAT",
        "serum_creatinine": "FLOAT",
        "serum_sodium": "FLOAT",
        "sex": "INT",
        "smoking": "INT",
        "time": "INT",
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

    # Create ingestor for time to event prediction data with validators
    ingestor = CSVIngestor(
        database=database,
        api_client=api_client,
        table_name=config.TABLE_NAME,
        schema=schema,
        data_format=DataFormat.TABULAR,
        category=TaskCategory.TIME_TO_EVENT_PREDICTION,
        csv_options=csv_options,
        file_options={
            "number_of_columns": len(schema),
            "schema": schema,
            "time_column": "time",  # Specify the time column name
        },
        label_column="DEATH_EVENT",  # The event column is the target
        intent=Intent.TRAIN,  # Is the data for training or testing
    )

    # Ingest data with validation
    logger.info("Starting time to event prediction data ingestion with data validation...")
    run_ingestion(
        ingestor, config.LABEL_FILE, batch_size=config.BATCH_SIZE, logger=logger
    )


if __name__ == "__main__":
    main()
