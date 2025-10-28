"""CSV Ingestion Example.

This example demonstrates how to ingest data from a CSV file into a database
for classification tasks. It includes data validation, proper error handling,
and supports various CSV formats with comprehensive configuration options.
"""

import logging

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import TaskCategory, Intent, DataFormat

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)


def main():
    """Run the tabular data ingestion example."""
    try:

        # Initialize components
        database = Database(config)
        # Initialize API client
        api_client = APIClient(config)

        # Schema definition for tabular data
        # schema should contain only feature columns only
        schema = {
            "feature_00": "FLOAT ",
            "feature_01": "FLOAT ",
            "feature_02": "FLOAT ",
            "feature_03": "FLOAT ",
            "feature_04": "FLOAT ",
            "feature_05": "FLOAT ",
            "feature_06": "FLOAT ",
            "feature_07": "FLOAT ",
            "feature_08": "FLOAT ",
            "feature_09": "FLOAT ",
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

        # Create ingestor for tabular data with validators
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            data_format=DataFormat.TABULAR,
            category=TaskCategory.TABULAR_CLASSIFICATION,
            csv_options=csv_options,
            file_options={"number_of_columns": len(schema)},
            label_column="name",
            intent=Intent.TRAIN,  # Is the data for training or testing
        )

        # Ingest data with validation
        logger.info("Starting tabular data ingestion with data validation...")
        with ingestor:
            failed_records = ingestor.ingest(
                config.LABEL_FILE, batch_size=config.BATCH_SIZE
            )
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(f"Failed record: {record.get('name', 'Unknown')}")
                    logger.warning(
                        f"Error details: {record.get('error', 'Unknown error')}"
                    )
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"{str(e)}")


if __name__ == "__main__":
    main()
