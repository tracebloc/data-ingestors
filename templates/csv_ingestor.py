"""Basic CSV Ingestion Example.
This example demonstrates how to use the CSVIngestor to ingest data from a CSV file
into a database and optionally send it to an API.
"""

import logging
from pathlib import Path

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import TaskCategory, Intent, DataFormat

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

# Initialize components
database = Database(config)
api_client = APIClient(config)

def main():
    """Run the CSV ingestion example."""
    try:
        # Schema definition with constraints
        schema = {
            "name": "VARCHAR(255)",
            "age": "INT",
            "email": "VARCHAR(255)",
            "description": "VARCHAR(255)",
            "profile_image_url": "VARCHAR(512)",
            "notes": "TEXT"
        }

        # CSV specific options with additional configurations
        csv_options = {
            "chunk_size": 1000,
            "delimiter": ",",
            "quotechar": '"',
            "escapechar": "\\",
            "encoding": "utf-8",
            "on_bad_lines": "warn",
            "skip_blank_lines": True,
            "na_values": ["", "NA", "NULL", "None"]
        }

        # Create ingestor
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            data_format=DataFormat.TABULAR,
            category=TaskCategory.TABULAR_CLASSIFICATION,
            csv_options=csv_options,
            label_column="name",
            intent=Intent.TRAIN
        )    
        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(config.LABEL_FILE, batch_size=config.BATCH_SIZE)
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(f"Failed record: {record.get('name', 'Unknown')}")
                    logger.warning(f"Error details: {record.get('error', 'Unknown error')}")
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Error during CSV ingestion: {str(e)}")
        raise

if __name__ == "__main__":
    main() 