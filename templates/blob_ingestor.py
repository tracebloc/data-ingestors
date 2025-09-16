"""Blob Data Ingestion Example.

This example demonstrates how to ingest binary data (BLOBs) from a CSV file into a database
and optionally send it to an API. It handles base64 encoded binary data.
"""

import base64
import json
import logging
from pathlib import Path
from typing import Dict, Any

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import TaskCategory, Intent, DataFormat

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)


def main():
    """Run the blob ingestion example."""
    try:
        # Initialize components
        database = Database(config)
        api_client = APIClient(config)

        # Schema definition with BLOB/LONGBLOB fields
        schema = {
            "name": "VARCHAR(255)",
            "document_type": "VARCHAR(50)",
            "content_type": "VARCHAR(100)",
            "document_data": "LONGBLOB",  # For storing large binary objects
            "thumbnail": "BLOB",  # For storing smaller binary objects
            "metadata": "TEXT"  # For storing additional JSON metadata
        }

        # CSV specific options
        csv_options = {
            "chunk_size": 500,  # Smaller chunk size due to larger data
            "delimiter": ",",
            "quotechar": '"',
            "escapechar": "\\",
            "on_bad_lines": 'warn'  # Just warn about bad lines instead of failing
        }

        # Create blob data processor
        blob_processor = BlobDataProcessor(config=config, storage_path=config.STORAGE_PATH)

        # Create ingestor with blob processor
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            data_format=DataFormat.TEXT,
            category=TaskCategory.TEXT_CLASSIFICATION,
            intent=Intent.TRAIN,
            csv_options=csv_options,
        )

        # Get the example data path
        data_path = Path(__file__).parent / "data" / "text_classification_sample.csv"
        
        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(
                str(data_path),
                batch_size=25  # Smaller batch size due to larger data
            )
            
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(f"Failed record: {record.get('name', 'Unknown')}")
                    logger.warning(f"Error details: {record.get('error', 'Unknown error')}")
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 