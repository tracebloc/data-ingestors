"""Object Detection Data Ingestion Example.

This example demonstrates how to ingest object detection data with images and XML annotations
into a database and optionally send it to an API. It processes both the image files and their
corresponding XML annotation files from the VisDrone dataset format.
"""

import logging
import shutil
import xml.etree.ElementTree as ET
from typing import Dict, Any
import json
import os

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import TaskCategory, Intent, DataFormat, FileExtension

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

# Schema definition for object detection data
schema = {}

# Object detection specific options including CSV options
object_detection_options = {
    # Image processing options
    "target_size": (448, 448),  # Resize images to this dimension
    "extension": FileExtension.JPG,  # allowed extension for images: jpeg, jpg, png
}

# CSV specific options
csv_options = {
    "chunk_size": 100,  # Smaller chunk size due to larger data
    "delimiter": ",",
    "quotechar": '"',
    "escapechar": "\\",
    "on_bad_lines": 'warn',
    "encoding": "utf-8"
}

def main():
    """Run the object detection ingestion example."""
    try:
        # Initialize components
        database = Database(config)
        api_client = APIClient(config)
       
        # Create ingestor for object detection data with validators
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            data_format=DataFormat.IMAGE,
            category=TaskCategory.OBJECT_DETECTION,
            csv_options=csv_options,
            file_options=object_detection_options,
            label_column="image_label",
            intent=Intent.TRAIN  # Is the data for training or testing
        )

        # Ingest data with validation
        logger.info("Starting object detection ingestion with data validation...")
        with ingestor:
            failed_records = ingestor.ingest(config.LABEL_FILE, batch_size=config.BATCH_SIZE)
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(f"Failed record: {record.get('image_id', 'Unknown')}")
                    logger.warning(f"Error details: {record.get('error', 'Unknown error')}")
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 