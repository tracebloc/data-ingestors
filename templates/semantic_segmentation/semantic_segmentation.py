"""Semantic Segmentation Data Ingestion Example.

This example demonstrates how to ingest semantic segmentation data with images and
corresponding mask files into a database and optionally send it to an API. It processes
both the image files and their corresponding mask annotation files.
"""

import logging
from typing import Dict, Any

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import (
    TaskCategory,
    Intent,
    DataFormat,
    FileExtension,
)

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

# Schema definition for semantic segmentation
# mask_id column is required by the client to locate mask files
schema = {
    "mask_id": "VARCHAR(255)",
}

# Semantic segmentation specific options
semantic_segmentation_options = {
    "target_size": (512, 512),  # image size. Height = Width
    "extension": FileExtension.JPG,  # allowed extension for images: jpeg, jpg, png
}

# CSV specific options
csv_options = {
    "chunk_size": 100,  # Smaller chunk size due to larger data
    "delimiter": ",",
    "quotechar": '"',
    "escapechar": "\\",
    "on_bad_lines": "warn",
    "encoding": "utf-8",
}


def main():
    """Run the semantic segmentation ingestion example."""
    try:
        # Initialize components
        database = Database(config)
        api_client = APIClient(config)

        # Create ingestor for semantic segmentation data with validators
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            data_format=DataFormat.IMAGE,
            category=TaskCategory.SEMANTIC_SEGMENTATION,
            csv_options=csv_options,
            file_options=semantic_segmentation_options,
            label_column="image_label",
            unique_id_column="filename",
            intent=Intent.TRAIN,  # Is the data for training or testing
        )

        # Ingest data with validation
        logger.info("Starting semantic segmentation ingestion with data validation...")
        with ingestor:
            failed_records = ingestor.ingest(
                config.LABEL_FILE, batch_size=config.BATCH_SIZE
            )
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(
                        f"Failed record: {record.get('record', {}).get('filename', 'Unknown')}"
                    )
                    logger.warning(
                        f"Error details: {record.get('error', 'Unknown error')}"
                    )
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
