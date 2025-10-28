"""Image Data Ingestion Example for Image Classification.

This example demonstrates how to ingest image data from a CSV file into a database
for image classification tasks. It includes image resizing and metadata extraction,
supporting both binary data and file-based processing.
"""

import logging

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

# Image specific options including CSV options
image_options = {
    # Image processing options
    "target_size": (512, 512),  # Define image size. Height = Width
    "extension": FileExtension.JPG,  # allowed extension for images: jpeg, jpg, png
}

# CSV specific options
csv_options = {
    "chunk_size": 1000,
    "delimiter": ",",
    "quotechar": '"',
    "escapechar": "\\",
}


def main():
    """Run the image classification data ingestion example."""
    try:

        # Initialize components
        database = Database(config)
        # Initialize API client
        api_client = APIClient(config)

        # Create ingestor for image classification data with validators
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            data_format=DataFormat.IMAGE,
            category=TaskCategory.IMAGE_CLASSIFICATION,
            csv_options=csv_options,
            file_options=image_options,
            label_column="label",
            intent=Intent.TEST,  # Is the data for training or testing
        )

        # Ingest data with validation
        logger.info("Starting image classification ingestion with data validation...")
        with ingestor:
            failed_records = ingestor.ingest(
                config.LABEL_FILE, batch_size=config.BATCH_SIZE
            )
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(
                        f"Failed record: {record.get('image_id', 'Unknown')}"
                    )
                    logger.warning(
                        f"Error details: {record.get('error', 'Unknown error')}"
                    )
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"{str(e)}")


if __name__ == "__main__":
    main()
