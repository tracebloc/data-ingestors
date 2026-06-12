"""Image Data Ingestion Example for Image Classification.

This example demonstrates how to ingest image data from a CSV file into a database
for image classification tasks. It includes image resizing and metadata extraction,
supporting both binary data and file-based processing.
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
    # Matches the bundled onboarding sample under data/images/ (#198).
    # Override per dataset when running against your own data.
    "target_size": (256, 256),  # image size. Height = Width
    "extension": FileExtension.JPEG,
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
    run_ingestion(
        ingestor, config.LABEL_FILE, batch_size=config.BATCH_SIZE, logger=logger
    )


if __name__ == "__main__":
    main()
