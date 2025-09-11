"""Image Data Ingestion Example.

This example demonstrates how to ingest image data from a CSV file into a database
and optionally send it to an API. It includes metadata extraction,
supporting both binary data and file-based image processing.
"""

import logging
import os
from typing import Dict, Any
import shutil

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
from tracebloc_ingestor.processors.base import BaseProcessor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import TaskCategory, Intent, DataFormat

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

# Initialize components
database = Database(config)
api_client = APIClient(config)

class ImageProcessor(BaseProcessor):
    """Processor for handling image data in records.

    This processor resizes images to a target size while maintaining aspect ratio,
    and extracts image metadata. It supports both binary data and file-based processing.
    """

    def __init__(self):
        """Initialize the image processor.

        Args:
            config: Configuration object
        """
        self.config = Config()

        # Create destination directory if it doesn't exist
        os.makedirs(self.config.DEST_PATH, exist_ok=True)

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process image data in the record.

        Args:
            record: The record containing image data

        Returns:
            Processed record with resized image and metadata

        Raises:
            ValueError: If image data is invalid or processing fails
        """
        try:
            # Get the image_id and mask_id from the record
            image_id = record.get("image_id")
            if not image_id:
                logger.error("No image_id found in record")
                return record

            # Process the image
            image_src_path = os.path.join(self.config.SRC_PATH, f"{image_id}.png")
            if not os.path.exists(image_src_path):
                logger.error(f"Source image not found: {image_src_path}")
                return record

            # Save the resized image
            image_dest_path = os.path.join(self.config.DEST_PATH, f"{image_id}.png")
            # Copy file 
            shutil.copy(image_src_path, image_dest_path)

            logger.info(f"Successfully copied image: {image_id}")
            return record

        except Exception as e:
            raise ValueError(f"Error processing binary image: {str(e)}")


def main():
    """Run the image ingestion example."""
    try:

        # Schema definition for image data with constraints
        schema = {
            "image_id": "VARCHAR(255) NOT NULL",
            "image_label": "VARCHAR(50) NOT NULL",
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

        # Create image processor with storage path
        image_processor = ImageProcessor()

        # Create ingestor for segmentation data
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            category=TaskCategory.IMAGE_CLASSIFICATION,
            data_format=DataFormat.IMAGE,
            csv_options=csv_options,
            processors=[image_processor],
            label_column="image_label",
            intent=Intent.TRAIN,  # Is the data for training or testing
        )

        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(config.LABEL_FILE, batch_size=1000)
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(
                        f"Failed record: {record.get('image_id', 'Unknown')} - {record.get('mask_id', 'Unknown')}")
                    logger.warning(f"Error details: {record.get('error', 'Unknown error')}")
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()