"""Image Data Ingestion Example.

This example demonstrates how to ingest image data from a CSV file into a database
and optionally send it to an API. It includes image resizing and metadata extraction,
supporting both binary data and file-based image processing.
"""

import logging
import os
from typing import Dict, Any
from PIL import Image

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
from tracebloc_ingestor.processors.base import BaseProcessor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import DataCategory, Intent

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

# Initialize components
database = Database(config)
api_client = APIClient(config)

# Schema definition for image metadata
schema = {
    "filename": "VARCHAR(255)",
    "width": "INT",
    "height": "INT",
    "format": "VARCHAR(50)",
    "notes": "TEXT"
}

# Image specific options including CSV options
image_options = {
    # Image processing options
    "target_size": (224, 224),  # Resize images to this dimension
}

# CSV specific options
csv_options = {
    "chunk_size": 1000,
    "delimiter": ",",
    "quotechar": '"',
    "escapechar": "\\",
}


# Define an image processor for basic transformations
class ImageResizeProcessor(BaseProcessor):
    def __init__(self, target_size: tuple = (224, 224)):
        self.target_size = target_size
        self.config = Config()

        # Create destination directory if it doesn't exist
        os.makedirs(self.config.DEST_PATH, exist_ok=True)

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # Get the source image path
            filename = record.get("filename")
            if not filename:
                logger.error("No filename found in record")
                return record

            src_path = os.path.join(self.config.SRC_PATH, filename)
            if not os.path.exists(src_path):
                logger.error(f"Source image not found: {src_path}")
                return record

            # Open and resize the image
            with Image.open(src_path) as img:
                # Resize the image
                resized_img = img.resize(self.target_size, Image.Resampling.LANCZOS)

                # Update record with new dimensions
                record["width"] = self.target_size[0]
                record["height"] = self.target_size[1]
                record["format"] = img.format

                # Save the resized image
                dest_path = os.path.join(self.config.DEST_PATH, filename)
                resized_img.save(dest_path, format=img.format)

                logger.info(f"Successfully processed image: {filename}")

        except Exception as e:
            logger.error(f"Error processing image {filename}: {str(e)}")

        return record

def main():
    """Run the image ingestion example."""
    try:
        # Create an instance of the processor
        image_processor = ImageResizeProcessor(target_size=image_options["target_size"])

        # Create ingestor for image data
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            category=DataCategory.IMAGE_CLASSIFICATION,
            csv_options=csv_options,
            processors=[image_processor],
            label_column="label",
            intent=Intent.TRAIN,  # Is the data for training or testing
        )

        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(config.LABEL_FILE, batch_size=config.BATCH_SIZE)
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(f"Failed record: {record.get('filename', 'Unknown')}")
                    logger.warning(f"Error details: {record.get('error', 'Unknown error')}")
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()