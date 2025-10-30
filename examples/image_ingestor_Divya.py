"""Image and Mask Data Ingestion Example for Segmentation.

This example demonstrates how to ingest image and mask data from a CSV file into a database
for segmentation tasks. It includes image and mask resizing and metadata extraction,
supporting both binary data and file-based processing.
"""

import logging
import os
from typing import Dict, Any
from PIL import Image

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

# Schema definition for segmentation data with constraints
schema = {
    "image_id": "VARCHAR(255) NOT NULL",
    "mask_id": "VARCHAR(255) NOT NULL",
    "image_label": "VARCHAR(50) NOT NULL",
}

# Image specific options including CSV options
image_options = {
    # Image processing options
    "target_size": (256, 256),  # Resize images to this dimension
}

# CSV specific options
csv_options = {
    "chunk_size": 1000,
    "delimiter": ",",
    "quotechar": '"',
    "escapechar": "\\",
}


# Define an image and mask processor for segmentation tasks
class SegmentationProcessor(BaseProcessor):
    def __init__(self, target_size: tuple = (256, 256)):
        self.target_size = target_size
        self.config = Config()

        # Create destination directory if it doesn't exist
        os.makedirs(self.config.DEST_PATH, exist_ok=True)

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # Get the image_id and mask_id from the record
            image_id = record.get("image_id")
            mask_id = record.get("mask_id")

            if not image_id:
                logger.error("No image_id found in record")
                return record
            if not mask_id:
                logger.error("No mask_id found in record")
                return record

            # Process the image
            image_src_path = os.path.join(
                self.config.SRC_PATH, "images", f"{image_id}.jpg"
            )
            if not os.path.exists(image_src_path):
                logger.error(f"Source image not found: {image_src_path}")
                return record

            # Process the mask
            mask_src_path = os.path.join(
                self.config.SRC_PATH, "masks", f"{mask_id}.png"
            )
            if not os.path.exists(mask_src_path):
                logger.error(f"Source mask not found: {mask_src_path}")
                return record

            # Open and resize the image
            with Image.open(image_src_path) as img:
                # Resize the image
                resized_img = img.resize(self.target_size, Image.Resampling.LANCZOS)

                # Save the resized image
                image_dest_path = os.path.join(self.config.DEST_PATH, f"{image_id}.png")
                resized_img.save(image_dest_path, format=img.format)

                logger.info(f"Successfully processed image: {image_id}")

            # Open and resize the mask
            with Image.open(mask_src_path) as mask:
                # Resize the mask to match the image size
                resized_mask = mask.resize(self.target_size, Image.Resampling.LANCZOS)

                # Save the resized mask
                mask_dest_path = os.path.join(self.config.DEST_PATH, f"{mask_id}.png")
                resized_mask.save(mask_dest_path, format=mask.format)

                logger.info(f"Successfully processed mask: {mask_id}")

        except Exception as e:
            logger.error(
                f"Error processing image {image_id} and mask {mask_id}: {str(e)}"
            )

        return record


def main():
    """Run the segmentation data ingestion example."""
    try:
        # Create an instance of the processor
        segmentation_processor = SegmentationProcessor(
            target_size=image_options["target_size"]
        )

        # Create ingestor for segmentation data
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            data_format=DataFormat.IMAGE,
            category=TaskCategory.SEMANTIC_SEGMENTATION,
            csv_options=csv_options,
            processors=[segmentation_processor],
            label_column="image_label",
            intent=Intent.TEST,  # Is the data for training or testing
        )

        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(config.LABEL_FILE, batch_size=1000)
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(
                        f"Failed record: {record.get('image_id', 'Unknown')} - {record.get('mask_id', 'Unknown')}"
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
