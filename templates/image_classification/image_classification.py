"""Image Data Ingestion Example for Image Classification.

This example demonstrates how to ingest image data from a CSV file into a database
for image classification tasks. It includes image resizing and metadata extraction,
supporting both binary data and file-based processing.
"""

import logging
import os
from typing import Dict, Any
from PIL import Image

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import TaskCategory, Intent, DataFormat, ImageExtension

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

# Initialize components
database = Database(config)


# Schema definition for segmentation data with constraints
schema = {
    "image_id": "VARCHAR(255) NOT NULL",
    "image_label": "VARCHAR(50) NOT NULL",
}

# Image specific options including CSV options
image_options = {
    # Image processing options
    "target_size": (256, 256),  # Resize images to this dimension
    "extension": ImageExtension.JPEG, # allowed extension for images: jpeg, jpg, png
}

# CSV specific options
csv_options = {
    "chunk_size": 1000,
    "delimiter": ",",
    "quotechar": '"',
    "escapechar": "\\",
}

# Image processing function for image classification tasks
def process_image(image_id: str, target_size: tuple = (256, 256)) -> bool:
    """Process a single image for classification tasks.
    
    Args:
        image_id: The ID of the image to process
        target_size: Target size for resizing the image
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    try:
        if not image_id:
            logger.error("No image_id provided")
            return False

        # Process the image
        image_src_path = os.path.join(config.SRC_PATH, "images", f"{image_id}.jpg")
        if not os.path.exists(image_src_path):
            logger.error(f"Source image not found: {image_src_path}")
            return False

        # Create destination directory if it doesn't exist
        os.makedirs(config.DEST_PATH, exist_ok=True)

        # Open and resize the image
        with Image.open(image_src_path) as img:
            # Resize the image
            resized_img = img.resize(target_size, Image.Resampling.LANCZOS)

            # Save the resized image
            image_dest_path = os.path.join(config.DEST_PATH, f"{image_id}.png")
            resized_img.save(image_dest_path, format=img.format)

            logger.info(f"Successfully processed image: {image_id}")
            return True

    except Exception as e:
        logger.error(f"Error processing image {image_id}: {str(e)}")
        return False

def main():
    """Run the image classification data ingestion example."""
    try:
     
        api_client = APIClient(config)
       

        # Create ingestor for image classification data with validators
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            data_format=DataFormat.IMAGE,
            category=TaskCategory.IMAGE_CLASSIFICATION,
            csv_options=csv_options,
            file_options=image_options,
            label_column="label",
            intent=Intent.TEST,  # Is the data for training or testing
            log_level=config.LOG_LEVEL
        )

        # Ingest data with validation
        logger.info("Starting image classification ingestion with data validation...")
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
        logger.error(f"{str(e)}")

if __name__ == "__main__":
    main() 