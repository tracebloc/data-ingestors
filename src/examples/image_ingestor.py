import sys
import os
import logging
from typing import Dict, Any, List

# Add the src directory to the PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.config import Config
from src.database import Database
from src.api.client import APIClient
from src.ingestors.image_ingestor import ImageIngestor
from src.processors.base import BaseProcessor
from src.utils.logging import setup_logging
from src.utils.constants import DataCategory, Intent

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

# Initialize components
database = Database(config)
api_client = APIClient(config)

# Schema definition for image metadata
schema = {
    "image_id": "VARCHAR(255)",
    "filename": "VARCHAR(255)",
    "width": "INT",
    "height": "INT",
    "format": "VARCHAR(50)",
    "label": "VARCHAR(255)",
    "confidence": "FLOAT",
    "notes": "TEXT"
}

# Image specific options
image_options = {
    "target_size": (224, 224),  # Resize images to this dimension
    "normalize": True,          # Normalize pixel values
    "formats": ["jpg", "jpeg", "png"],  # Supported image formats
    "recursive": True,          # Search subdirectories
}


# Define an image processor for basic transformations
class ImageResizeProcessor(BaseProcessor):
    def __init__(self, target_size: tuple = (224, 224)):
        self.target_size = target_size

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        # In a real implementation, this would resize the image
        # Here we're just updating metadata to reflect the resize
        if "width" in record and "height" in record:
            record["original_width"] = record["width"]
            record["original_height"] = record["height"]
            record["width"] = self.target_size[0]
            record["height"] = self.target_size[1]
            record["resized"] = True
        return record


# Create an instance of the processor
image_processor = ImageResizeProcessor(target_size=image_options["target_size"])

# Create ingestor for image data
ingestor = ImageIngestor(
    database=database,
    api_client=api_client,
    table_name=config.IMAGE_TABLE_NAME,
    schema=schema,
    category=DataCategory.IMAGE_CLASSIFICATION,
    image_options=image_options,
    processors=[image_processor],
    label_column="label",
    intent=Intent.TRAIN,  # Is the data for training or testing
    annotation_column="notes"
)

# Ingest data from a directory of images
with ingestor:
    # Assuming config.IMAGE_DIRECTORY contains path to folder with images
    # and config.IMAGE_METADATA_FILE contains a CSV with image metadata
    failed_records = ingestor.ingest(
        image_dir=config.IMAGE_DIRECTORY,
        metadata_file=config.IMAGE_METADATA_FILE,
        batch_size=config.BATCH_SIZE
    )
    
    if failed_records:
        print(f"Failed to process {len(failed_records)} images")
        for record in failed_records[:5]:  # Print first 5 failures
            print(f"Failed to process: {record.get('filename', 'unknown')}") 