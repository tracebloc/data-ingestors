"""Image Data Ingestion Example.

This example demonstrates how to ingest image data from a CSV file into a database
and optionally send it to an API. It includes metadata extraction,
supporting both binary data and file-based image processing.
"""

import logging
import os
from typing import Dict, Any
import shutil

from tracebloc_ingestor import Config
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import TaskCategory

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)


def image_transfer(record: Dict[str, Any], options: Dict[str, Any]) -> Dict[str, Any]:
   # Create destination directory if it doesn't exist
    os.makedirs(config.DEST_PATH, exist_ok=True)

    print(f"image_transfer record: {record}")

    try:
        # Get the filename from the record
        filename = record.get("filename")
        data_id = record.get("data_id")
        extension = options.get("extension")
        if not filename:
            logger.error("No filename found in record")
            return record

        # Process the image
        image_src_path = os.path.join(config.SRC_PATH, f"{filename}")
        if not os.path.exists(image_src_path):
            logger.error(f"Source image not found: {image_src_path}")
            return record

        # Save the resized image
        image_dest_path = os.path.join(config.DEST_PATH, f"{data_id}{extension}")
        # Copy file 
        shutil.copy(image_src_path, image_dest_path)

        logger.info(f"Successfully copied image: {filename}")
        return record

    except Exception as e:
        raise ValueError(f"Error processing binary image: {str(e)}")

def map_file_transfer(task_category: TaskCategory, record: Dict[str, Any], options: Dict[str, Any]) -> Dict[str, Any]:

    if task_category == TaskCategory.IMAGE_CLASSIFICATION:
        result = image_transfer(record, options)
        print(f"image_transfer result: {result}")
        exit(1)
        return result
    else:
        return None




