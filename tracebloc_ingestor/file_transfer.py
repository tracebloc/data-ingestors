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
from tracebloc_ingestor.utils.constants import RESET, GREEN, RED

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


def image_transfer(record: Dict[str, Any], options: Dict[str, Any]) -> Dict[str, Any]:
   # Create destination directory if it doesn't exist
    os.makedirs(config.DEST_PATH, exist_ok=True)

    try:
        # Get the filename from the record
        filename = record.get("filename")
        data_id = record.get("data_id")
        extension = options.get("extension")
        if not filename:
            logger.error(f"{RED}No filename found in record{RESET}")
            return record

    
        # Add extension to filename if it doesn't have one
        if not os.path.splitext(filename)[1]:
            filename_with_ext = f"{filename}{extension}"
        else:
            filename_with_ext = filename

        # Process the image
        image_src_path = os.path.join(config.SRC_PATH, filename_with_ext)
        if not os.path.exists(image_src_path):
            logger.error(f"{RED}Source image not found: {image_src_path}{RESET}")
            return record

        # Save the resized image
        image_dest_path = os.path.join(config.DEST_PATH, f"{data_id}{extension}")
        # Copy file 
        shutil.copy(image_src_path, image_dest_path)

        logger.info(f"{GREEN}Successfully copied image: {filename}{RESET}")
        return record

    except Exception as e:
        raise ValueError(f"{RED}Error processing binary image: {str(e)}{RESET}")

def map_file_transfer(task_category: TaskCategory, record: Dict[str, Any], options: Dict[str, Any]) -> Dict[str, Any]:

    if task_category == TaskCategory.IMAGE_CLASSIFICATION:
        result = image_transfer(record, options)
        return result
    else:
        return None




