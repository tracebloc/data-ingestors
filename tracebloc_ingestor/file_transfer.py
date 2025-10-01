"""Image Data Ingestion Example.

This example demonstrates how to ingest image data from a CSV file into a database
and optionally send it to an API. It includes metadata extraction,
supporting both binary data and file-based image processing.
"""

import logging
import os
from typing import Dict, Any
import shutil
import time

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
from tracebloc_ingestor import Config
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import RETRY_MAX_ATTEMPTS, RETRY_WAIT_MULTIPLIER, RETRY_WAIT_MIN, RETRY_WAIT_MAX, TaskCategory
from tracebloc_ingestor.utils.constants import RESET, GREEN, RED

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

# Define retry decorator for file operations
retry_decorator = retry(
    stop=stop_after_attempt(RETRY_MAX_ATTEMPTS),
    wait=wait_exponential(
        multiplier=RETRY_WAIT_MULTIPLIER,
        min=RETRY_WAIT_MIN,
        max=RETRY_WAIT_MAX
    ),
    retry=retry_if_exception_type((OSError, IOError, shutil.Error)),
    before_sleep=before_sleep_log(logger, config.LOG_LEVEL),
    reraise=True
)


@retry_decorator
def _copy_file_with_retry(src_path: str, dest_path: str) -> None:
    """Copy file with retry logic for handling transient errors."""
    logger.debug(f"Attempting to copy file from {src_path} to {dest_path}")
    shutil.copy(src_path, dest_path)
    logger.debug(f"Successfully copied file from {src_path} to {dest_path}")


def _has_extension(filename: str) -> bool:
    """Check if filename has an extension, handling multiple dots correctly."""
    if not filename:
        return False
    
    # Split by dots and check if there's more than one part
    # and the last part is not empty (indicating a real extension)
    parts = filename.split('.')
    return len(parts) > 1 and parts[-1].strip() != ''


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
        if not _has_extension(filename):
            filename_with_ext = f"{filename}{extension}"
        else:
            filename_with_ext = filename

        # Process the image
        image_src_path = os.path.join(config.SRC_PATH, "images", filename_with_ext)
        if not os.path.exists(image_src_path):
            logger.error(f"{RED}Source image not found: {image_src_path}{RESET}")
            return record

        # Save the resized image
        image_dest_path = os.path.join(config.DEST_PATH, f"{data_id}{extension}")
        # Copy file with retry logic
        _copy_file_with_retry(image_src_path, image_dest_path)

        logger.info(f"{GREEN}Successfully copied image: {filename}{RESET}")
        return record

    except Exception as e:
        raise ValueError(f"{RED}Error processing binary image: {str(e)}{RESET}")



def annotation_transfer(record: Dict[str, Any], options: Dict[str, Any], extension: str) -> Dict[str, Any]:
   # Create destination directory if it doesn't exist
    os.makedirs(config.DEST_PATH, exist_ok=True)

    try:
        # Get the filename from the record
        filename = record.get("filename")
        data_id = record.get("data_id")
        extension = extension
        if not filename:
            logger.error(f"{RED}No filename found in record{RESET}")
            return record

    
        # Add extension to filename if it doesn't have one
        if not _has_extension(filename):
            filename_with_ext = f"{filename}{extension}"
        else:
            filename_with_ext = filename

        # Process the image
        file_src_path = os.path.join(config.SRC_PATH, "annotations", filename_with_ext)
        if not os.path.exists(file_src_path):
            logger.error(f"{RED}Source file not found: {file_src_path}{RESET}")
            return record

        # Save the file
        file_dest_path = os.path.join(config.DEST_PATH, f"{data_id}{extension}")
        # Copy file with retry logic
        _copy_file_with_retry(file_src_path, file_dest_path)

        logger.info(f"{GREEN}Successfully copied file: {filename}{RESET}")
        return record

    except Exception as e:
        raise ValueError(f"{RED}Error processing binary file: {str(e)}{RESET}")



def map_file_transfer(task_category: TaskCategory, record: Dict[str, Any], options: Dict[str, Any]) -> Dict[str, Any]:

    if task_category == TaskCategory.IMAGE_CLASSIFICATION:
        result = image_transfer(record, options)
        return result
    elif task_category == TaskCategory.OBJECT_DETECTION:
        result = image_transfer(record, options)
        result2 = annotation_transfer(record, options, ".xml")
        return result, result2
    else:
        return None




