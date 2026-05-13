"""File Transfer Module.

This example demonstrates how to ingest image data from a CSV file into a database
and optionally send it to an API. It includes metadata extraction,
supporting both binary data and file-based image processing.
"""

import logging
import os
from typing import Dict, Any
import shutil
import time

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from tracebloc_ingestor import Config
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import (
    RETRY_MAX_ATTEMPTS,
    RETRY_WAIT_MULTIPLIER,
    RETRY_WAIT_MIN,
    RETRY_WAIT_MAX,
    TaskCategory,
    FileExtension,
)
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
        multiplier=RETRY_WAIT_MULTIPLIER, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX
    ),
    retry=retry_if_exception_type((OSError, IOError, shutil.Error)),
    before_sleep=before_sleep_log(logger, config.LOG_LEVEL),
    reraise=True,
)


@retry_decorator
def _copy_file_with_retry(src_path: str, dest_path: str) -> None:
    """Copy file with retry logic for handling transient errors."""
    logger.debug(f"Attempting to copy file from {src_path} to {dest_path}")

    # Remove destination file if it exists to avoid conflicts
    if os.path.exists(dest_path):
        logger.debug(f"Destination file exists, removing: {dest_path}")
        os.remove(dest_path)

    shutil.copy(src_path, dest_path)
    logger.debug(f"Successfully copied file from {src_path} to {dest_path}")


def _has_extension(filename: str) -> bool:
    """Check if filename has an extension, handling multiple dots correctly."""
    if not filename:
        return False

    allowed_extensions = FileExtension.get_all_extensions()
    parts = filename.split(".")
    if len(parts) > 1:
        ext = parts[len(parts) - 1]
        return ext in allowed_extensions
    return False


def _find_image_src(filename: str, extension: str):
    """Resolve the source path for an image in SRC_PATH/images/.

    Returns (src_path, filename_with_ext) on success, or
    (None, filename_with_ext) if the file does not exist. Centralising the
    path/extension resolution keeps pre-checks (e.g. the atomic
    SEMANTIC_SEGMENTATION branch in `map_file_transfer`) consistent with
    what `image_transfer` actually copies.
    """
    filename_with_ext = (
        filename if _has_extension(filename) else f"{filename}{extension}"
    )
    candidate = os.path.join(config.SRC_PATH, "images", filename_with_ext)
    if os.path.exists(candidate):
        return candidate, filename_with_ext
    return None, filename_with_ext


def image_transfer(record: Dict[str, Any], options: Dict[str, Any]) -> Dict[str, Any]:
    # Create destination directory if it doesn't exist
    os.makedirs(config.DEST_PATH, exist_ok=True)

    try:
        # Get the filename from the record
        filename = record.get("filename")
        extension = options.get("extension")
        if not filename:
            logger.error(f"{RED}No filename found in record{RESET}")
            return record

        image_src_path, filename_with_ext = _find_image_src(filename, extension)
        if image_src_path is None:
            logger.error(
                f"{RED}Source image not found: {os.path.join(config.SRC_PATH, 'images', filename_with_ext)}{RESET}"
            )
            return record

        # Save the resized image
        image_dest_path = os.path.join(config.DEST_PATH, filename_with_ext)
        # Copy file with retry logic
        _copy_file_with_retry(image_src_path, image_dest_path)

        record["filename"] = os.path.splitext(filename_with_ext)[0]
        record["extension"] = extension

        logger.info(f"{GREEN}Successfully copied image: {filename}{RESET}")
        return record

    except Exception as e:
        raise ValueError(f"{RED}Error processing binary image: {str(e)}{RESET}")


"""
Row: id, data_id, filename, extension, label, intent, ingestor_id
filename: file_name.png (or any other extension) file_name.xml

"""


def annotation_transfer(
    record: Dict[str, Any], options: Dict[str, Any], extension: str
) -> Dict[str, Any]:
    # Create destination directory if it doesn't exist
    os.makedirs(config.DEST_PATH, exist_ok=True)

    try:
        # Get the filename from the record
        filename = record.get("filename")
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
        file_dest_path = os.path.join(config.DEST_PATH, filename_with_ext)
        # Copy file with retry logic
        _copy_file_with_retry(file_src_path, file_dest_path)

        logger.info(f"{GREEN}Successfully copied file: {filename}{RESET}")
        return record

    except Exception as e:
        raise ValueError(f"{RED}Error processing binary file: {str(e)}{RESET}")


def text_transfer(record: Dict[str, Any], options: Dict[str, Any]) -> Dict[str, Any]:
    """Transfer text files for text classification tasks.

    Args:
        record: Dictionary containing filename and other record data
        options: Dictionary containing transfer options like extension

    Returns:
        Updated record dictionary
    """
    # Create destination directory if it doesn't exist
    os.makedirs(config.DEST_PATH, exist_ok=True)

    try:
        # Get the filename from the record
        filename = record.get("filename")
        extension = options.get("extension")
        if not filename:
            logger.error(f"{RED}No filename found in record{RESET}")
            return record

        # Add extension to filename if it doesn't have one
        if not _has_extension(filename):
            filename_with_ext = f"{filename}{extension}"
        else:
            filename_with_ext = filename

        # Process the text file
        text_src_path = os.path.join(config.SRC_PATH, "texts", filename_with_ext)
        if not os.path.exists(text_src_path):
            logger.error(f"{RED}Source text file not found: {text_src_path}{RESET}")
            return record

        # Save the text file
        text_dest_path = os.path.join(config.DEST_PATH, filename_with_ext)
        # Copy file with retry logic
        _copy_file_with_retry(text_src_path, text_dest_path)

        record["filename"] = os.path.splitext(filename_with_ext)[0]
        record["extension"] = extension

        logger.info(f"{GREEN}Successfully copied text file: {filename}{RESET}")
        return record

    except Exception as e:
        raise ValueError(f"{RED}Error processing text file: {str(e)}{RESET}")


def _find_mask_src(mask_id: str):
    """Locate a mask file in SRC_PATH/masks/, trying common image extensions.

    Returns (src_path, extension, mask_name) on success, or (None, None, mask_name)
    if no matching file is found.
    """
    mask_name = mask_id.split(".")[0] if "." in mask_id else mask_id
    for ext in [".png", ".jpg", ".jpeg"]:
        candidate = os.path.join(config.SRC_PATH, "masks", f"{mask_name}{ext}")
        if os.path.exists(candidate):
            return candidate, ext, mask_name
    return None, None, mask_name


def mask_transfer(
    record: Dict[str, Any],
    mask_src_path: str,
    mask_ext: str,
    mask_name: str,
) -> Dict[str, Any]:
    """Copy a pre-resolved mask file from SRC_PATH/masks/ to DEST_PATH/.

    The caller is responsible for locating the mask via `_find_mask_src` and
    passing the resolved path; this keeps the filesystem lookup to a single
    call per record.
    """
    os.makedirs(config.DEST_PATH, exist_ok=True)

    try:
        mask_dest_path = os.path.join(config.DEST_PATH, f"{mask_name}{mask_ext}")
        _copy_file_with_retry(mask_src_path, mask_dest_path)

        logger.info(f"{GREEN}Successfully copied mask: {mask_name}{RESET}")
        return record

    except Exception as e:
        raise ValueError(f"{RED}Error processing mask file: {str(e)}{RESET}")


def map_file_transfer(
    task_category: TaskCategory, record: Dict[str, Any], options: Dict[str, Any]
) -> Dict[str, Any]:
    """Map file transfer function based on task category.

    Args:
        task_category: The type of task (IMAGE_CLASSIFICATION, OBJECT_DETECTION, TEXT_CLASSIFICATION, etc.)
        record: Dictionary containing filename, data_id, and other record data
        options: Dictionary containing transfer options

    Returns:
        Updated record dictionary or tuple of results for multi-file tasks
    """
    if task_category == TaskCategory.IMAGE_CLASSIFICATION:
        result = image_transfer(record, options)
        return result
    elif task_category == TaskCategory.OBJECT_DETECTION:
        record = image_transfer(record, options)
        result = annotation_transfer(record, options, ".xml")
        return result
    elif task_category == TaskCategory.TEXT_CLASSIFICATION:
        result = text_transfer(record, options)
        return result
    elif task_category == TaskCategory.SEMANTIC_SEGMENTATION:
        # Atomic: only copy image+mask together. Pre-verify both sources
        # before either copy, since image_transfer returns the record (not
        # None) when the source image is missing — without this pre-check
        # a missing image would still let mask_transfer leave an orphan
        # mask on disk. Both sides resolve their source via shared helpers
        # (_find_image_src / _find_mask_src) so the pre-check stays in
        # lockstep with what the copy functions actually look for.
        filename = record.get("filename")
        if not filename:
            logger.error(f"{RED}No filename found in record{RESET}")
            return None
        image_src_path, image_filename = _find_image_src(
            filename, options.get("extension")
        )
        if image_src_path is None:
            logger.error(
                f"{RED}Source image not found: {os.path.join(config.SRC_PATH, 'images', image_filename)} — skipping record{RESET}"
            )
            return None

        mask_id = record.get("mask_id")
        if not mask_id:
            logger.error(f"{RED}No mask_id found in record{RESET}")
            return None
        mask_src_path, mask_ext, mask_name = _find_mask_src(mask_id)
        if mask_src_path is None:
            logger.error(
                f"{RED}Source mask not found: {mask_name} in {config.SRC_PATH}/masks/ — skipping record{RESET}"
            )
            return None
        record = image_transfer(record, options)
        record = mask_transfer(record, mask_src_path, mask_ext, mask_name)
        return record
    elif task_category == TaskCategory.KEYPOINT_DETECTION:
        result = image_transfer(record, options)
        return result
    else:
        return None
