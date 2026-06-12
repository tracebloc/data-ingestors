"""Object Detection Data Ingestion Example.

This example demonstrates how to ingest object detection data with images and XML annotations
into a database and optionally send it to an API. It processes both the image files and their
corresponding XML annotation files from the VisDrone dataset format.
"""

import logging
import shutil
import xml.etree.ElementTree as ET
from typing import Dict, Any
import json
import os

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

# Object detection specific options including CSV options
object_detection_options = {
    # Matches the bundled VisDrone aerial sample under data/images/ (#199),
    # kept at native resolution because aggressive downscaling obliterates
    # the tiny-object content the sample exists to demonstrate. Override per
    # dataset when running against tiled / pre-resized data.
    "target_size": (1920, 1080),
    "extension": FileExtension.JPG,  # allowed extension for images: jpeg, jpg, png
}

# CSV specific options
csv_options = {
    "chunk_size": 100,  # Smaller chunk size due to larger data
    "delimiter": ",",
    "quotechar": '"',
    "escapechar": "\\",
    "on_bad_lines": "warn",
    "encoding": "utf-8",
}


def main():
    """Run the object detection ingestion example."""
    # Initialize components
    database = Database(config)
    api_client = APIClient(config)

    # Create ingestor for object detection data with validators
    ingestor = CSVIngestor(
        database=database,
        api_client=api_client,
        table_name=config.TABLE_NAME,
        data_format=DataFormat.IMAGE,
        category=TaskCategory.OBJECT_DETECTION,
        csv_options=csv_options,
        file_options=object_detection_options,
        label_column="image_label",
        intent=Intent.TRAIN,  # Is the data for training or testing
    )

    # Ingest data with validation
    logger.info("Starting object detection ingestion with data validation...")
    run_ingestion(
        ingestor, config.LABEL_FILE, batch_size=config.BATCH_SIZE, logger=logger
    )


if __name__ == "__main__":
    main()
