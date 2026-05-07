"""Keypoint Detection Data Ingestion Example.

This example demonstrates how to ingest keypoint detection data with images and
corresponding keypoint annotations into a database and optionally send it to an API.
It processes image files along with JSON-based keypoint coordinate annotations.
"""

import logging
from typing import Dict, Any

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
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

# Keypoint schema definition
# Define the expected keypoints for the dataset
keypoints = [
    "nose",
    "left_eye",
    "right_eye",
    "left_shoulder",
    "right_shoulder",
    "left_elbow",
    "right_elbow",
    "left_wrist",
    "right_wrist",
]

# Keypoint detection specific options
keypoint_detection_options = {
    "target_size": (448, 448),  # image size. Height = Width
    "extension": FileExtension.JPG,  # allowed extension for images: jpeg, jpg, png
    "number_of_keypoints": len(keypoints),  # number of keypoints per sample
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
    """Run the keypoint detection ingestion example."""
    try:
        # Initialize components
        database = Database(config)
        api_client = APIClient(config)

        # Create ingestor for keypoint detection data with validators
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            data_format=DataFormat.IMAGE,
            category=TaskCategory.KEYPOINT_DETECTION,
            csv_options=csv_options,
            file_options=keypoint_detection_options,
            label_column="image_label",
            annotation_column="Annotation",
            unique_id_column="filename",
            intent=Intent.TRAIN,  # Is the data for training or testing
        )

        # Ingest data with validation
        logger.info("Starting keypoint detection ingestion with data validation...")
        with ingestor:
            failed_records = ingestor.ingest(
                config.LABEL_FILE, batch_size=config.BATCH_SIZE
            )
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(
                        f"Failed record: {record.get('image_id', 'Unknown')}"
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
