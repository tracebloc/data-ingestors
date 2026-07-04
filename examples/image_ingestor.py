"""Image Data Ingestion Example.

This example demonstrates how to ingest an image-classification dataset from
a CSV label file into the database and register it with the tracebloc
backend. Image files are picked up by the framework's per-category sidecar
convention (an ``images/`` directory under ``SRC_PATH``) — there is no need
to copy or transform files yourself.

See ``templates/image_classification/`` for a runnable version of this
workflow with bundled sample data, and ``examples/yaml/`` for the
declarative ``ingest.yaml`` equivalent.
"""

import logging

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

# Image specific options
image_options = {
    "target_size": (256, 256),  # expected image size. Height = Width
    "extension": FileExtension.JPEG,
}

# CSV specific options
csv_options = {
    "chunk_size": 1000,
    "delimiter": ",",
    "quotechar": '"',
    "escapechar": "\\",
    "on_bad_lines": "warn",
    "encoding": "utf-8",
}


def main():
    """Run the image ingestion example."""
    # Initialize components
    database = Database(config)
    api_client = APIClient(config)

    # Create ingestor for image classification data with validators
    ingestor = CSVIngestor(
        database=database,
        api_client=api_client,
        table_name=config.TABLE_NAME,
        data_format=DataFormat.IMAGE,
        category=TaskCategory.IMAGE_CLASSIFICATION,
        csv_options=csv_options,
        file_options=image_options,
        label_column="label",
        intent=Intent.TRAIN,  # Is the data for training or testing
    )

    # Ingest data with validation
    logger.info("Starting image classification ingestion with data validation...")
    run_ingestion(
        ingestor, config.LABEL_FILE, batch_size=config.BATCH_SIZE, logger=logger
    )


if __name__ == "__main__":
    main()
