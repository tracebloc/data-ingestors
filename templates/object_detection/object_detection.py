"""Object Detection Data Ingestion Example.

Ingests object detection data from images and Pascal-VOC XML annotations.

Since backend#1006 this needs **no labels.csv**: records are enumerated
straight from ``annotations/*.xml`` — ``<annotation><filename>`` names the
image and ``<object><name>`` gives the classes — so the user stages
``images/`` and ``annotations/`` and nothing else.

The record model is **one row per image** (not per bounding box, as the CSV
manifest produced). See ``tracebloc_ingestor/utils/od_label_semantics.py`` for
what a per-image ``label`` cell means and why the summary deliberately carries
two units (``labels`` counts boxes, ``record_count`` counts images).
"""

import logging

from tracebloc_ingestor import (
    Config,
    Database,
    APIClient,
    VOCIngestor,
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

# Object detection specific options
object_detection_options = {
    # Matches the bundled VisDrone aerial sample under data/images/ (#199),
    # kept at native resolution because aggressive downscaling obliterates
    # the tiny-object content the sample exists to demonstrate. Override per
    # dataset when running against tiled / pre-resized data.
    "target_size": (1920, 1080),
    "extension": FileExtension.JPG,  # allowed extension for images: jpeg, jpg, png
}


def main():
    """Run the object detection ingestion example."""
    # Initialize components
    database = Database(config)
    api_client = APIClient(config)

    # Enumerate records from the Pascal-VOC XML — no labels.csv (backend#1006).
    ingestor = VOCIngestor(
        database=database,
        api_client=api_client,
        table_name=config.TABLE_NAME,
        data_format=DataFormat.IMAGE,
        category=TaskCategory.OBJECT_DETECTION,
        file_options=object_detection_options,
        label_column="image_label",
        intent=Intent.TRAIN,  # Is the data for training or testing
        # data_id_strategy is left at the package default, content_hash.
        #
        # The CSV path pinned this to "uuid" because objdet manifests carried
        # one row per OBJECT, so duplicate (filename, label) rows were real
        # distinct boxes that a content hash would have collapsed (bugbot on
        # #383). Per-image enumeration removes that hazard at the source: one
        # record per annotation file, and the filenames are unique on disk
        # (FilePairingValidator pairs each image to exactly one <stem>.xml), so
        # there is nothing left for a content hash to collapse.
        #
        # Taking the default back is a real gain, not just tidiness — a
        # content_hash data_id lets a retried Kubernetes Job re-claim its rows
        # through the data_id UNIQUE upsert instead of inserting a second copy
        # of every image (#350).
    )

    # Ingest data with validation. The source is the ANNOTATIONS DIRECTORY, not
    # a manifest file; omitted here so it defaults to <SRC_PATH>/annotations/.
    logger.info("Starting object detection ingestion with data validation...")
    run_ingestion(ingestor, None, batch_size=config.BATCH_SIZE, logger=logger)


if __name__ == "__main__":
    main()
