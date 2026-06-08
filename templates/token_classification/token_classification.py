"""Token Classification (NER/POS) Data Ingestion Example.

This example demonstrates how to ingest token classification data — one
``.txt`` file of whitespace-tokenized words per sample, plus a CSV whose
``label`` column holds the space-separated BIO/IOB2 tags (exactly one tag per
word). The on-disk layout is identical to text classification; only the label
semantics differ (a sequence of per-token tags instead of a single class).

Example row:
    filename = "sentence_001",  label = "B-PER I-PER O O B-LOC"
    sentence_001.txt           -> "John Smith works in Paris"
"""

import logging
import os
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

# Text specific options including CSV options
text_options = {"extension": FileExtension.TXT}  # Allowed text file extensions

# CSV specific options
csv_options = {
    "chunk_size": 100,  # Smaller chunk size due to text processing
    "delimiter": ",",
    "quotechar": '"',
    "escapechar": "\\",
    "on_bad_lines": "warn",
    "encoding": "utf-8",
}


def main():
    """Run the token classification ingestion example."""
    try:
        # Initialize components
        database = Database(config)
        api_client = APIClient(config)

        # Create ingestor for token classification data with validators
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            data_format=DataFormat.TEXT,
            category=TaskCategory.TOKEN_CLASSIFICATION,
            csv_options=csv_options,
            file_options=text_options,
            label_column="label",
            intent=Intent.TRAIN,  # Is the data for training or testing
        )

        # Ingest data with validation
        logger.info("Starting token classification ingestion with data validation...")
        with ingestor:
            failed_records = ingestor.ingest(
                config.LABEL_FILE, batch_size=config.BATCH_SIZE
            )
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(
                        f"Failed record: {record.get('filename', 'Unknown')}"
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
