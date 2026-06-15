"""Masked Language Modeling Data Ingestion Example.

This example demonstrates how to ingest pre-tokenized text sequences for
masked language modeling (MLM) into a database and optionally send metadata
to the tracebloc API.

MLM is self-supervised — no label column is needed.  Each row in the CSV
maps to a .txt file containing a space-separated token sequence (e.g. a
random walk over a knowledge graph).  The MLM client applies masking
on-the-fly during training.
"""

import logging
import os
from typing import Dict, Any

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

# Text file options
text_options = {"extension": FileExtension.TXT}

# CSV specific options
csv_options = {
    "chunk_size": 100,
    "delimiter": ",",
    "quotechar": '"',
    "escapechar": "\\",
    "on_bad_lines": "warn",
    "encoding": "utf-8",
}


def main():
    """Run the masked language modeling ingestion example."""
    # Initialize components
    database = Database(config)
    api_client = APIClient(config)

    # Create ingestor for MLM data
    # No label_column — MLM is self-supervised
    ingestor = CSVIngestor(
        database=database,
        api_client=api_client,
        table_name=config.TABLE_NAME,
        data_format=DataFormat.TEXT,
        category=TaskCategory.MASKED_LANGUAGE_MODELING,
        csv_options=csv_options,
        file_options=text_options,
        intent=Intent.TRAIN,
    )

    # Ingest data with validation
    logger.info("Starting masked language modeling ingestion with data validation...")
    run_ingestion(
        ingestor, config.LABEL_FILE, batch_size=config.BATCH_SIZE, logger=logger
    )


if __name__ == "__main__":
    main()
