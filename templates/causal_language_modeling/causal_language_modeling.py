"""Causal Language Modeling Data Ingestion Example.

This example demonstrates how to ingest text samples for causal (next-token)
language modeling (CLM) into a database and optionally send metadata to the
tracebloc API.

CLM is self-supervised — no label column is needed.  Each row in the CSV maps
to a ``.txt`` file under ``texts/`` holding RAW text:

- **Pretraining:** the whole file is plain text. The training client builds
  next-token targets from it on-the-fly.
- **SFT:** the file is a single tab-separated ``prompt\\tcompletion`` pair; the
  client masks the prompt's loss and trains on the completion.

Decoder-only models tie ``pad`` to ``eos`` — there is no ``[MASK]`` token, so
(unlike masked language modeling) no mask token is required. The contributor's
tokenizer alignment is checked centrally at dataset linking via the
data-derived text profile (#805); nothing tokenizer-specific is needed here.
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
    """Run the causal language modeling ingestion example."""
    # Initialize components
    database = Database(config)
    api_client = APIClient(config)

    # Create ingestor for CLM data
    # No label_column — CLM is self-supervised
    ingestor = CSVIngestor(
        database=database,
        api_client=api_client,
        table_name=config.TABLE_NAME,
        data_format=DataFormat.TEXT,
        category=TaskCategory.CAUSAL_LANGUAGE_MODELING,
        csv_options=csv_options,
        file_options=text_options,
        intent=Intent.TRAIN,
    )

    # Ingest data with validation
    logger.info("Starting causal language modeling ingestion with data validation...")
    run_ingestion(
        ingestor, config.LABEL_FILE, batch_size=config.BATCH_SIZE, logger=logger
    )


if __name__ == "__main__":
    main()
