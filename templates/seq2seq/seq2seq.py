"""Sequence-to-Sequence (seq2seq) Data Ingestion Example.

This example demonstrates how to ingest text samples for sequence-to-sequence
(encoder-decoder) modeling into a database and optionally send metadata to the
tracebloc API.

seq2seq is self-supervised — no label column is needed.  Each row in the CSV
maps to a ``.txt`` file under ``texts/`` holding RAW text:

- Each file is a single tab-separated ``source\\ttarget`` pair; the training
  client feeds the source to the encoder and trains the decoder to produce the
  target (e.g. translation, summarization, paraphrase).

This is the same on-disk shape as causal language modeling's
``prompt\\tcompletion`` pair — one ``.txt`` per sample under ``texts/`` — so it
reuses the same raw-text ingestion path. The contributor's tokenizer alignment
is checked centrally at dataset linking via the data-derived text profile
(#805); nothing tokenizer-specific is needed here.
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
    """Run the sequence-to-sequence ingestion example."""
    # Initialize components
    database = Database(config)
    api_client = APIClient(config)

    # Create ingestor for seq2seq data
    # No label_column — seq2seq is self-supervised
    ingestor = CSVIngestor(
        database=database,
        api_client=api_client,
        table_name=config.TABLE_NAME,
        data_format=DataFormat.TEXT,
        category=TaskCategory.SEQ2SEQ,
        csv_options=csv_options,
        file_options=text_options,
        intent=Intent.TRAIN,
    )

    # Ingest data with validation
    logger.info("Starting sequence-to-sequence ingestion with data validation...")
    run_ingestion(
        ingestor, config.LABEL_FILE, batch_size=config.BATCH_SIZE, logger=logger
    )


if __name__ == "__main__":
    main()
