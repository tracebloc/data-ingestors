"""Embeddings (self-supervised contrastive) Data Ingestion Example.

This example demonstrates how to ingest text samples for self-supervised
contrastive embedding training into a database and optionally send metadata to
the tracebloc API.

embeddings is self-supervised — no label column is needed. Each row in the CSV
maps to a ``.txt`` file under ``texts/`` holding RAW text:

- Each file is a single tab-separated record, either a ``anchor\\tpositive``
  PAIR or an ``anchor\\tpositive\\tnegative`` TRIPLET. The training client pulls
  the anchor and positive (and optional hard negative) closer / further apart in
  embedding space via a contrastive objective.

This is the same raw-text ingestion path as seq2seq / causal language modeling
(one ``.txt`` per sample under ``texts/``), but the on-disk shape is STRUCTURED:
files that are not exactly 2 or 3 non-empty tab-separated fields are rejected at
ingest by ``ContrastivePairsValidator``. The contributor's tokenizer alignment
is checked centrally at dataset linking via the data-derived text profile
(#805); nothing tokenizer-specific is needed here.
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
    """Run the contrastive embeddings ingestion example."""
    # Initialize components
    database = Database(config)
    api_client = APIClient(config)

    # Create ingestor for embeddings data
    # No label_column — embeddings is self-supervised (contrastive)
    ingestor = CSVIngestor(
        database=database,
        api_client=api_client,
        table_name=config.TABLE_NAME,
        data_format=DataFormat.TEXT,
        category=TaskCategory.EMBEDDINGS,
        csv_options=csv_options,
        file_options=text_options,
        intent=Intent.TRAIN,
    )

    # Ingest data with validation
    logger.info("Starting contrastive embeddings ingestion with data validation...")
    run_ingestion(
        ingestor, config.LABEL_FILE, batch_size=config.BATCH_SIZE, logger=logger
    )


if __name__ == "__main__":
    main()
