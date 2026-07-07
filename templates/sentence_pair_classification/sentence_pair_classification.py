"""Sentence-Pair Classification Data Ingestion Example.

This example demonstrates how to ingest sentence-pair classification data — text
files holding a tab-separated ``text_a<TAB>text_b`` pair plus a class label — into
a database and optionally send metadata to the tracebloc API.

sentence_pair_classification is SUPERVISED: the class label travels in the labels
CSV, exactly like text_classification. Each row maps to a ``.txt`` file under
``texts/`` holding RAW text:

- Each file is a single tab-separated ``text_a<TAB>text_b`` sentence pair (e.g. a
  premise and a hypothesis for NLI, or two sentences for a paraphrase / STS
  task). The training client encodes the two sentences as a pair and predicts
  the CSV ``label``.

This is the same raw-text ingestion path as text_classification (one ``.txt`` per
sample under ``texts/``), but the on-disk shape is STRUCTURED: files that are not
exactly 2 non-empty tab-separated fields are rejected at ingest by
``SentencePairValidator``. The contributor's tokenizer alignment is checked
centrally at dataset linking via the data-derived text profile (#805); nothing
tokenizer-specific is needed here.
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
    """Run the sentence-pair classification ingestion example."""
    # Initialize components
    database = Database(config)
    api_client = APIClient(config)

    # Create ingestor for sentence-pair classification data
    # Supervised — the class label lives in the CSV `label` column.
    ingestor = CSVIngestor(
        database=database,
        api_client=api_client,
        table_name=config.TABLE_NAME,
        data_format=DataFormat.TEXT,
        category=TaskCategory.SENTENCE_PAIR_CLASSIFICATION,
        csv_options=csv_options,
        file_options=text_options,
        label_column="label",
        intent=Intent.TRAIN,
    )

    # Ingest data with validation
    logger.info(
        "Starting sentence-pair classification ingestion with data validation..."
    )
    run_ingestion(
        ingestor, config.LABEL_FILE, batch_size=config.BATCH_SIZE, logger=logger
    )


if __name__ == "__main__":
    main()
