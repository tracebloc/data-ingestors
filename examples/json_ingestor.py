"""Basic JSON Ingestion Example.

This example demonstrates how to use the JSONIngestor to ingest data from a JSON file
into a database and optionally send it to an API. It includes a custom processor
that normalizes data types and formats.
"""

import logging
from pathlib import Path
from typing import Dict, Any

from tracebloc_ingestor import Config, Database, APIClient, JSONIngestor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import TaskCategory, Intent, DataFormat

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)


def main():
    """Run the JSON ingestion example."""
    try:
        # Initialize components
        database = Database(config)
        api_client = APIClient(config)

        # Schema definition with constraints
        schema = {
            "id": "VARCHAR(255)",
            "name": "VARCHAR(255)",
            "email": "VARCHAR(255)",
            "age": "INT",
            "is_active": "BOOL DEFAULT FALSE",
            "created_at": "DATE",
            "metadata": "TEXT",
        }

        # JSON specific options with additional configurations
        json_options = {
            "encoding": "utf-8",
            "parse_float": float,
            "parse_int": int,
            "parse_constant": None,
            "object_pairs_hook": None,
            "strict": True,
            "object_hook": None,
        }

        # Get the example data path
        data_path = (
            Path(__file__).parent
            / "example_data"
            / "tabular_classification_sample_in_json_format.json"
        )

        # Create ingestor
        ingestor = JSONIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            data_format=DataFormat.IMAGE,
            category=TaskCategory.TABULAR_CLASSIFICATION,
            json_options=json_options,
            unique_id_column="unique_id",
            label_column="name",
            intent=Intent.TRAIN,
            annotation_column="metadata",
        )

        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(
                str(data_path), batch_size=config.BATCH_SIZE
            )
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(f"Failed record: {record.get('id', 'Unknown')}")
                    logger.warning(
                        f"Error details: {record.get('error', 'Unknown error')}"
                    )
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Error during JSON ingestion: {str(e)}")
        raise


if __name__ == "__main__":
    main()
