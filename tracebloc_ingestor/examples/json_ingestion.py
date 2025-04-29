"""Basic JSON Ingestion Example.

This example demonstrates how to use the JSONIngestor to ingest data from a JSON file
into a database and optionally send it to an API. It includes a custom processor
that normalizes data types and formats.
"""

import logging
from pathlib import Path
from typing import Dict, Any

from tracebloc_ingestor import Config, Database, APIClient, JSONIngestor
from tracebloc_ingestor.processors.base import BaseProcessor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import DataCategory, Intent

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

class DataNormalizer(BaseProcessor):
    """Processor that normalizes data types and formats.
    
    This processor demonstrates how to create a custom processor that
    normalizes data during ingestion.
    """
    
    def __init__(self):
        """Initialize the processor."""
        super().__init__()
        
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process the record by normalizing data types and formats.
        
        Args:
            record: The record to process
            
        Returns:
            Processed record with normalized data
        """
        try:
            if 'email' in record:
                record['email'] = record['email'].lower().strip()
            if 'age' in record:
                record['age'] = int(record['age'])
            if 'is_active' in record:
                record['is_active'] = bool(record['is_active'])
            return record
        except (ValueError, TypeError) as e:
            raise ValueError(f"Error normalizing data: {str(e)}")
            
    def cleanup(self):
        """Cleanup any temporary files if needed."""
        pass

def main():
    """Run the JSON ingestion example."""
    try:
        # Initialize components
        database = Database(config)
        api_client = APIClient(config)

        # Schema definition
        schema = {
            "id": "VARCHAR(255)",
            "name": "VARCHAR(255)",
            "email": "VARCHAR(255)",
            "age": "INT",
            "is_active": "BOOL",
            "created_at": "DATE",
            "metadata": "TEXT"
        }

        # JSON specific options
        json_options = {
            "encoding": "utf-8",
            "parse_float": float,
            "parse_int": int,
            "parse_constant": None
        }

        # Create an instance of the processor
        data_normalizer = DataNormalizer()

        # Create ingestor
        ingestor = JSONIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            category=DataCategory.TABULAR_CLASSIFICATION,
            json_options=json_options,
            processors=[data_normalizer],
            unique_id_column="id",
            label_column="name",
            intent=Intent.TRAIN,
            annotation_column="metadata"
        )

        # Get the example data path
        data_path = Path(__file__).parent / "data" / "users.json"
        
        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(str(data_path), batch_size=config.BATCH_SIZE)
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(f"Failed record: {record.get('id', 'Unknown')}")
                    logger.warning(f"Error details: {record.get('error', 'Unknown error')}")
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Error during JSON ingestion: {str(e)}")
        raise

if __name__ == "__main__":
    main() 