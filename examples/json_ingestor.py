import sys
import os

# Add the src directory to the PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.ingestors.json_ingestor import JSONIngestor
from src.database import Database
from src.api.client import APIClient
from src.processors.base import BaseProcessor
from src.config import Config
from src.utils.logging import setup_logging
import logging
from src.utils.constants import DataCategory, Intent

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

def main():
    # Initialize database connection
    database = Database(config)
    
    # Initialize API client
    api_client = APIClient(config)
    
    # Define schema for the target table
    schema = {
        "name": "VARCHAR(100)",
        "email": "VARCHAR(255)",
        "age": "INTEGER",
        "active": "BOOLEAN"
    }
    
    # Optional: Create a custom processor
    class DataNormalizer(BaseProcessor):
        def __init__(self):
            super().__init__(config)

        def process(self, record):
            if 'email' in record:
                record['email'] = record['email'].lower().strip()
            if 'age' in record:
                record['age'] = int(record['age'])
            if 'active' in record:
                record['active'] = bool(record['active'])

            return record
    
    # Initialize JSON ingestor with properly configured processor
    ingestor = JSONIngestor(
        database=database,
        api_client=api_client,
        table_name="users",
        schema=schema,
        category=DataCategory.TABULAR_CLASSIFICATION,
        processors=[DataNormalizer()],
        json_options={
            "encoding": "utf-8"
        },
        unique_id_column="unique_id",
        label_column="label",
        intent=Intent.TRAIN, # Is the data for training or testing
        annotation_column="annotation"

    )
    
    try:
        # Ingest JSON file
        failed_records = ingestor.ingest(
            file_path="examples/data/users.json",
            batch_size=config.BATCH_SIZE
        )
        
        # Handle failed records if any
        if failed_records:
            logger.warning(f"Failed to process {len(failed_records)} records")
            for record in failed_records:
                logger.warning(f"Failed record: {record}")
                
    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 