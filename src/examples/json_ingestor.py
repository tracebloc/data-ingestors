import sys
import os

# Add the src directory to the PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.ingestors.json_ingestor import JSONIngestor
from src.database import Database
from src.api.client import APIClient
from src.processors.base import BaseProcessor
from src.config import Config
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def main():
    # Initialize config
    config = Config()
    
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
        processors=[DataNormalizer()],
        json_options={
            "encoding": "utf-8"
        },
        unique_id_column="unique_id",
        label_column="label",
        intent_column="data_intent",
        annotation_column="annotation"
    )
    
    try:
        # Ingest JSON file
        failed_records = ingestor.ingest(
            file_path="src/examples/data/users.json",
            batch_size=50
        )
        
        # Handle failed records if any
        if failed_records:
            logging.warning(f"Failed to process {len(failed_records)} records")
            for record in failed_records:
                logging.warning(f"Failed record: {record}")
                
    except Exception as e:
        logging.error(f"Ingestion failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 