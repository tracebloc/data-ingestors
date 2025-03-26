import sys
import os
import logging
from typing import Dict, Any

# Add the src directory to the PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.config import Config
from src.database import Database
from src.api.client import APIClient
from src.ingestors.csv_ingestor import CSVIngestor
from src.processors.base import BaseProcessor
from src.utils.logging import setup_logging
from src.utils.constants import DataCategory, Intent

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

# Initialize components
database = Database(config)
api_client = APIClient(config)

# Schema definition
schema = {
    "name": "VARCHAR(255)",
    "age": "INT",
    "email": "VARCHAR(255)",
    "description": "VARCHAR(255)",
    "profile_image_url": "VARCHAR(512)",
    "notes": "TEXT"
}

# CSV specific options
csv_options = {
    "chunk_size": 1000,
    "delimiter": ",",
    "quotechar": '"',
    "escapechar": "\\",
}


# Define a simple processor
class UpperCaseProcessor(BaseProcessor):
    def __init__(self, column_name: str):
        self.column_name = column_name

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        # Convert the specified column to uppercase
        if self.column_name in record and isinstance(record[self.column_name], str):
            record[self.column_name] = record[self.column_name].upper()
        return record

# Create an instance of the processor
upper_case_processor = UpperCaseProcessor(column_name="name")

# Create ingestor with unique_id_column specified
ingestor = CSVIngestor(
    database=database,
    api_client=api_client,
    table_name=config.TABLE_NAME,
    schema=schema,
    category=DataCategory.TABULAR_CLASSIFICATION,
    csv_options=csv_options,
    processors=[upper_case_processor],
    label_column="name",
    intent=Intent.TRAIN, # Is the data for training or testing
    annotation_column="notes"
)

# Ingest data
with ingestor:
    failed_records = ingestor.ingest(config.LABEL_FILE, batch_size=config.BATCH_SIZE)
    if failed_records:
        print(f"Failed to process {len(failed_records)} records") 