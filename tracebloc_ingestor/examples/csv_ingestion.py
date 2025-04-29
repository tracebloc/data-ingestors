"""Basic CSV Ingestion Example.

This example demonstrates how to use the CSVIngestor to ingest data from a CSV file
into a database and optionally send it to an API. It includes a custom processor
that converts specified columns to uppercase.
"""

import logging
from pathlib import Path
from typing import Dict, Any

from traceblocingestor import Config, Database, APIClient, CSVIngestor
from traceblocingestor.processors.base import BaseProcessor
from traceblocingestor.utils.logging import setup_logging
from traceblocingestor.utils.constants import DataCategory, Intent

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

class UpperCaseProcessor(BaseProcessor):
    """Processor that converts specified columns to uppercase.
    
    This processor demonstrates how to create a custom processor that
    transforms data during ingestion.
    """
    
    def __init__(self, column_name: str):
        """Initialize the processor.
        
        Args:
            column_name: Name of the column to convert to uppercase
        """
        self.column_name = column_name
        
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process the record by converting the specified column to uppercase.
        
        Args:
            record: The record to process
            
        Returns:
            Processed record with uppercase column
        """
        if self.column_name in record and isinstance(record[self.column_name], str):
            record[self.column_name] = record[self.column_name].upper()
        return record
        
    def cleanup(self):
        """Cleanup any temporary files if needed."""
        pass

def main():
    """Run the CSV ingestion example."""
    try:
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
            intent=Intent.TRAIN,  # Is the data for training or testing
            annotation_column="notes"
        )

        # Get the example data path
        data_path = Path(__file__).parent / "data" / "sample.csv"
        
        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(str(data_path), batch_size=config.BATCH_SIZE)
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(f"Failed record: {record.get('name', 'Unknown')}")
                    logger.warning(f"Error details: {record.get('error', 'Unknown error')}")
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Error during CSV ingestion: {str(e)}")
        raise

if __name__ == "__main__":
    main() 