"""Custom Processor Example.

This example demonstrates how to create and use custom processors with the ingestors
to transform data during ingestion.
"""

import logging
from pathlib import Path
from typing import Dict, Any

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
from tracebloc_ingestor.processors.base import BaseProcessor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import DataCategory, Intent

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

class UpperCaseProcessor(BaseProcessor):
    """Processor that converts specified column values to uppercase."""
    
    def __init__(self, config: Config, column_name: str):
        """Initialize the processor.
        
        Args:
            config: Configuration object
            column_name: Name of the column to process
        """
        super().__init__(config)
        self.column_name = column_name

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process a record by converting the specified column to uppercase.
        
        Args:
            record: The record to process
            
        Returns:
            The processed record
        """
        if self.column_name in record and isinstance(record[self.column_name], str):
            record[self.column_name] = record[self.column_name].upper()
        return record

class EmailDomainProcessor(BaseProcessor):
    """Processor that extracts domain from email addresses."""
    
    def __init__(self, config: Config):
        """Initialize the processor.
        
        Args:
            config: Configuration object
        """
        super().__init__(config)
    
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process a record by extracting email domain.
        
        Args:
            record: The record to process
            
        Returns:
            The processed record with added 'email_domain' field
            
        Raises:
            ValueError: If email format is invalid
        """
        if 'email' in record and isinstance(record['email'], str):
            try:
                domain = record['email'].split('@')[1]
                if not domain:  # Check if domain is empty
                    raise ValueError("Empty domain in email")
                record['email_domain'] = domain
            except IndexError:
                raise ValueError(f"Invalid email format: {record['email']}")
        return record

def main():
    """Run the custom processor example."""
    try:
        # Initialize components
        database = Database(config)
        api_client = APIClient(config)

        # Schema definition with constraints
        schema = {
            "name": "VARCHAR(255) NOT NULL",
            "email": "VARCHAR(255) UNIQUE",
            "email_domain": "VARCHAR(255)",  # Added for the EmailDomainProcessor
            "age": "INT CHECK (age >= 0 AND age <= 150)",
            "description": "VARCHAR(255)"
        }

        # Create processors
        processors = [
            UpperCaseProcessor(config=config, column_name="name"),
            EmailDomainProcessor(config=config)
        ]

        # Create ingestor with processors
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            category=DataCategory.TABULAR_CLASSIFICATION,
            processors=processors,
            label_column="name",
            intent=Intent.TRAIN
        )

        # Get the example data path
        data_path = Path(__file__).parent / "data" / "tabular_classification_sample_in_csv_format.csv"
        
        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(str(data_path), batch_size=config.BATCH_SIZE)
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Error during ingestion: {str(e)}")
        raise

if __name__ == "__main__":
    main() 