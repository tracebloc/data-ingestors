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
from tracebloc_ingestor.utils.constants import DataCategory, Intent, DataFormat

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

class DataNormalizer(BaseProcessor):
    """Processor that normalizes data types and formats.
    
    This processor demonstrates how to create a custom processor that
    normalizes data during ingestion.
    """
    
    def __init__(self, config: Config):
        """Initialize the processor.
        
        Args:
            config: Configuration object
        """
        super().__init__(config)
        
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process the record by normalizing data types and formats.
        
        Args:
            record: The record to process
            
        Returns:
            Processed record with normalized data
            
        Raises:
            ValueError: If data normalization fails
        """
        try:
            # Normalize string fields
            if 'email' in record:
                if not isinstance(record['email'], str):
                    raise ValueError("email must be a string")
                record['email'] = record['email'].lower().strip()
                if not record['email']:
                    raise ValueError("email cannot be empty")
                
            if 'name' in record:
                if not isinstance(record['name'], str):
                    raise ValueError("name must be a string")
                record['name'] = record['name'].strip()
                if not record['name']:
                    raise ValueError("name cannot be empty")
                
            # Normalize numeric fields
            if 'age' in record:
                try:
                    record['age'] = int(record['age'])
                    if record['age'] < 0 or record['age'] > 150:
                        raise ValueError("age must be between 0 and 150")
                except (ValueError, TypeError):
                    raise ValueError("age must be a valid integer")
                
            # Normalize boolean fields
            if 'is_active' in record:
                if isinstance(record['is_active'], str):
                    record['is_active'] = record['is_active'].lower() in ('true', '1', 'yes')
                else:
                    record['is_active'] = bool(record['is_active'])
                
            # Normalize date fields
            if 'created_at' in record:
                if not isinstance(record['created_at'], str):
                    raise ValueError("created_at must be a string")
                record['created_at'] = record['created_at'].strip()
                
            # Normalize metadata
            if 'metadata' in record:
                if isinstance(record['metadata'], dict):
                    import json
                    record['metadata'] = json.dumps(record['metadata'])
                elif not isinstance(record['metadata'], str):
                    raise ValueError("metadata must be a string or dict")
                
            return record
        except Exception as e:
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

        # Schema definition with constraints
        schema = {
            "id": "VARCHAR(255)",
            "name": "VARCHAR(255)",
            "email": "VARCHAR(255)",
            "age": "INT",
            "is_active": "BOOL DEFAULT FALSE",
            "created_at": "DATE",
            "metadata": "TEXT"
        }

        # JSON specific options with additional configurations
        json_options = {
            "encoding": "utf-8",
            "parse_float": float,
            "parse_int": int,
            "parse_constant": None,
            "object_pairs_hook": None,
            "strict": True,
            "object_hook": None
        }

        # Create an instance of the processor
        data_normalizer = DataNormalizer(config=config)

        # Create ingestor
        ingestor = JSONIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            category=DataCategory.TABULAR_CLASSIFICATION,
            data_format=DataFormat.IMAGE,
            json_options=json_options,
            processors=[data_normalizer],
            unique_id_column="id",
            label_column="name",
            intent=Intent.TRAIN,
            annotation_column="metadata"
        )

        # Get the example data path
        data_path = Path(__file__).parent / "data" / "tabular_classification_sample_in_json_format.json"
        
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