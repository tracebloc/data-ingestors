"""Blob Data Ingestion Example.

This example demonstrates how to ingest binary data (BLOBs) from a CSV file into a database
and optionally send it to an API. It handles base64 encoded binary data and includes
validation and processing of binary content.
"""

import base64
import json
import logging
from pathlib import Path
from typing import Dict, Any

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
from tracebloc_ingestor.processors.base import BaseProcessor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import DataCategory, Intent, DataFormat

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

class BlobDataProcessor(BaseProcessor):
    """Processor for handling binary data (BLOBs) in records.
    
    This processor handles base64 encoded binary data, validates required fields,
    and ensures proper formatting of metadata.
    """
    
    def __init__(self, config: Config, storage_path: str):
        """Initialize the blob data processor.
        
        Args:
            config: Configuration object
            storage_path: Path to store temporary files if needed
        """
        super().__init__(config)
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process blob data in the record.
        
        Args:
            record: The record containing blob data
            
        Returns:
            Processed record with properly formatted blob data
            
        Raises:
            ValueError: If required fields are missing or data is invalid
        """
        try:
            # Validate required fields
            required_fields = ['name', 'document_type', 'content_type', 'document_data']
            for field in required_fields:
                if not record.get(field):
                    raise ValueError(f"Missing required field: {field}")

            # Handle document data (assuming base64 encoded in CSV)
            if record['document_data']:
                try:
                    if not isinstance(record['document_data'], str):
                        raise ValueError("document_data must be a string")
                    # Remove any whitespace or newlines that might have been added
                    clean_data = record['document_data'].strip()
                    record['document_data'] = base64.b64decode(clean_data)
                except Exception as e:
                    raise ValueError(f"Invalid base64 data for document: {str(e)}")
            
            # Handle thumbnail (assuming base64 encoded in CSV)
            if record.get('thumbnail'):
                try:
                    if not isinstance(record['thumbnail'], str):
                        raise ValueError("thumbnail must be a string")
                    # Remove any whitespace or newlines that might have been added
                    clean_thumb = record['thumbnail'].strip()
                    record['thumbnail'] = base64.b64decode(clean_thumb)
                except Exception as e:
                    raise ValueError(f"Invalid base64 data for thumbnail: {str(e)}")
            else:
                record['thumbnail'] = None  # Ensure NULL for empty thumbnails
            
            # Validate and process metadata
            if record.get('metadata'):
                try:
                    # Ensure metadata is valid JSON
                    if isinstance(record['metadata'], str):
                        metadata_dict = json.loads(record['metadata'])
                    elif isinstance(record['metadata'], dict):
                        metadata_dict = record['metadata']
                    else:
                        raise ValueError("metadata must be either a JSON string or a dictionary")
                    record['metadata'] = json.dumps(metadata_dict)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON metadata: {str(e)}")
            else:
                record['metadata'] = '{}'  # Default empty JSON object
            
            # Validate content type
            if not record['content_type'].strip():
                record['content_type'] = 'application/octet-stream'
            
            return record
            
        except Exception as e:
            raise ValueError(f"Error processing blob data: {str(e)}")
    
    def cleanup(self):
        """Cleanup any temporary files if needed."""
        pass

def main():
    """Run the blob ingestion example."""
    try:
        # Initialize components
        database = Database(config)
        api_client = APIClient(config)

        # Schema definition with BLOB/LONGBLOB fields
        schema = {
            "name": "VARCHAR(255)",
            "document_type": "VARCHAR(50)",
            "content_type": "VARCHAR(100)",
            "document_data": "LONGBLOB",  # For storing large binary objects
            "thumbnail": "BLOB",  # For storing smaller binary objects
            "metadata": "TEXT"  # For storing additional JSON metadata
        }

        # CSV specific options
        csv_options = {
            "chunk_size": 500,  # Smaller chunk size due to larger data
            "delimiter": ",",
            "quotechar": '"',
            "escapechar": "\\",
            "on_bad_lines": 'warn'  # Just warn about bad lines instead of failing
        }

        # Create blob data processor
        blob_processor = BlobDataProcessor(config=config, storage_path=config.STORAGE_PATH)

        # Create ingestor with blob processor
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            category=DataCategory.TEXT_CLASSIFICATION,
            data_format=DataFormat.TEXT,
            intent=Intent.TRAIN,
            csv_options=csv_options,
            processors=[blob_processor]
        )

        # Get the example data path
        data_path = Path(__file__).parent / "data" / "text_classification_sample.csv"
        
        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(
                str(data_path),
                batch_size=25  # Smaller batch size due to larger data
            )
            
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(f"Failed record: {record.get('name', 'Unknown')}")
                    logger.warning(f"Error details: {record.get('error', 'Unknown error')}")
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 