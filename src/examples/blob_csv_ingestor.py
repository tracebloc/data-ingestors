import sys
import os
import base64
import json
from typing import Dict, Any
from pathlib import Path

# Add the src directory to the PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.config import Config
from src.database import Database
from src.api.client import APIClient
from src.ingestors.csv_ingestor import CSVIngestor
from src.processors.base import BaseProcessor

# Initialize components
config = Config()
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

class BlobDataProcessor(BaseProcessor):
    def __init__(self, storage_path: str):
        """
        Initialize the blob data processor
        
        Args:
            storage_path: Path to store temporary files if needed
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process blob data in the record
        
        Args:
            record: The record containing blob data
            
        Returns:
            Processed record with properly formatted blob data
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
                    # Remove any whitespace or newlines that might have been added
                    clean_data = record['document_data'].strip()
                    record['document_data'] = base64.b64decode(clean_data)
                except Exception as e:
                    raise ValueError(f"Invalid base64 data for document: {str(e)}")
            
            # Handle thumbnail (assuming base64 encoded in CSV)
            if record.get('thumbnail'):
                try:
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
                        record['metadata'] = json.loads(record['metadata'])
                    record['metadata'] = json.dumps(record['metadata'])
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
        """Cleanup any temporary files if needed"""
        pass

# Create blob data processor
blob_processor = BlobDataProcessor(storage_path=config.STORAGE_PATH)

# Create ingestor with blob processor
ingestor = CSVIngestor(
    database=database,
    api_client=api_client,
    table_name="documents",
    schema=schema,
    csv_options=csv_options,
    processors=[blob_processor]
)

# Example usage
if __name__ == "__main__":
    # Ingest data
    try:
        failed_records = ingestor.ingest(
            "src/examples/data/documents.csv",
            batch_size=25  # Smaller batch size due to larger data
        )
        
        if failed_records:
            print(f"Failed to process {len(failed_records)} records")
            for record in failed_records:
                print(f"Failed record: {record.get('name', 'Unknown')}")
                print(f"Error details: {record.get('error', 'Unknown error')}")
                
    except Exception as e:
        print(f"Ingestion failed: {str(e)}")
        raise 