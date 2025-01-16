from typing import Dict, Any, Generator, Optional, List
import json
import logging
from pathlib import Path
from .base import BaseIngestor
from ..database import Database
from ..api.client import APIClient
from ..processors.base import BaseProcessor

logger = logging.getLogger(__name__)

class JSONIngestor(BaseIngestor):
    def __init__(
        self,
        database: Database,
        api_client: APIClient,
        table_name: str,
        schema: Dict[str, str],
        processors: Optional[List[BaseProcessor]] = None,
        max_retries: int = 3,
        json_options: Optional[Dict[str, Any]] = None,
        unique_id_column: Optional[str] = None,
        label_column: Optional[str] = None,
        intent_column: Optional[str] = None,
        annotation_column: Optional[str] = None
    ):
        """
        Initialize JSON Ingestor
        
        Args:
            database: Database instance
            api_client: API client instance
            table_name: Name of the target table
            schema: Database schema definition
            processors: List of data processors
            max_retries: Maximum number of retry attempts
            json_options: Additional options for JSON processing
            unique_id_column: Name of the column to use as unique identifier
            label_column: Name of the column to use as label
            intent_column: Name of the column to use as data_intent
            annotation_column: Name of the column to use as annotation
        """
        super().__init__(
            database, 
            api_client, 
            table_name, 
            schema, 
            processors, 
            max_retries,
            unique_id_column,
            label_column,
            intent_column,
            annotation_column
        )
        self.json_options = json_options or {}
        
    def _validate_record(self, record: Dict[str, Any]) -> None:
        """
        Validate JSON record against schema
        
        Args:
            record: JSON record
            
        Raises:
            ValueError: If validation fails
        """
        # Check for required fields
        schema_fields = set(self.schema.keys())
        record_fields = set(record.keys())
        
        missing_fields = schema_fields - record_fields
        if missing_fields:
            raise ValueError(
                f"Missing required fields in JSON record: {', '.join(missing_fields)}"
            )
            
        # Validate unique_id_column exists if specified
        if self.unique_id_column and self.unique_id_column not in record:
            raise ValueError(f"Specified unique_id_column '{self.unique_id_column}' not found in record")

        # Basic data type validation
        for field, dtype in self.schema.items():
            value = record[field]
            try:
                if 'INT' in dtype.upper():
                    int(value) if value != '' else None
                elif 'FLOAT' in dtype.upper():
                    float(value) if value != '' else None
                elif 'BOOL' in dtype.upper():
                    bool(value) if value != '' else None
                # Add more type validations as needed
            except Exception as e:
                raise ValueError(
                    f"Data type validation failed for field {field}: {str(e)}"
                )

    def read_data(self, file_path: str) -> Generator[Dict[str, Any], None, None]:
        """
        Read and validate JSON file
        
        Args:
            file_path: Path to the JSON file
            
        Yields:
            Dict containing record data
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"JSON file not found: {file_path}")
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Load JSON data
                data = json.load(f)
                
                # Handle both array and object formats
                if isinstance(data, dict):
                    data = [data]
                elif not isinstance(data, list):
                    raise ValueError("JSON data must be an object or array of objects")
                
                # Process each record
                for record in data:
                    if not isinstance(record, dict):
                        logger.warning(f"Skipping invalid record: {record}")
                        continue
                        
                    try:
                        self._validate_record(record)
                        yield record  # Let base class handle the cleaning and unique ID mapping
                    except ValueError as e:
                        logger.warning(f"Skipping invalid record: {str(e)}")
                        continue
                    
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON file: {str(e)}")
            raise
            
        except Exception as e:
            logger.error(f"Unexpected error reading JSON: {str(e)}")
            raise

    def ingest(self, file_path: str, batch_size: int = 50) -> List[Dict[str, Any]]:
        """
        Ingest JSON file with progress tracking
        
        Args:
            file_path: Path to the JSON file
            batch_size: Size of each batch
            
        Returns:
            List of failed records
        """
        logger.info(f"Starting JSON ingestion from {file_path}")
        
        try:
            failed_records = super().ingest(file_path, batch_size)
            
            logger.info(
                f"JSON ingestion completed. "
                f"Failed records: {len(failed_records)}"
            )
            
            return failed_records
            
        except Exception as e:
            logger.error(f"JSON ingestion failed: {str(e)}")
            raise 