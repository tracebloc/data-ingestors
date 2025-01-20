from abc import ABC, abstractmethod
from typing import Dict, Any, Generator, List, Optional, Callable
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import logging
from ..database import Database
from ..processors.base import BaseProcessor
from ..api.client import APIClient

logger = logging.getLogger(__name__)

class BaseIngestor(ABC):
    def __init__(self, 
                 database: Database, 
                 api_client: APIClient,
                 table_name: str,
                 schema: Dict[str, str],
                 processors: List[BaseProcessor] = None,
                 max_retries: int = 3,
                 unique_id_column: Optional[str] = None,
                 label_column: Optional[str] = None,
                 intent_column: Optional[str] = None,
                 annotation_column: Optional[str] = None
                 ):
        """
        Initialize the base ingestor
        
        Args:
            database: Database instance
            api_client: API client instance
            table_name: Name of the target table
            schema: Database schema definition
            processors: List of data processors
            max_retries: Maximum number of retry attempts
            unique_id_column: Name of the column to use as unique identifier
            
        Raises:
            ValueError: If unique_id_column is not provided
        """
        if not unique_id_column:
            raise ValueError(
                "unique_id_column must be specified. This column will be used to map records "
                "to their unique data_id in the database. Please provide the name of the column "
                "that contains unique identifiers in your data source."
            )
            
        self.database = database
        self.engine: Engine = database.engine
        self.api_client = api_client
        self.table_name = table_name
        self.schema = schema
        self.processors = processors or []
        self.max_retries = max_retries
        self.unique_id_column = unique_id_column
        self.label_column = label_column
        self.intent_column = intent_column
        self.annotation_column = annotation_column
        # Ensure table exists
        self.table = self.database.create_table(table_name, schema)
       

    def _map_unique_id(self, record: Dict[str, Any], cleaned_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Maps the unique ID from the source record to data_id in the cleaned record.
        
        Args:
            record: Original record with all fields
            cleaned_record: Processed record with schema fields
            
        Returns:
            Updated cleaned record if valid, None if invalid unique ID
        """

        # Validate label_column exists if specified
        columns_to_validate = [(self.label_column, "label_column"), (self.intent_column, "intent_column"), (self.annotation_column, "annotation_column")]
        columns_not_found = False
        for column, column_name in columns_to_validate:
            if column and column not in record:
                logger.warning(f"Specified {column_name} '{column}' not found in record")
                columns_not_found = True

        if columns_not_found:
            print(f"Record: {record}")

        if self.label_column:
            cleaned_record['label'] = record.get(self.label_column)

        if self.intent_column:
            cleaned_record['data_intent'] = record.get(self.intent_column)

        if self.annotation_column:
            cleaned_record['annotation'] = record.get(self.annotation_column)

        if not self.unique_id_column:
            return cleaned_record
            
        unique_id = record.get(self.unique_id_column)
        if unique_id is not None and str(unique_id).strip():
            cleaned_record['data_id'] = str(unique_id).strip()
            return cleaned_record
        else:
            logger.warning(f"Missing or invalid unique ID for record: {record}")
            return None

    def process_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single record through all processors"""
        try:
            # Clean data according to schema
            cleaned_record = {
                k.strip(): ('' if v is None else str(v).strip())
                for k, v in record.items()
                if k in self.schema
            }
            
            # Map unique ID if specified
            cleaned_record = self._map_unique_id(record, cleaned_record)
            if cleaned_record is None:
                return None
                
            # Apply all processors
            for processor in self.processors:
                cleaned_record = processor.process(cleaned_record)
                
            return cleaned_record
            
        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            return None

    @abstractmethod
    def read_data(self, source: Any) -> Generator[Dict[str, Any], None, None]:
        """Read data from the input source"""
        pass

    def ingest(self, source: Any, batch_size: int = 50) -> List[Dict[str, Any]]:
        """
        Ingest data from the source
        
        Args:
            source: The input data source
            batch_size: Number of records to process in each batch
            
        Returns:
            List of failed records
        """
        batch = []
        failed_records = []
        
        with Session(self.engine) as session:
            try:
                for record in self.read_data(source):
                    processed_record = self.process_record(record)
                    if processed_record:
                        batch.append(processed_record)
                        
                        if len(batch) >= batch_size:
                            try:
                                self._process_batch(batch, session)
                            except Exception as e:
                                logger.error(f"Batch processing failed: {str(e)}")
                                failed_records.extend(batch)
                            finally:
                                batch = []
                
                # Process remaining records
                if batch:
                    try:
                        self._process_batch(batch, session)
                    except Exception as e:
                        logger.error(f"Final batch processing failed: {str(e)}")
                        failed_records.extend(batch)
                
                session.commit()
                
            except Exception as e:
                session.rollback()
                logger.error(f"Error during ingestion: {str(e)}")
                raise
                
        return failed_records

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup when used as context manager"""
        for processor in self.processors:
            try:
                processor.cleanup()
            except Exception as e:
                logger.error(f"Error during processor cleanup: {str(e)}") 

    def _process_batch(self, batch: List[Dict[str, Any]], session: Session) -> List[int]:
        """
        Process and insert a batch of records
        
        Args:
            batch: List of records to process
            session: Database session
            
        Returns:
            List of record IDs
            
        Raises:
            Exception: If batch processing fails
        """
        try:
            # Insert batch and get IDs
            ids = self.database.insert_batch(self.table_name, batch)
            # schema = self.database.get_table_schema(self.table_name)
            # Send to API
            success = self.api_client.send_batch(
                [(id, record) for id, record in zip(ids, batch)],
                self.table_name
            )
            
            if not success:
                raise Exception("Failed to send batch to API")
            
            return ids
            
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            raise 