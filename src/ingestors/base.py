from abc import ABC, abstractmethod
from typing import Dict, Any, Generator, List, Optional, Callable, NamedTuple
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import logging
from tqdm import tqdm
import os
from ..database import Database
from ..processors.base import BaseProcessor
from ..api.client import APIClient
from ..utils.logging import setup_logging
from ..config import Config
from ..utils.constants import DataCategory
import uuid

# Configure unified logging with config
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class IngestionSummary(NamedTuple):
    """Data class to hold ingestion summary statistics"""
    total_records: int
    processed_records: int
    inserted_records: int
    api_sent_records: int
    failed_records: int

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
                 annotation_column: Optional[str] = None,
                 category: Optional[str] = None
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
            label_column: Name of the column to use as label
            intent_column: Name of the column to use as intent
            annotation_column: Name of the column to use as annotation
            category: Category of the data
        Raises:
            ValueError: If unique_id_column is not provided
        """
        # TODO: Remove this once we have a way to handle unique_id_column
        # if not unique_id_column:
        #     raise ValueError(
        #         "unique_id_column must be specified. This column will be used to map records "
        #         "to their unique data_id in the database. Please provide the name of the column "
        #         "that contains unique identifiers in your data source."
        #     )
            
        self.ingestor_id = str(uuid.uuid4())  # Generate unique ID for this ingestor instance
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
        self.category = category
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
            logger.warning(f"Record {record} does not contain the required columns: {columns_not_found}")

        if self.label_column:
            cleaned_record['label'] = record.get(self.label_column)

        if self.intent_column:
            cleaned_record['data_intent'] = record.get(self.intent_column)

        if self.annotation_column:
            cleaned_record['annotation'] = record.get(self.annotation_column)

        if not self.unique_id_column:
            logger.warning("No unique ID column specified, generating unique ID mapping")
            cleaned_record['data_id'] = str(uuid.uuid4())
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
            
            # Add ingestor_id to the record
            cleaned_record['ingestor_id'] = self.ingestor_id
            return cleaned_record
            
        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            return None

    @abstractmethod
    def read_data(self, source: Any) -> Generator[Dict[str, Any], None, None]:
        """Read data from the input source"""
        pass

    def _count_records(self, source: Any) -> Optional[int]:
        """
        Try to count total records in the source for progress tracking.
        Subclasses should override this if they can provide a more efficient count.
        
        Args:
            source: The data source
            
        Returns:
            Total number of records if countable, None otherwise
        """
        try:
            # Default implementation tries to count by iterating
            return sum(1 for _ in self.read_data(source))
        except Exception as e:
            logger.debug(f"Unable to count records: {str(e)}")
            return None

    def ingest(self, source: Any, batch_size: int = 50) -> List[Dict[str, Any]]:
        """
        Ingest data from the source with progress tracking
        
        Args:
            source: The input data source
            batch_size: Number of records to process in each batch
            
        Returns:
            List of failed records
        """
        batch = []
        failed_records = []
        
        # Statistics tracking
        stats = {
            'total_records': 0,
            'processed_records': 0,
            'inserted_records': 0,
            'api_sent_records': 0,
            'failed_records': 0
        }
        
        # Try to get total count for progress bar
        total = self._count_records(source)
        stats['total_records'] = total or 0
        
        # Determine if we should show progress bar
        disable_progress = not os.isatty(0) or os.getenv('DISABLE_PROGRESS_BAR')
        
        with Session(self.engine) as session:
            try:
                pbar = tqdm(
                    total=total,
                    desc="Ingesting records",
                    unit="records",
                    disable=disable_progress
                )
                
                for record in self.read_data(source):
                    stats['total_records'] += 0 if total else 1  # Increment if total wasn't pre-counted
                    
                    processed_record = self.process_record(record)
                    if processed_record:
                        stats['processed_records'] += 1
                        batch.append(processed_record)
                        
                        if len(batch) >= batch_size:
                            try:
                                inserted_ids, api_success, db_failures = self._process_batch(batch, session)
                                # Only count records that were successfully inserted
                                if inserted_ids:
                                    stats['inserted_records'] += len(inserted_ids)
                                if api_success:
                                    stats['api_sent_records'] += len(inserted_ids)
                                if db_failures:
                                    stats['failed_records'] += len(db_failures)
                                    failed_records.extend(db_failures)
                            except Exception as e:
                                logger.error(f"Batch processing failed: {str(e)}")
                            finally:
                                pbar.update(len(batch))
                                batch = []
                    else:
                        stats['skipped_records'] += 1
                        pbar.update(1)  # Update progress bar for skipped records
                
                # Process remaining records
                if batch:
                    try:
                        inserted_ids, api_success, db_failures = self._process_batch(batch, session)
                        # Only count records that were successfully inserted
                        if inserted_ids:
                            stats['inserted_records'] += len(inserted_ids)
                        if api_success:
                            stats['api_sent_records'] += len(inserted_ids)
                        if db_failures:
                            stats['failed_records'] += len(db_failures)
                            failed_records.extend(db_failures)
                        pbar.update(len(batch))
                    except Exception as e:
                        logger.error(f"Final batch processing failed: {str(e)}")
                
                session.commit()
                pbar.close()


                # Send edge label metadata
                self.api_client.send_generate_edge_label_meta(self.table_name, self.ingestor_id)

                # schema dict
                schema_dict = self.database.get_table_schema(self.table_name)
                # Send global metadata
                self.api_client.send_global_meta_meta(self.table_name, schema_dict)


                # Prepare dataset
                self.api_client.prepare_dataset(self.category, self.ingestor_id)

                # create dataset
                self.api_client.create_dataset(category=self.category, ingestor_id=self.ingestor_id)

                # Create and log summary
                summary = IngestionSummary(**stats)

                self._log_summary(summary)
                
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
            ids, db_failures = self.database.insert_batch(self.table_name, batch)

            # Send to API with ingestor_id
            if ids:  # Only send to API if we have valid IDs
                api_success = self.api_client.send_batch(
                    [(id, record) for id, record in zip(ids, batch)],
                    self.table_name,
                    ingestor_id=self.ingestor_id  # Include ingestor_id in API requests
                )
            return ids if ids else [], api_success, db_failures  # Ensure we always return a list
            
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            raise

    def _log_summary(self, summary: IngestionSummary):
        """Log ingestion summary in a clear, formatted way"""

        logger.info("\n" + "="*50)
        logger.info("INGESTION SUMMARY")
        logger.info("="*50)
        logger.info(f"Total Records Found:     {summary.total_records:,}")
        logger.info(f"Successfully Processed:  {summary.processed_records:,}")
        logger.info(f"Inserted to Database:    {summary.inserted_records:,}")
        logger.info(f"Sent to API:            {summary.api_sent_records:,}")
        logger.info(f"Failed Records:          {summary.failed_records:,}")
        logger.info("="*50)
        
        # Calculate success rate
        if summary.total_records > 0:
            success_rate = (summary.inserted_records / summary.total_records) * 100
            logger.info(f"Success Rate: {success_rate:.2f}%")
        logger.info("="*50 + "\n") 