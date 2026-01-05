from abc import ABC, abstractmethod
from typing import Dict, Any, Generator, List, Optional, NamedTuple
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
import logging
from tqdm import tqdm
import uuid

from ..database import Database
from ..api.client import APIClient
from ..utils.logging import setup_logging
from ..config import Config
from ..utils.constants import (
    Intent,
    TaskCategory,
    RESET,
    BOLD,
    GREEN,
    RED,
    YELLOW,
    BLUE,
    CYAN,
)
from ..utils.validators_mapping import map_validators
from ..file_transfer import map_file_transfer

# Configure unified logging with config
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

__all__ = ["BaseIngestor", "IngestionSummary"]


class IngestionSummary(NamedTuple):
    """Data class to hold ingestion summary statistics.

    Attributes:
        total_records: Total number of records processed
        processed_records: Number of records successfully processed
        inserted_records: Number of records inserted into database
        api_sent_records: Number of records sent to API
        failed_records: Number of records that failed processing
        skipped_records: Number of records that were skipped
    """

    ingestor_id: str
    total_records: int
    processed_records: int
    inserted_records: int
    api_sent_records: int
    failed_records: int
    skipped_records: int


class BaseIngestor(ABC):
    """Base class for all data ingestors.

    This abstract base class provides the core functionality for ingesting data from various sources
    into a database and optionally sending it to an API. It handles batching, retries, and progress tracking.

    Attributes:
        ingestor_id: Unique identifier for this ingestor instance
        database: Database instance for data storage
        engine: SQLAlchemy engine instance
        api_client: API client for sending data
        table_name: Name of the target database table
        schema: Database schema definition
        max_retries: Maximum number of retry attempts
        unique_id_column: Column name for unique identifiers
        label_column: Column name for labels
        intent: Data intent (training/testing)
        annotation_column: Column name for annotations
        category: Data category
    """

    def __init__(
        self,
        database: Database,
        api_client: APIClient,
        table_name: str,
        schema: Dict[str, str] = {},
        max_retries: int = 3,
        unique_id_column: Optional[str] = None,
        label_column: Optional[str] = None,
        intent: Optional[str] = None,
        annotation_column: Optional[str] = None,
        category: Optional[str] = None,
        data_format: Optional[str] = None,
        file_options: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the base ingestor.

        Args:
            database: Database instance for data storage
            api_client: API client instance for data transmission
            table_name: Name of the target table
            schema: Database schema definition
            max_retries: Maximum number of retry attempts
            unique_id_column: Name of the column to use as unique identifier
            label_column: Name of the column to use as label
            intent: Is the data for training or testing
            annotation_column: Name of the column to use as annotation
            category: Category of the data
            data_format: Format of the data
            file_options: File options to run before ingestion
        Raises:
            ValueError: If unique_id_column is not provided
        """
        self.ingestor_id = str(uuid.uuid4())
        self.database = database
        self.engine: Engine = database.engine
        self.api_client = api_client
        self.table_name = table_name
        self.schema = schema
        self.max_retries = max_retries
        self.unique_id_column = unique_id_column
        self.label_column = label_column
        self.intent = intent
        self.annotation_column = annotation_column
        self.category = category
        self.data_format = data_format
        self.file_options = file_options or {}
        
        # Add schema to file_options for validators if not already present
        if schema and "schema" not in self.file_options:
            self.file_options["schema"] = schema

        # Ensure table exists
        self.table = self.database.create_table(table_name, schema)

    def _map_unique_id(
        self, record: Dict[str, Any], cleaned_record: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Maps the unique ID from the source record to data_id in the cleaned record.

        Args:
            record: Original record with all fields
            cleaned_record: Processed record with schema fields

        Returns:
            Updated cleaned record if valid, None if invalid unique ID
        """

        # validate intent is valid
        if not self.intent or self.intent not in Intent.get_all_intents():
            logger.warning(
                f"Invalid intent: {self.intent}. Must be one of: {Intent.get_all_intents()}"
            )
            return None

        # Validate label_column exists if specified
        columns_to_validate = [
            (self.label_column, "label_column"),
            (self.annotation_column, "annotation_column"),
        ]
        columns_not_found = False
        for column, column_name in columns_to_validate:
            if column and column not in record:
                logger.warning(
                    f"Specified {column_name} '{column}' not found in record"
                )
                columns_not_found = True

        if columns_not_found:
            logger.warning(
                f"Record {record} does not contain the required columns: {columns_not_found}"
            )

        if self.label_column:
            cleaned_record["label"] = record.get(self.label_column)

        if self.intent:
            cleaned_record["data_intent"] = self.intent

        if self.annotation_column:
            cleaned_record["annotation"] = record.get(self.annotation_column)

        if not self.unique_id_column:
            # logger.warning("No unique ID column specified, generating unique ID mapping")
            cleaned_record["data_id"] = str(uuid.uuid4())
            return cleaned_record

        unique_id = record.get(self.unique_id_column)
        if unique_id is not None and str(unique_id).strip():
            cleaned_record["data_id"] = str(unique_id).strip()
            return cleaned_record
        else:
            logger.warning(f"Missing or invalid unique ID for record: {record}")
            return None

    def process_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single record"""
        try:
            # Clean data according to schema
            cleaned_record = {
                k.strip(): ("" if v is None else str(v).strip())
                for k, v in record.items()
                if k in self.schema
            }
            # Map unique ID if specified
            cleaned_record = self._map_unique_id(record, cleaned_record)

            logger.info(f"Cleaned record: {cleaned_record}")

            if cleaned_record is None:
                return None

            # Add ingestor_id to the record
            cleaned_record["ingestor_id"] = self.ingestor_id
            cleaned_record["filename"] = record.get("filename")
            cleaned_record["extension"] = record.get("extension")
            return cleaned_record

        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            return None

    def validate_data(self, source: Any) -> bool:
        """Validate data before ingestion using configured validators.

        Args:
            source: The data source to validate

        Returns:
            True if all validations pass, False otherwise

        Raises:
            ValueError: If validation fails
        """

        validators = map_validators(self.category, self.file_options)
        logger.info(f"Running {len(validators)} validator(s) on data source")
        all_valid = True
        validation_errors = []

        for validator in validators:
            try:
                logger.info(f"{CYAN}Running validator: {validator.name}{RESET}")
                result = validator.validate(source)

                if not result.is_valid:
                    all_valid = False
                    validation_errors.append(
                        f"{BOLD}{validator.name} Validator failed: {RESET} \n {RED}"
                    )
                    validation_errors.extend(result.errors)
                    validation_errors.append(f"{RESET}")

                # Log warnings if any
                for warning in result.warnings:
                    logger.warning(
                        f"{YELLOW}Validation warning - {validator.name}: {warning}{RESET}"
                    )
                if result.is_valid:
                    print(
                        f"{GREEN}{validator.name} Validator successfully passed{RESET}"
                    )
            except Exception as e:
                all_valid = False
                validation_errors.append(f"Validator {validator.name} error: {str(e)}")

        if not all_valid:
            error_summary = "\n".join(validation_errors)
            raise ValueError(f"{RED}{error_summary}{RESET}")

        print(f"{GREEN}All validations passed successfully{RESET}")
        return True

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
        # Validate data before ingestion
        logger.info(f"{CYAN}Starting data validation before ingestion...{RESET}")
        try:
            self.validate_data(f"{source}")
            logger.info(f"{GREEN}Data validation completed successfully{RESET}")
        except ValueError as e:
            raise e
        except Exception as e:
            raise e

        batch = []
        failed_records = []

        # Statistics tracking
        stats = {
            "ingestor_id": self.ingestor_id,
            "total_records": 0,
            "processed_records": 0,
            "inserted_records": 0,
            "api_sent_records": 0,
            "failed_records": 0,
            "skipped_records": 0,
        }

        # Try to get total count for progress bar
        total = self._count_records(source)
        stats["total_records"] = total or 0

        with Session(self.engine) as session:
            try:
                pbar = tqdm(total=total, desc="Ingesting records", unit="records")

                for record in self.read_data(source):
                    stats["total_records"] += 0 if total else 1

                    try:
                        processed_record = self.process_record(record)
                        if processed_record:
                            stats["processed_records"] += 1

                            if self.category in [
                                TaskCategory.IMAGE_CLASSIFICATION,
                                TaskCategory.OBJECT_DETECTION,
                                TaskCategory.TEXT_CLASSIFICATION,
                            ]:
                                processed_record = map_file_transfer(
                                    self.category, processed_record, self.file_options
                                )
                                # Skip record if file transfer failed
                                if processed_record is None:
                                    stats["skipped_records"] += 1
                                    logger.warning(
                                        f"Skipping record due to file transfer failure: {record.get('filename', 'Unknown')}"
                                    )
                                    continue

                            batch.append(processed_record)

                            if len(batch) >= batch_size:
                                try:
                                    inserted_ids, api_success, db_failures = (
                                        self._process_batch(batch, session)
                                    )
                                    # Only count records that were successfully inserted
                                    if inserted_ids:
                                        stats["inserted_records"] += len(inserted_ids)
                                    if api_success:
                                        stats["api_sent_records"] += len(inserted_ids)
                                    if db_failures:
                                        stats["failed_records"] += len(db_failures)
                                        failed_records.extend(db_failures)
                                except Exception as e:
                                    logger.error(f"Batch processing failed: {str(e)}")
                                finally:
                                    pbar.update(len(batch))
                                    batch = []
                        else:
                            stats["skipped_records"] += 1
                            pbar.update(1)  # Update progress bar for skipped records
                    except Exception as e:
                        # Count processing errors (including missing columns) as failed records
                        stats["failed_records"] += 1
                        failed_records.append({"record": record, "error": str(e)})
                        pbar.update(1)

                # Process remaining records
                if batch:
                    try:
                        inserted_ids, api_success, db_failures = self._process_batch(
                            batch, session
                        )
                        # Only count records that were successfully inserted
                        if inserted_ids:
                            stats["inserted_records"] += len(inserted_ids)
                        if api_success:
                            stats["api_sent_records"] += len(inserted_ids)
                        if db_failures:
                            stats["failed_records"] += len(db_failures)
                            failed_records.extend(db_failures)
                        pbar.update(len(batch))
                    except Exception as e:
                        logger.error(f"Final batch processing failed: {str(e)}")

                session.commit()
                pbar.close()

                # Send edge label metadata
                if self.api_client.send_generate_edge_label_meta(
                    self.table_name, self.ingestor_id, self.intent
                ):

                    # schema dict
                    schema_dict = self.database.get_table_schema(self.table_name)
                    add_info = self.file_options
                    # Send global metadata
                    if self.api_client.send_global_meta_meta(
                        self.table_name, schema_dict, add_info
                    ):

                        # Prepare dataset
                        if self.api_client.prepare_dataset(
                            self.category,
                            self.ingestor_id,
                            self.data_format,
                            self.intent,
                        ):

                            self.api_client.create_dataset(
                                category=self.category, ingestor_id=self.ingestor_id
                            )

                            # Create and log summary
                            summary = IngestionSummary(**stats)

                            self._log_summary(summary)

            except Exception as e:
                session.rollback()
                logger.error(f"Error during ingestion: {str(e)}")
                raise e

        return failed_records

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup when used as context manager"""
        pass

    def _process_batch(
        self, batch: List[Dict[str, Any]], session: Session
    ) -> List[int]:
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
            api_success = False
            # Send to API with ingestor_id
            if ids:  # Only send to API if we have valid IDs
                api_success = self.api_client.send_batch(
                    [(id, record) for id, record in zip(ids, batch)],
                    self.table_name,
                    ingestor_id=self.ingestor_id,  # Include ingestor_id in API requests
                )
            return (
                ids if ids else [],
                api_success,
                db_failures,
            )  # Ensure we always return a list

        except Exception as e:
            logger.error(f"{RED}Error processing batch: {str(e)}{RESET}")
            if hasattr(e.response, "text"):
                logger.error(f"{RED}Error response: {e.response.text}{RESET}")
            raise

    def _log_summary(self, summary: IngestionSummary):
        """Log ingestion summary in a clear, formatted way with enhanced visual appeal"""

        # Calculate success rate
        success_rate = 0
        if summary.total_records > 0:
            success_rate = (summary.inserted_records / summary.total_records) * 100

        # Determine overall status color
        status_color = (
            GREEN if success_rate >= 90 else YELLOW if success_rate >= 70 else RED
        )

        print(f"\n{CYAN}{'═'*60}{RESET}")
        print(f"{BOLD}{CYAN}📊 INGESTION SUMMARY 📊{RESET}")
        print(f"{CYAN}{'═'*60}{RESET}")
        print(
            f"{BOLD}Ingestor ID:{RESET}                {BLUE}{summary.ingestor_id}{RESET}"
        )
        # Main statistics with icons and colors
        print(
            f"{BOLD}📈 Total Records Found:{RESET}     {BLUE}{summary.total_records:,}{RESET}"
        )
        print(
            f"{BOLD}✅ Successfully Processed:{RESET}  {GREEN}{summary.processed_records:,}{RESET}"
        )
        print(
            f"{BOLD}💾 Inserted to Database:{RESET}    {GREEN}{summary.inserted_records:,}{RESET}"
        )
        print(
            f"{BOLD}🚀 Sent to API:{RESET}             {GREEN}{summary.api_sent_records:,}{RESET}"
        )
        print(
            f"{BOLD}⏭️  Skipped Records:{RESET}        {YELLOW}{summary.skipped_records:,}{RESET}"
        )
        print(
            f"{BOLD}❌ Failed DB Insertion:{RESET}     {RED}{summary.failed_records:,}{RESET}"
        )
        print(
            f"{BOLD}❌ Failed to Send to API:{RESET}   {RED}{(summary.total_records - summary.api_sent_records):,}{RESET}"
        )
        print(f"{CYAN}{'─'*60}{RESET}")

        # Success rate with visual indicator
        if summary.total_records > 0:
            # Progress bar
            bar_length = 30
            filled_length = int(bar_length * success_rate / 100)
            bar = "█" * filled_length + "░" * (bar_length - filled_length)
            print(
                f"{BOLD}📊 Success Rate:{RESET} [{status_color}{bar}{RESET}] {status_color}{success_rate:.1f}%{RESET}"
            )

        # Status message
        if success_rate >= 95:
            status_msg = "🎉 Ingestion completed successfully!"
        elif success_rate >= 80:
            status_msg = "👍 Most records processed successfully."
        elif success_rate >= 60:
            status_msg = "⚠️  Some records failed to process."
        else:
            status_msg = "❌ Critical! Many records failed to process."

        print(f"{CYAN}{'─'*60}{RESET}")
        print(f"{BOLD}{status_color}{status_msg}{RESET}")
        print(f"{CYAN}{'═'*60}{RESET}\n")
