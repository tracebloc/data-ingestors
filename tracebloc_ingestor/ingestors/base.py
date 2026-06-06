from abc import ABC, abstractmethod
from typing import Dict, Any, Generator, List, Optional, NamedTuple
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
import logging
from tqdm import tqdm
import uuid
from pathlib import Path

from ..database import Database
from ..api.client import APIClient
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
from ..utils import label_policy as label_policy_module
from ..utils.validators_mapping import map_validators
from ..file_transfer import map_file_transfer

# Logger for this module. Level is set by `setup_logging()` on the root
# logger when the user script calls it; child loggers inherit that level.
logger = logging.getLogger(__name__)

__all__ = ["BaseIngestor", "IngestionSummary"]


# Tabular-family categories carry `number_of_columns` in file_options;
# image / text categories do not (a schema may still be supplied — e.g.
# keypoint_detection's "Visibility" column — but a column count there
# would be a misleading metric).
_TABULAR_FAMILY_CATEGORIES = frozenset({
    TaskCategory.TABULAR_CLASSIFICATION,
    TaskCategory.TABULAR_REGRESSION,
    TaskCategory.TIME_SERIES_FORECASTING,
    TaskCategory.TIME_TO_EVENT_PREDICTION,
})


class IngestionSummary(NamedTuple):
    """Data class to hold ingestion summary statistics.

    Attributes:
        total_records: Total number of records processed
        processed_records: Number of records successfully processed
        inserted_records: Number of records inserted into database
        api_sent_records: Number of records sent to API
        failed_records: Number of records that failed processing
        skipped_records: Number of records that were skipped for non-file
            reasons (e.g. missing label / invalid intent / processing error)
        file_transfer_failures: Number of records whose source file (image,
            annotation, mask, text) was missing or unreadable, so the
            record was dropped before the DB / API write. Tracked
            separately from ``skipped_records`` so operators can
            distinguish data-loss from validation skips (issue #99).
    """

    ingestor_id: str
    total_records: int
    processed_records: int
    inserted_records: int
    api_sent_records: int
    failed_records: int
    skipped_records: int
    file_transfer_failures: int = 0

    @property
    def has_failures(self) -> bool:
        """True if any non-trivial failure occurred — DB insert short of
        total, API short of inserted, file-transfer skipped any record,
        or processing errored. Used to gate the "completed successfully"
        banner so customers can't mistake a partial run for a clean one.
        """
        return (
            self.failed_records > 0
            or self.file_transfer_failures > 0
            or self.inserted_records < self.total_records
            or self.api_sent_records < self.inserted_records
        )


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
        label_policy: str = label_policy_module.PASSTHROUGH,
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
            label_policy: ``"passthrough"`` (default; classification — the
                label value crosses the cluster boundary unchanged) or
                ``"bucket"`` (regression-class — each label is replaced
                with a stable hash-bucket ID before the API payload is
                built, so raw target values never leak). Schema-validated
                upstream by the YAML entrypoint; templates pass the
                appropriate constant from :mod:`tracebloc_ingestor.utils.label_policy`.
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
        self.label_policy = label_policy

        # Default behavior is UUID-generated data_id (no source column leaves
        # the cluster). Opting into source-column mapping is allowed but loud:
        # warn at startup naming the column whose values will be sent to the
        # central backend, so reviewers can audit the privacy implication.
        if self.unique_id_column:
            logger.warning(
                f"{YELLOW}Source-column data_id mapping enabled: values from "
                f"column '{self.unique_id_column}' will be sent to the central "
                f"backend as 'data_id'. To prevent source PII leakage (e.g. "
                f"patient_id, user_id), omit unique_id_column to use "
                f"server-side UUIDs instead.{RESET}"
            )
        
        # Remove label_column, annotation_column, and unique_id_column from schema
        # These are handled separately and should not be ingested as regular columns
        table_schema = schema.copy()
        if self.label_column and self.label_column in table_schema:
            del table_schema[self.label_column]
        if self.annotation_column and self.annotation_column in table_schema:
            del table_schema[self.annotation_column]
        if self.unique_id_column and self.unique_id_column in table_schema:
            del table_schema[self.unique_id_column]

        # Add cleaned schema to file_options for validators / downstream metadata.
        # Always overwrite so a schema passed in by the template (which may still
        # contain the label/annotation/unique_id columns) is sanitized before
        # being sent to the backend as part of meta_data.
        if schema:
            self.file_options["schema"] = table_schema
            # number_of_columns is only meaningful for tabular-family
            # categories — that's where the validator + backend metadata
            # consume it. Image categories may also carry a schema (e.g.
            # keypoint_detection's "Visibility" column) but the count
            # would be misleading there, so don't inject it.
            if self.category in _TABULAR_FAMILY_CATEGORIES:
                self.file_options["number_of_columns"] = len(table_schema)

        # Ensure table exists
        self.table = self.database.create_table(table_name, table_schema)

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
            # Apply the configured label policy at the latest possible moment
            # before the API client builds its payload. For classification-class
            # categories ``label_policy="passthrough"`` is a no-op; for
            # regression-class categories ``"bucket"`` replaces the raw target
            # with a stable hash-bucket ID so the value never leaks to the
            # central backend (#44 / parent client#85).
            cleaned_record["label"] = label_policy_module.apply(
                record.get(self.label_column), self.label_policy
            )

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
            # Clean data according to schema, excluding label_column, annotation_column, and unique_id_column
            # These are handled separately and should not be ingested as regular columns
            columns_to_exclude = set()
            if self.label_column:
                columns_to_exclude.add(self.label_column)
            if self.annotation_column:
                columns_to_exclude.add(self.annotation_column)
            if self.unique_id_column:
                columns_to_exclude.add(self.unique_id_column)
            cleaned_record = {
                k.strip(): ("" if v is None else str(v).strip())
                for k, v in record.items()
                if k in self.schema and k not in columns_to_exclude
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

    @staticmethod
    def _check_csv_encoding(source: Any) -> None:
        """Fail fast with a clear message if a CSV source is not valid UTF-8.

        Every validator reads CSVs as UTF-8 and swallows decode errors into a
        misleading "No data found"; a non-UTF-8 export (e.g. a Latin-1/Windows
        CSV with umlauts) would otherwise crash or mislead. Probe once, up front.
        """
        if not isinstance(source, (str, Path)):
            return
        path = Path(source)
        if path.suffix.lower() != ".csv" or not path.exists():
            return
        try:
            with open(path, "r", encoding="utf-8") as fh:
                while fh.read(1 << 20):  # decode in 1 MB chunks; raises on a bad byte
                    pass
        except UnicodeDecodeError as exc:
            raise ValueError(
                f"{RED}'{path.name}' is not valid UTF-8 — a non-UTF-8 byte was found at "
                f"byte {exc.start}. Re-save the file as UTF-8 (in Excel: Save As → "
                f"'CSV UTF-8 (Comma delimited)'), then re-ingest.{RESET}"
            ) from exc

    def validate_data(self, source: Any) -> bool:
        """Validate data before ingestion using configured validators.

        Args:
            source: The data source to validate

        Returns:
            True if all validations pass, False otherwise

        Raises:
            ValueError: If validation fails
        """
        # Pre-flight: a non-UTF-8 CSV otherwise surfaces as a misleading
        # "No data found" (validators read UTF-8 and swallow decode errors).
        # Catch it once here with a clear, actionable message.
        self._check_csv_encoding(source)

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
            "file_transfer_failures": 0,
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
                                TaskCategory.SEMANTIC_SEGMENTATION,
                                TaskCategory.KEYPOINT_DETECTION,
                                TaskCategory.MASKED_LANGUAGE_MODELING,
                            ]:
                                processed_record = map_file_transfer(
                                    self.category, processed_record, self.file_options
                                )
                                # Skip record if file transfer failed. Tracked as
                                # `file_transfer_failures` (not `skipped_records`)
                                # so the summary can flag the silent-data-loss
                                # pattern from issue #99 — a missing source
                                # would otherwise let the DB / API write succeed
                                # and falsely report 100% success.
                                if processed_record is None:
                                    stats["file_transfer_failures"] += 1
                                    filename = record.get("filename", "Unknown")
                                    logger.warning(
                                        f"Skipping record due to file transfer failure: {filename}"
                                    )
                                    # Also surface the failure to the caller
                                    # so cli.run.main exits non-zero — without
                                    # this, a 100%-failed run would still
                                    # return [] and the K8s job marker would
                                    # be `Succeeded` (the silent-data-loss
                                    # pattern from #99).
                                    failed_records.append(
                                        {
                                            "record": record,
                                            "error": "file_transfer_failed",
                                        }
                                    )
                                    # Advance the progress bar so an
                                    # all-transfer-failure run doesn't leave
                                    # tqdm stuck at 0/N — without this the
                                    # `continue` skips the batch update that
                                    # would normally tick the bar.
                                    pbar.update(1)
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
        """Log ingestion summary in a clear, formatted way with enhanced visual appeal.

        A "success" here means the record was inserted to the DB, sent to
        the API, AND its sidecar file (image / annotation / mask / text)
        was copied to the destination. Records whose source file was
        missing are subtracted from the success count even if the DB
        row landed — see issue #99 for the silent-data-loss pattern that
        motivated this.
        """

        # A successful record requires DB insert AND API send AND, where
        # applicable, a successful file transfer. File-transfer failures
        # short-circuit before the DB write (they're skipped from the
        # batch), so subtracting them from total_records gives the
        # denominator's effective ceiling. Use inserted_records (the
        # actual durable outcome) as the numerator.
        success_rate = 0
        if summary.total_records > 0:
            success_rate = (summary.inserted_records / summary.total_records) * 100

        # Determine overall status color
        status_color = (
            GREEN if success_rate >= 90 and not summary.has_failures
            else YELLOW if success_rate >= 70
            else RED
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
        file_transfer_color = (
            RED if summary.file_transfer_failures > 0 else GREEN
        )
        print(
            f"{BOLD}📁 File Transfer Failures:{RESET}  {file_transfer_color}{summary.file_transfer_failures:,}{RESET}"
        )
        print(
            f"{BOLD}❌ Failed DB Insertion:{RESET}     {RED}{summary.failed_records:,}{RESET}"
        )
        # Only count records that made it to a DB insert but didn't ship
        # to the API. Using `total_records - api_sent_records` would also
        # include file-transfer failures and DB failures (which never had
        # a chance to ship), giving an inflated, double-counted total.
        api_only_failures = max(
            0, summary.inserted_records - summary.api_sent_records
        )
        print(
            f"{BOLD}❌ Failed to Send to API:{RESET}   {RED}{api_only_failures:,}{RESET}"
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

        # Status banner. Any non-trivial failure (DB, API, or file-transfer)
        # disqualifies the "completed successfully" message — a customer
        # seeing 🎉 should be able to trust that no record was silently
        # dropped. The three failure channels are mutually exclusive per
        # record (file-transfer failures never reach DB; DB failures never
        # reach API; api_only_failures are records that hit DB but didn't
        # ship), so summing them gives a clean unique count instead of
        # the double-count `total_records - api_sent_records` would produce.
        total_failures = (
            summary.failed_records
            + summary.file_transfer_failures
            + api_only_failures
        )
        if not summary.has_failures:
            status_msg = "🎉 Ingestion completed successfully!"
        elif success_rate >= 80:
            status_msg = (
                f"⚠️  Ingestion completed with {total_failures:,} failure(s), "
                "see logs."
            )
        elif success_rate >= 60:
            status_msg = (
                f"⚠️  Ingestion completed with {total_failures:,} failure(s); "
                "many records failed to process — see logs."
            )
        else:
            status_msg = (
                f"❌ Critical! Ingestion completed with {total_failures:,} "
                "failure(s); most records failed — see logs."
            )

        print(f"{CYAN}{'─'*60}{RESET}")
        print(f"{BOLD}{status_color}{status_msg}{RESET}")
        print(f"{CYAN}{'═'*60}{RESET}\n")
