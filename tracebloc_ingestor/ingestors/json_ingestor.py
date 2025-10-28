"""JSON Data Ingestor Module.

This module provides a specialized ingestor for handling JSON files, with support for
both single-object and array-of-objects formats. It includes validation and type
conversion capabilities.
"""

from typing import Dict, Any, Generator, Optional, List
import json
import logging
from pathlib import Path

from .base import BaseIngestor
from ..database import Database
from ..api.client import APIClient
from ..utils.constants import RESET, RED, YELLOW
from ..validators import BaseValidator

logger = logging.getLogger(__name__)

__all__ = ["JSONIngestor"]


class JSONIngestor(BaseIngestor):
    """A specialized ingestor for JSON files.

    This ingestor extends the BaseIngestor to provide JSON file handling capabilities.
    It supports both single JSON objects and arrays of objects, with validation and
    type conversion according to the specified schema.

    Attributes:
        json_options: Additional options for JSON processing
    """

    def __init__(
        self,
        database: Database,
        api_client: APIClient,
        table_name: str,
        schema: Dict[str, str],
        max_retries: int = 3,
        json_options: Optional[Dict[str, Any]] = None,
        unique_id_column: Optional[str] = None,
        label_column: Optional[str] = None,
        intent: Optional[str] = None,
        annotation_column: Optional[str] = None,
        category: Optional[str] = None,
        data_format: Optional[str] = None,
        log_level: Optional[int] = None,
        validators: Optional[List[BaseValidator]] = None,
    ):
        """Initialize JSON Ingestor.

        Args:
            database: Database instance for data storage
            api_client: API client instance for data transmission
            table_name: Name of the target table
            schema: Database schema definition
            max_retries: Maximum number of retry attempts
            json_options: Additional options for JSON processing
            unique_id_column: Name of the column to use as unique identifier
            label_column: Name of the column to use as label
            intent: Is the data for training or testing
            annotation_column: Name of the column to use as annotation
            category: Category of the data
            data_format: Format of the data
            log_level: Level of the logger
            validators: List of validators to run before ingestion
        """
        super().__init__(
            database,
            api_client,
            table_name,
            schema,
            max_retries,
            unique_id_column,
            label_column,
            intent,
            annotation_column,
            category,
            data_format,
            log_level,
            validators,
        )
        self.json_options = json_options or {}
        logger.setLevel(log_level)

    def _validate_record(self, record: Dict[str, Any]) -> None:
        """Validate JSON record against schema.

        This method performs type validation for the JSON record according to the
        specified schema. It handles common data types including integers, floats,
        and booleans.

        Args:
            record: JSON record to validate

        Raises:
            ValueError: If validation fails for any field
        """
        # Only validate fields that exist in both schema and record
        schema_fields = set(self.schema.keys())
        record_fields = set(record.keys())

        # Log which schema fields are not in the record (for information only)
        missing_fields = schema_fields - record_fields
        if missing_fields:
            logger.warning(
                f"{YELLOW}Schema fields not present in JSON record: {', '.join(missing_fields)}{RESET}"
            )

        # Validate unique_id_column exists if specified
        if self.unique_id_column and self.unique_id_column not in record:
            raise ValueError(
                f"{RED}Specified unique_id_column '{self.unique_id_column}' not found in record{RESET}"
            )

        # Basic data type validation - only for fields that exist in the record
        common_fields = schema_fields & record_fields
        for field in common_fields:
            value = record[field]
            dtype = self.schema[field]
            try:
                if "INT" in dtype.upper():
                    int(value) if value != "" else None
                elif "FLOAT" in dtype.upper():
                    float(value) if value != "" else None
                elif "BOOL" in dtype.upper():
                    bool(value) if value != "" else None
                # Add more type validations as needed
            except Exception as e:
                raise ValueError(
                    f"{RED}Data type validation failed for field {field}: {str(e)}{RESET}"
                )

    def read_data(self, file_path: str) -> Generator[Dict[str, Any], None, None]:
        """Read and validate JSON file.

        This method reads the JSON file and handles both single-object and
        array-of-objects formats. It performs validation according to the schema
        and yields records one at a time.

        Args:
            file_path: Path to the JSON file

        Yields:
            Dict containing record data

        Raises:
            FileNotFoundError: If the JSON file doesn't exist
            ValueError: If the JSON data is not in the expected format
            json.JSONDecodeError: If there's an error parsing the JSON
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"{RED}JSON file not found: {file_path}{RESET}")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
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
                        logger.warning(
                            f"{YELLOW}Skipping invalid record: {record}{RESET}"
                        )
                        continue

                    try:
                        self._validate_record(record)
                        yield record  # Let base class handle the cleaning and unique ID mapping
                    except ValueError as e:
                        logger.warning(
                            f"{YELLOW}Skipping invalid record: {str(e)}{RESET}"
                        )
                        continue

        except json.JSONDecodeError as e:
            logger.error(f"{RED}Error parsing JSON file: {str(e)}{RESET}")
            raise

        except Exception as e:
            logger.error(f"{RED}Unexpected error reading JSON: {str(e)}{RESET}")
            raise

    def _count_records(self, file_path: str) -> Optional[int]:
        """Count total records in JSON file efficiently.

        This method provides an optimized way to count records in a JSON file
        by loading the file once and checking its structure.

        Args:
            file_path: Path to the JSON file

        Returns:
            Total number of records if countable, None otherwise
        """
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return 1
                elif isinstance(data, list):
                    return len(data)
            return None
        except Exception as e:
            logger.debug(f"{YELLOW}Unable to count JSON records: {str(e)}{RESET}")
            return None

    def ingest(self, file_path: str, batch_size: int = 50) -> List[Dict[str, Any]]:
        """Ingest JSON file with progress tracking.

        This method extends the base ingest method to add JSON-specific logging
        and error handling.

        Args:
            file_path: Path to the JSON file
            batch_size: Size of each batch for processing

        Returns:
            List of failed records

        Raises:
            Exception: If ingestion fails
        """
        logger.info(f"Starting JSON ingestion from {file_path}")

        try:
            failed_records = super().ingest(file_path, batch_size)

            logger.info(
                f"JSON ingestion completed. " f"Failed records: {len(failed_records)}"
            )

            return failed_records

        except Exception as e:
            logger.error(f"{RED}JSON ingestion failed: {str(e)}{RESET}")
            raise
