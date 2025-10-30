"""CSV Data Ingestor Module.

This module provides a specialized ingestor for handling CSV files, with optimized
pandas-based reading and validation capabilities.
"""

from typing import Dict, Any, Generator, Optional, List
import pandas as pd
import logging
from pathlib import Path

from .base import BaseIngestor
from ..database import Database
from ..api.client import APIClient
from ..utils.constants import RESET, RED, YELLOW
from ..config import Config

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


__all__ = ["Ingestor"]


class CSVIngestor(BaseIngestor):
    """A specialized ingestor for CSV files.

    This ingestor extends the BaseIngestor to provide optimized CSV file handling
    using pandas. It includes features for efficient chunked reading, data validation,
    and type conversion.

    Attributes:
        csv_options: Additional options for pandas read_csv
    """

    def __init__(
        self,
        database: Database,
        api_client: APIClient,
        table_name: str,
        schema: Dict[str, str] = {},
        max_retries: int = 3,
        csv_options: Optional[Dict[str, Any]] = None,
        file_options: Optional[Dict[str, Any]] = None,
        unique_id_column: Optional[str] = None,
        label_column: Optional[str] = None,
        intent: Optional[str] = None,
        annotation_column: Optional[str] = None,
        category: Optional[str] = None,
        data_format: Optional[str] = None,
    ):
        """Initialize CSV Ingestor.

        Args:
            database: Database instance for data storage
            api_client: API client instance for data transmission
            table_name: Name of the target table
            schema: Database schema definition
            max_retries: Maximum number of retry attempts
            csv_options: Additional options for pandas read_csv
            file_options: Additional options for file processing
            unique_id_column: Name of the column to use as unique identifier
            label_column: Name of the column to use as label
            intent: Is the data for training or testing
            annotation_column: Name of the column to use as annotation
            category: Category of the data
            data_format: Format of the data
            log_level: Level of the logger
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
            file_options,
        )
        self.csv_options = csv_options or {}
        self.file_options = file_options or {}

    def _validate_csv(self, df: pd.DataFrame) -> None:
        """Validate CSV data against schema using pandas functionality.

        This method performs type validation and conversion for the CSV data
        according to the specified schema. It handles common data types including
        integers, floats, booleans, dates, and strings.

        Args:
            df: Pandas DataFrame to validate

        Raises:
            ValueError: If validation fails for any column
        """
        # Only validate columns that exist in both schema and CSV
        common_columns = set(self.schema.keys()) & set(df.columns)

        # Log which schema columns are not in the CSV (for information only)
        missing_columns = set(self.schema.keys()) - set(df.columns)
        if missing_columns:
            raise ValueError(
                f"{RED}Schema columns not present in CSV: {', '.join(missing_columns)}{RESET}"
            )

        # Type validation using pandas dtypes - only for columns that exist in the CSV
        for column in common_columns:
            dtype = self.schema[column]
            try:
                if "INT" in dtype.upper():
                    df[column] = pd.to_numeric(df[column], downcast="integer")
                elif "FLOAT" in dtype.upper():
                    df[column] = pd.to_numeric(df[column], downcast="float")
                elif "BOOL" in dtype.upper():
                    df[column] = df[column].astype("boolean")
                elif "DATE" in dtype.upper():
                    df[column] = pd.to_datetime(df[column])
                elif "STRING" in dtype.upper() or "TEXT" in dtype.upper():
                    df[column] = df[column].astype("string")
            except Exception as e:
                raise ValueError(
                    f"{RED}Data type validation failed for column {column}: {str(e)}{RESET}"
                )

    def read_data(self, file_path: str) -> Generator[Dict[str, Any], None, None]:
        """Read and validate CSV file using pandas optimizations.

        This method reads the CSV file in chunks for memory efficiency and performs
        validation according to the schema. It uses pandas' optimized C engine for
        better performance.

        Args:
            file_path: Path to the CSV file

        Yields:
            Dict containing record data

        Raises:
            FileNotFoundError: If the CSV file doesn't exist
            ValueError: If the unique_id_column is not found in the CSV
            pd.errors.ParserError: If there's an error parsing the CSV
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"{RED}CSV file not found: {file_path}{RESET}")

        try:
            chunk_size = self.csv_options.pop("chunk_size", 1000)

            # Enhanced default options for pandas
            default_options = {
                "dtype": None,  # Let pandas infer types initially
                "keep_default_na": False,
                "na_values": [""],
                "encoding": "utf-8",
                "on_bad_lines": "warn",
                "low_memory": False,  # Prevent mixed type inference warnings
                "engine": "c",  # Use faster C engine
            }

            csv_options = {**default_options, **self.csv_options}

            first_chunk = True
            for chunk in pd.read_csv(file_path, chunksize=chunk_size, **csv_options):
                if first_chunk:
                    chunk.columns = chunk.columns.str.strip()
                    self._validate_csv(chunk)
                    if (
                        self.unique_id_column
                        and self.unique_id_column not in chunk.columns
                    ):
                        raise ValueError(
                            f"{RED}Specified unique_id_column '{self.unique_id_column}' not found in CSV{RESET}"
                        )
                    first_chunk = False

                # Process each row efficiently using itertuples instead of iterrows
                for row in chunk.itertuples(index=False, name=None):
                    record = dict(zip(chunk.columns, row))
                    yield record

        except pd.errors.EmptyDataError:
            logger.warning(f"{YELLOW}Empty CSV file: {file_path}{RESET}")
            return

        except (pd.errors.ParserError, Exception):
            raise

    def ingest(self, file_path: str, batch_size: int = 50) -> List[Dict[str, Any]]:
        """Ingest CSV file with progress tracking.

        This method extends the base ingest method to add CSV-specific logging
        and error handling.

        Args:
            file_path: Path to the CSV file
            batch_size: Size of each batch for processing

        Returns:
            List of failed records

        Raises:
            Exception: If ingestion fails
        """
        logger.info(f"Starting CSV ingestion from {file_path}")

        try:
            failed_records = super().ingest(file_path, batch_size)

            logger.info(
                f"CSV ingestion completed. " f"Failed records: {len(failed_records)}"
            )

            return failed_records

        except Exception as e:
            raise

    def _count_records(self, file_path: str) -> Optional[int]:
        """Count total records in CSV file efficiently using pandas.

        This method provides an optimized way to count records in a CSV file
        using pandas' efficient reading capabilities.

        Args:
            file_path: Path to the CSV file

        Returns:
            Total number of records if countable, None otherwise
        """
        try:
            # Use pandas to count lines efficiently
            return pd.read_csv(file_path).shape[0]
        except Exception as e:
            logger.debug(
                f"{YELLOW}Unable to count CSV records using pandas: {str(e)}{RESET}"
            )
            return None
