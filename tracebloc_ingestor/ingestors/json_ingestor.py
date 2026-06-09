"""JSON Data Ingestor Module.

This module provides a specialized ingestor for handling JSON files, with support for
both single-object and array-of-objects formats. It includes validation and type
conversion capabilities.
"""

from typing import Dict, Any, Generator, Optional, List
import json
import logging
import math
import re
from pathlib import Path

import pandas as pd

from .base import BaseIngestor
from ..database import Database
from ..api.client import APIClient
from ..utils.constants import RESET, RED, YELLOW
from ..utils import label_policy as label_policy_module

logger = logging.getLogger(__name__)

__all__ = ["JSONIngestor"]


# Boolean string forms DataValidator._validate_boolean accepts. Keep this list
# in lockstep with that validator so the JSON per-record check and the CSV
# preflight agree.
_VALID_BOOL_STRINGS = {
    "true", "false", "yes", "no", "y", "n", "t", "f", "1", "0", "1.0", "0.0"
}


def _validate_value_against_dtype(value: Any, dtype_upper: str) -> None:
    """Raise ValueError if ``value`` doesn't fit the declared MySQL dtype.

    Mirrors ``DataValidator``'s per-type rules so JSON and CSV give the same
    verdict on the same record (issue #189). Caller guarantees ``value`` is
    not None / "" (handled separately as NULL).
    """
    # Order matters: DATETIME / TIMESTAMP must match before DATE / TIME because
    # "DATE" and "TIME" are substrings of "DATETIME" / "TIMESTAMP".
    if "DATETIME" in dtype_upper or "TIMESTAMP" in dtype_upper:
        ts = pd.to_datetime(str(value), errors="coerce")
        if pd.isna(ts):
            raise ValueError(
                f"value {value!r} is not a valid {dtype_upper} (expected an "
                f"ISO 8601 date-time)"
            )
    elif "DATE" in dtype_upper or "TIME" in dtype_upper:
        ts = pd.to_datetime(str(value), errors="coerce")
        if pd.isna(ts):
            raise ValueError(
                f"value {value!r} is not a valid {dtype_upper}"
            )
    elif "BOOL" in dtype_upper:
        # ``bool(value)`` is truthy for any non-empty value, so "maybe" / 2
        # / "banana" all "passed" — match DataValidator._validate_boolean's
        # fixed vocabulary instead.
        if isinstance(value, bool):
            return
        if isinstance(value, (int, float)) and value in (0, 1):
            return
        if isinstance(value, str) and value.strip().lower() in _VALID_BOOL_STRINGS:
            return
        raise ValueError(
            f"value {value!r} is not a valid BOOLEAN (expected true/false, "
            f"yes/no, 1/0, or a recognised string form)"
        )
    elif "INT" in dtype_upper:
        # ``int(value)`` silently truncated 3.5 -> 3; require integer-valued
        # input. JSON booleans (a subclass of int) would slip through as
        # 0/1, so guard against the bool case separately.
        if isinstance(value, bool):
            raise ValueError(
                f"value {value!r} is a boolean, not an integer"
            )
        try:
            f = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"value {value!r} is not numeric")
        # Reject non-finite (inf / -inf / NaN) before is_integer(). inf and
        # NaN both return False from is_integer() in CPython today, so the
        # check below already rejects them — but make the guard explicit so
        # the contract doesn't depend on a CPython detail and so the error
        # message names the real problem (mirrors DataValidator's
        # ``_non_finite_error`` on the CSV path).
        if not math.isfinite(f):
            raise ValueError(
                f"value {value!r} is non-finite (inf/NaN) and cannot be "
                f"stored in an INT column"
            )
        if not f.is_integer():
            raise ValueError(
                f"value {value!r} is not an integer (would silently truncate)"
            )
    elif any(t in dtype_upper for t in ("FLOAT", "DOUBLE", "DECIMAL", "NUMERIC")):
        try:
            f = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"value {value!r} is not numeric")
        # Reject inf / -inf / NaN. ``float("Infinity")`` returns +inf
        # without raising, so the bare float() above lets non-finite values
        # through silently; DataValidator's FLOAT branch already rejects
        # them on the CSV path (``_non_finite_error``). Match that here so
        # JSON and CSV give the same verdict on the same record.
        if not math.isfinite(f):
            raise ValueError(
                f"value {value!r} is non-finite (inf/NaN) and cannot be "
                f"stored in a numeric column"
            )
    elif any(t in dtype_upper for t in ("VARCHAR", "CHAR", "TEXT")):
        # MySQL binds any scalar as a string against a string column (see
        # issue #188), so accept ints/floats/bools too. The only constraint
        # is the declared length (when present) — and the only shape error
        # is a non-scalar container.
        if isinstance(value, (list, dict, set, tuple)):
            raise ValueError(
                f"value {value!r} is a non-scalar container; cannot be "
                f"stored as a {dtype_upper.split('(')[0]}"
            )
        m = re.search(r"\((\d+)\)", dtype_upper)
        if m and len(str(value)) > int(m.group(1)):
            raise ValueError(
                f"value {value!r} exceeds the declared length "
                f"{m.group(1)} (got {len(str(value))} characters)"
            )


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
        file_options: Optional[Dict[str, Any]] = None,
        log_level: Optional[int] = None,
        label_policy: str = label_policy_module.PASSTHROUGH,
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
            file_options: Options passed to the validator set resolved by
                ``map_validators(category, file_options)``. For
                ``time_to_event_prediction`` this carries ``time_column``;
                for image categories it carries ``target_size`` / ``extension``.
            log_level: Level of the logger
            label_policy: Bucketing policy for the label value before it's
                sent to the central backend. ``"passthrough"`` (default)
                for classification; ``"bucket"`` for regression-class.
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
            label_policy=label_policy,
        )
        self.json_options = json_options or {}
        if log_level is not None:
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

        # Per-record type validation. The previous implementation used
        # ``int(value)`` / ``float(value)`` / ``bool(value)`` to "check"
        # types — but Python's casts are far too permissive:
        #   bool("maybe")  -> True   (any non-empty string is truthy)
        #   bool(2)        -> True   (any non-zero int is truthy)
        #   int(3.5)       -> 3      (silent truncation, no error)
        # …so JSON ingestion silently accepted data the CSV path correctly
        # rejected (issue #189). Match the vocabulary DataValidator already
        # enforces at file load (the same one CSV uses), so the two formats
        # give the same verdict on the same record. NULL / "" are still
        # tolerated as missing (mirrors #170).
        common_fields = schema_fields & record_fields
        for field in common_fields:
            value = record[field]
            dtype_upper = self.schema[field].upper()
            if value is None or value == "":
                continue
            try:
                _validate_value_against_dtype(value, dtype_upper)
            except ValueError as e:
                raise ValueError(
                    f"{RED}Data type validation failed for field {field}: {e}{RESET}"
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
