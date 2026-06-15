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

import ijson
import pandas as pd


def _peek_json_shape(path: Path) -> Optional[str]:
    """Detect whether a JSON file is a single object or an array of
    objects by peeking at the first non-whitespace character.

    Returns ``"array"`` for ``[...]``, ``"object"`` for ``{...}``, or
    None if the file is empty / starts with anything else (the caller
    will raise the right error). The peek reads at most a few hundred
    bytes — bounded by the leading whitespace.

    OSError is intentionally NOT caught here (#222 bugbot): a permission-
    denied / unreadable file used to be swallowed into None, then
    ``read_data`` raised a misleading ``ValueError("object or array")``
    instead of the underlying I/O error. Let the caller see the real
    cause — ``read_data`` already had a ``FileNotFoundError`` guard
    before this function runs, and any other OSError is a real problem
    the user needs to know about.
    """
    with open(path, "rb") as f:
        # Read in small chunks until we see a non-whitespace byte.
        buf = b""
        while True:
            chunk = f.read(1024)
            if not chunk:
                break
            buf += chunk
            stripped = buf.lstrip()
            if stripped:
                first = stripped[:1]
                if first == b"[":
                    return "array"
                if first == b"{":
                    return "object"
                return None  # neither — let downstream raise
            if len(buf) > 65536:
                # Defensive: 64 KB of leading whitespace is pathological;
                # bail rather than read the whole file.
                return None
    return None

from .base import BaseIngestor
from ..database import Database
from ..api.client import APIClient
from ..utils.constants import RESET, RED, YELLOW
from ..utils import label_policy as label_policy_module
from ..utils import coercion

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
        # vocabulary instead. That validator also accepts string forms that
        # ``pd.to_numeric`` maps to 0 or 1 (e.g. "00", "01", "1.0", "0.0"),
        # so we try numeric coercion as a fallback before failing — keeps
        # JSON and CSV in lockstep on the same input (#204 bugbot).
        if isinstance(value, bool):
            return
        if isinstance(value, (int, float)) and value in (0, 1):
            return
        if isinstance(value, str):
            s = value.strip().lower()
            if s in _VALID_BOOL_STRINGS:
                return
            # Numeric-coercible strings ("00", "01", "1.0", "0.0", "1e0", …)
            # that resolve to 0 or 1 are accepted by DataValidator; mirror that.
            num = pd.to_numeric(s, errors="coerce")
            if not pd.isna(num) and num in (0, 1):
                return
        raise ValueError(
            f"value {value!r} is not a valid BOOLEAN (expected true/false, "
            f"yes/no, 1/0, or a recognised string form)"
        )
    elif "INT" in dtype_upper:
        # ``int(value)`` silently truncated 3.5 -> 3; require integer-valued
        # input. Python booleans are intentionally allowed: ``True``/``False``
        # are subclasses of int and ``DataValidator._validate_int`` accepts a
        # bool column via ``pd.to_numeric`` (True -> 1, False -> 0). Rejecting
        # them here would let a record pass CSV-style preflight and then be
        # dropped mid-ingest by this check — the silent-drop pathway #204
        # bugbot flagged. So a bool falls through to the numeric path below
        # (True.is_integer() is True via float coercion).
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
        # Reject values beyond signed 64-bit range with the same verdict the
        # CSV cast + DataValidator give (#236) — a value no MySQL integer type
        # can hold. The "declare BIGINT" hint is dropped when the column is
        # already BIGINT (its ceiling is int64 too).
        if coercion.int_value_overflows(value):
            base = dtype_upper.split("(")[0].strip()
            hint = (
                ""
                if base == "BIGINT"
                else " (declare the column as BIGINT for larger integers)"
            )
            raise ValueError(
                f"value {value!r} is outside the signed 64-bit integer range "
                f"(max {coercion.INT64_MAX}){hint}"
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
        """Read and validate JSON file, streaming records one at a time.

        Handles both single-object and array-of-objects formats. For the
        array case the file is parsed *incrementally* via ``ijson`` so a
        multi-GB JSON ingest doesn't OOM the pod — the old implementation
        called ``json.load`` and materialised the whole array in memory
        (backend/#772 P2, deferred half of the streaming item in #771).

        Args:
            file_path: Path to the JSON file

        Yields:
            Dict containing record data

        Raises:
            FileNotFoundError: If the JSON file doesn't exist
            ValueError: If the JSON data is not in the expected format
            ijson.JSONError: If there's an error parsing the JSON
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"{RED}JSON file not found: {file_path}{RESET}")

        try:
            shape = _peek_json_shape(file_path)
            if shape == "object":
                # Single-object form: one record. Not OOM-risky (a single
                # record is by definition tractable). Parse non-incrementally.
                with open(file_path, "r", encoding="utf-8") as f:
                    record = json.load(f)
                yield from self._iter_validated_records([record])
                return
            if shape != "array":
                raise ValueError(
                    "JSON data must be an object or array of objects"
                )
            # Array form: stream item-by-item. Memory cost is bounded
            # by the largest *record* (not the full file). The file
            # handle is opened in a `with` so a partial-consume / early
            # exit / parse error closes it deterministically (#222
            # bugbot — previously a bare ``open(...)`` was passed to
            # ``ijson.items``, leaking the descriptor until GC).
            with open(file_path, "rb") as f:
                yield from self._iter_validated_records(ijson.items(f, "item"))

        except (json.JSONDecodeError, ijson.JSONError) as e:
            logger.error(f"{RED}Error parsing JSON file: {str(e)}{RESET}")
            raise

        except FileNotFoundError:
            raise

        except Exception as e:
            logger.error(f"{RED}Unexpected error reading JSON: {str(e)}{RESET}")
            raise

    def _iter_validated_records(
        self, records: Any
    ) -> Generator[Dict[str, Any], None, None]:
        """Shared per-record validation + yield loop used by both the
        single-object and array-streaming paths in ``read_data``. Factored
        out so the array path can ``yield from`` it inside the ``with
        open(...)`` block — the file handle stays open exactly as long as
        the generator is being consumed."""
        for record in records:
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

    def _count_records(self, file_path: str) -> Optional[int]:
        """Count total records in JSON file without materialising it.

        Single-object form -> 1. Array form -> count by streaming via
        ``ijson`` so even a multi-GB array reports its size without an
        OOM. Returns None on any read error (the caller treats it as
        'unknown total', which only affects the progress bar).
        """
        try:
            shape = _peek_json_shape(Path(file_path))
            if shape == "object":
                # #222 bugbot: don't return 1 based on the peek alone — a
                # truncated / invalid JSON object would advertise 1 record
                # to the progress bar and then make ``read_data`` raise a
                # decode error mid-ingest. Validate parseability via
                # ``json.load`` (single-object form ingests one record by
                # definition — same as ``read_data`` does for this shape,
                # so the parse cost is paid either way). A bad object
                # returns None so the progress bar shows "unknown" rather
                # than a misleading "1 record" that fails mid-ingest.
                with open(file_path, "r", encoding="utf-8") as f:
                    json.load(f)
                return 1
            if shape == "array":
                with open(file_path, "rb") as f:
                    return sum(1 for _ in ijson.items(f, "item"))
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
