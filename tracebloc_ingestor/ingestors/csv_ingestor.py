"""CSV Data Ingestor Module.

This module provides a specialized ingestor for handling CSV files, with optimized
pandas-based reading and validation capabilities.
"""

from typing import Dict, Any, Generator, Optional, List
import csv as _csv
import pandas as pd
import logging
from pathlib import Path

from .base import BaseIngestor
from ..database import Database
from ..api.client import APIClient
from ..utils.constants import RESET, RED, YELLOW, TaskCategory
from ..utils import label_policy as label_policy_module
from ..config import Config

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


__all__ = ["Ingestor"]


# Tabular-family categories parse CSVs with a wider null sentinel set so the
# strings "NA" / "NULL" / "None" become NaN instead of being stored verbatim.
# This matches what the legacy templates (templates/tabular_*/...) passed
# explicitly via csv_options; the YAML path can't express it because
# schema/ingest.v1.json restricts spec.csv_options to a small whitelist.
_TABULAR_NA_VALUES = ["", "NA", "NULL", "None"]
_TABULAR_FAMILY_CATEGORIES = frozenset({
    TaskCategory.TABULAR_CLASSIFICATION,
    TaskCategory.TABULAR_REGRESSION,
    TaskCategory.TIME_SERIES_FORECASTING,
    TaskCategory.TIME_TO_EVENT_PREDICTION,
})


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
        label_policy: str = label_policy_module.PASSTHROUGH,
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
            label_policy: Bucketing policy for the label value before it's
                sent to the central backend. ``"passthrough"`` for
                classification (default); ``"bucket"`` for regression-class
                tasks so raw target values never leak.
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
        self.csv_options = csv_options or {}

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
                    # Nullable Int64, NOT to_numeric(downcast="integer"): under
                    # the old code any missing cell forced the column to float64,
                    # so 7 round-tripped as "7.0" — silent corruption of every
                    # integer in any INT column that had a single blank cell.
                    # Int64 keeps integers integral and stores missing as pd.NA
                    # (-> SQL NULL); default errors="raise" still surfaces a
                    # genuinely non-numeric value as a clear per-column error.
                    df[column] = pd.to_numeric(df[column]).astype("Int64")
                elif any(t in dtype.upper() for t in ("FLOAT", "DOUBLE", "DECIMAL", "NUMERIC")):
                    # float64 — NOT downcast='float' (float32), which corrupted
                    # precision: 3.14 -> '3.140000104904175'. Also covers DOUBLE/
                    # DECIMAL/NUMERIC, which previously matched NO branch and let
                    # non-numeric junk flow untouched to the DB; errors='raise'
                    # (the default) now rejects junk with a clear per-column
                    # error. MySQL still applies the column's own precision/scale
                    # on write.
                    df[column] = pd.to_numeric(df[column])
                elif "BOOL" in dtype.upper():
                    # Map the textual/numeric boolean forms DataValidator accepts
                    # (true/false, yes/no, t/f, y/n, 1/0) to a nullable boolean
                    # column. df.astype("boolean") alone raises "Need to pass
                    # bool-like values" on those strings — a direct contradiction
                    # with the validator, which blesses them, so a CSV with a
                    # yes/no column passed validation then crashed the ingestor.
                    _truthy = {"true", "t", "yes", "y", "1", "1.0"}
                    _falsy = {"false", "f", "no", "n", "0", "0.0"}
                    _norm = df[column].astype("string").str.strip().str.lower()
                    df[column] = _norm.map(
                        lambda x: True if x in _truthy
                        else (False if x in _falsy else pd.NA),
                        na_action="ignore",
                    ).astype("boolean")
                elif "DATETIME" in dtype.upper() or "TIMESTAMP" in dtype.upper():
                    # Full date+time. Checked before DATE/TIME because the
                    # substrings "DATE" and "TIME" both appear in "DATETIME"
                    # (and "TIME" in "TIMESTAMP").
                    df[column] = pd.to_datetime(df[column], errors="coerce", format="mixed")
                elif "DATE" in dtype.upper():
                    # DATE only — emit a plain date so the value doesn't gain a
                    # spurious time ('2026-01-02' was becoming '2026-01-02 00:00:00').
                    df[column] = pd.to_datetime(df[column], errors="coerce", format="mixed").dt.date
                elif "TIME" in dtype.upper():
                    # TIME only — emit a plain time so the value doesn't gain a
                    # spurious (today's) date ('14:30:00' was becoming
                    # '2026-06-08 14:30:00', which MySQL TIME then truncates).
                    df[column] = pd.to_datetime(df[column], errors="coerce", format="mixed").dt.time
                elif any(t in dtype.upper() for t in ("STRING", "TEXT", "VARCHAR", "CHAR")):
                    # Coerce to pandas StringDtype so missing cells become pd.NA
                    # (not float NaN), then map pd.NA -> Python None so the DB
                    # binder writes SQL NULL. Without this, VARCHAR/CHAR columns
                    # were left as the float64 dtype pandas inferred for an
                    # empty/mixed cell, and str(nan) "nan" landed in MySQL —
                    # silent corruption of missing-data semantics. #167 widened
                    # NULL-tolerance in the validator so all-null VARCHAR no
                    # longer fails validation; this completes the fix on the
                    # write side.
                    df[column] = (
                        df[column].astype("string").astype(object).where(
                            df[column].notna(), None
                        )
                    )
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

            # NA handling. Tabular-family CSVs use pandas' full default NA set
            # (keep_default_na=True) so every common missing sentinel — ""/NaN/
            # "N/A"/"null"/"NA"/"NULL"/"None" — parses as NaN. Crucially this
            # MATCHES how the validators read the file (plain pd.read_csv with
            # defaults), so a file that passes validation can't then crash the
            # numeric type-conversion below on an unrecognised NA token. Other
            # categories keep the conservative empty-only behaviour.
            is_tabular = self.category in _TABULAR_FAMILY_CATEGORIES
            na_values = _TABULAR_NA_VALUES if is_tabular else [""]

            # Pin string-family schema columns to dtype=str so pandas can't infer
            # them numeric and silently strip meaning. An all-digit code column
            # (zip / UniProt accession / zero-padded ID) like "007" is otherwise
            # inferred as int64 at read time and is already 7 by the time the
            # VARCHAR cast in _validate_csv runs -> "7", with the leading zeros
            # gone. na_values are still applied first, so empty/NA cells become
            # NaN (-> SQL NULL) before the str pin; only present values are kept
            # verbatim. dtype keys for columns absent from the file are ignored
            # by pandas, so this is safe when the schema lists more columns than
            # the CSV carries.
            _STRING_TYPES = ("VARCHAR", "CHAR", "TEXT", "STRING")
            string_dtype = {
                col: str
                for col, t in self.schema.items()
                if isinstance(t, str)
                and t.upper().split("(")[0].strip() in _STRING_TYPES
            }

            # Enhanced default options for pandas
            default_options = {
                # Pin string-family columns to str; let pandas infer the rest
                # (numeric/bool/date columns are coerced explicitly in
                # _validate_csv). None when there are no string columns.
                "dtype": string_dtype or None,
                "keep_default_na": is_tabular,
                "na_values": na_values,
                "encoding": "utf-8",
                # Fail loudly on a malformed (ragged) row instead of silently
                # dropping it. A wrong-field-count line almost always signals a
                # real problem (wrong delimiter, an unquoted/embedded comma), and
                # silently shrinking the dataset corrupts it with no signal and a
                # still-green "success". pandas' error names the offending line +
                # field counts. (This also matches _count_records, which reads
                # with pandas' default on_bad_lines='error'.)
                "on_bad_lines": "error",
                "low_memory": False,  # Prevent mixed type inference warnings
                "engine": "c",  # Use faster C engine
            }

            csv_options = {**default_options, **self.csv_options}

            # Reject duplicate column names before pandas silently disambiguates
            # them (a, a -> a, a.1) and the schema mapping then targets the wrong
            # physical column — invisible corruption. Read just the raw header row
            # with the stdlib csv module (NOT pandas) so this is independent of
            # the pd.read_csv path (and the same delimiter/encoding as the main
            # read). csv.reader needs a single-char delimiter; a multi-char/regex
            # sep or a bad encoding falls back to "no header read" (the main read
            # then surfaces the real error).
            _sep = csv_options.get("sep", csv_options.get("delimiter", ","))
            _header = []
            try:
                with open(
                    file_path,
                    "r",
                    encoding=csv_options.get("encoding", "utf-8"),
                    newline="",
                ) as _fh:
                    _row = next(_csv.reader(_fh, delimiter=_sep), [])
                _header = [str(h).strip() for h in _row]
            except (OSError, UnicodeDecodeError, _csv.Error, TypeError):
                _header = []
            _dup_headers = sorted({h for h in _header if _header.count(h) > 1})
            if _dup_headers:
                raise ValueError(
                    f"{RED}Duplicate column name(s) in the CSV header: "
                    f"{_dup_headers}. Each column must be unique — otherwise the "
                    f"second is silently renamed '<name>.1' by the parser and the "
                    f"schema maps onto the wrong column. Rename the duplicates and "
                    f"re-ingest.{RESET}"
                )

            first_chunk = True
            for chunk in pd.read_csv(file_path, chunksize=chunk_size, **csv_options):
                # Strip headers + type-convert EVERY chunk. Doing this only for
                # the first chunk left every row past chunk_size (default 1000)
                # un-converted — a DATE column came back as raw strings, numeric
                # columns fell back to pandas' per-chunk inference, and header
                # whitespace was stripped only for chunk 1 — all invisible until
                # a file exceeds a single chunk.
                chunk.columns = chunk.columns.str.strip()
                self._validate_csv(chunk)
                if first_chunk:
                    # One-time check: the unique_id column exists (columns are
                    # identical across chunks, so checking the first is enough).
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
            # Count rows WITHOUT materialising the whole file. The old
            # `pd.read_csv(file_path).shape[0]` loaded every column of every row
            # into memory just to get a count — for a multi-GB dataset that's an
            # OOM (the pod is Killed/137) before ingestion even starts. Read a
            # single column in chunks and sum the lengths: CSV-aware (quoting /
            # embedded newlines handled, unlike a raw line count) and bounded
            # memory. usecols=[0] keeps each chunk to one column.
            total = 0
            for chunk in pd.read_csv(
                file_path, usecols=[0], chunksize=100_000, encoding="utf-8"
            ):
                total += len(chunk)
            return total
        except Exception as e:
            logger.debug(
                f"{YELLOW}Unable to count CSV records using pandas: {str(e)}{RESET}"
            )
            return None
