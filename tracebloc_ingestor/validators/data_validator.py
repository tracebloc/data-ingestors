"""Data Validator Module.

This module provides validation for data type compliance with schema.
It validates that data types in CSV files match the data types specified
in the schema and provides clear errors when mismatches are found.
"""

import csv as _csv
import json
import logging
import re
from pathlib import Path
from typing import Any, List, Dict, Optional, Union
from datetime import datetime

import numpy as np
import pandas as pd

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class DataValidator(BaseValidator):
    """Validator for ensuring data type compliance with schema.

    This validator focuses on validating that data types in the CSV file
    match the data types specified in the schema. It provides clear errors
    when data type mismatches are found.

    Attributes:
        schema: Expected schema definition (column_name -> data_type)
    """

    def __init__(
        self, schema: Optional[Dict[str, str]] = None, name: str = "Data Validator"
    ):
        """Initialize the data validator.

        Args:
            schema: Expected schema definition (column_name -> data_type)
            name: Human-readable name of the validator
        """
        super().__init__(name)
        self.schema = schema or {}

        # Map database types to validation functions
        self.type_validators = {
            "VARCHAR": self._validate_varchar,
            "CHAR": self._validate_char,
            "TEXT": self._validate_text,
            "INT": self._validate_int,
            "INTEGER": self._validate_int,
            "BIGINT": self._validate_bigint,
            "FLOAT": self._validate_float,
            "DOUBLE": self._validate_double,
            "DECIMAL": self._validate_decimal,
            # NUMERIC is a MySQL alias for DECIMAL and is accepted by both
            # Database._get_sqlalchemy_type (DDL) and CSVIngestor._validate_csv
            # (type cast). Without this entry the preflight DataValidator
            # rejects a NUMERIC(p,s) schema with "Unknown data type" even
            # though ingestion would happily proceed — the two layers must
            # accept the same vocabulary, not diverge.
            "NUMERIC": self._validate_decimal,
            "BOOLEAN": self._validate_boolean,
            "BOOL": self._validate_boolean,
            "DATE": self._validate_date,
            "DATETIME": self._validate_datetime,
            "TIMESTAMP": self._validate_timestamp,
            "TIME": self._validate_time,
        }

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate data types against schema.

        This method validates that data types in the CSV file match the
        data types specified in the schema. It provides clear errors when
        mismatches are found.

        Args:
            data: CSV file path or pandas DataFrame to validate
            **kwargs: Additional validation parameters
                - sample_size: Number of rows to sample for validation (default: 1000)

        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            if not self.schema:
                return self._create_result(
                    is_valid=True,
                    warnings=["No schema provided for validation"],
                    metadata={"schema_provided": False},
                )

            # Validate the ENTIRE column, not a sample. The old default of 1000
            # meant a bad value past row 1000 (a non-numeric in an INT column, a
            # too-long VARCHAR) passed validation and then corrupted or crashed
            # the ingest — validation success did not predict ingestion success.
            # Callers can still pass an explicit sample_size to cap it.
            sample_size = kwargs.get("sample_size", None)

            # Full-column scan of a CSV is done CHUNK BY CHUNK so a very large
            # file is never materialised at once — the old full load OOM'd the
            # pod (Killed/137) on multi-GB datasets. Each chunk is validated and
            # we return on the FIRST failing chunk (reporting that problem
            # without scanning further or holding the whole file). DataFrame /
            # JSON inputs, and any explicit sample_size, keep the single-shot
            # path below.
            if (
                sample_size is None
                and isinstance(data, (str, Path))
                and Path(data).suffix.lower() == ".csv"
            ):
                return self._validate_csv_streaming(
                    Path(data), chunk_size=kwargs.get("chunk_size", 50_000)
                )

            # Load data (DataFrame, JSON, or an explicitly capped CSV sample)
            df = self._load_data(data, sample_size)
            if df is None or df.empty:
                return self._create_result(
                    is_valid=False,
                    errors=["No data found to validate"],
                    metadata={"rows_checked": 0},
                )

            # Validate schema
            return self._validate_schema(df)

        except Exception as e:
            logger.error(f"Error during data type validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Data type validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )

    def _validate_csv_streaming(
        self, path: Path, chunk_size: int = 50_000
    ) -> ValidationResult:
        """Validate a CSV's full contents chunk by chunk, with bounded memory.

        Reads ``chunk_size`` rows at a time, validates each against the schema,
        and returns on the FIRST failing chunk — so a bad value anywhere in a
        large file is caught without materialising the whole file or scanning
        past the first problem. A clean file scans to the end and reports the
        total rows checked.
        """
        # Preflight read rules MUST match CSVIngestor.read_data — otherwise a
        # file can pass validation and then explode at ingest time on a
        # structural error the validator never surfaced. Two mismatches were
        # silent until #190's bugbot caught them:
        #   1. on_bad_lines="error" (CSVIngestor.read_data #76711e6) so a
        #      ragged row is rejected here too, not silently warned-and-dropped.
        #   2. Reject duplicate headers up front so they fail in validation,
        #      not later in the ingestor — same error CSVIngestor raises.
        try:
            with open(path, "r", encoding="utf-8", newline="") as _fh:
                _raw_header = next(_csv.reader(_fh), [])
            _stripped = [str(h).strip() for h in _raw_header]
            _dups = sorted({h for h in _stripped if _stripped.count(h) > 1})
            if _dups:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"Duplicate column name(s) in the CSV header: {_dups}. "
                        f"Each column must be unique — otherwise the second is "
                        f"silently renamed '<name>.1' by the parser and the "
                        f"schema maps onto the wrong column."
                    ],
                    metadata={"rows_checked": 0},
                )
        except (OSError, UnicodeDecodeError, _csv.Error, TypeError, StopIteration):
            # Header probe failed; let pd.read_csv surface the real error below.
            pass

        try:
            reader = pd.read_csv(
                path, chunksize=chunk_size, encoding="utf-8", on_bad_lines="error"
            )
        except pd.errors.EmptyDataError:
            return self._create_result(
                is_valid=False,
                errors=["No data found to validate"],
                metadata={"rows_checked": 0},
            )

        rows_checked = 0
        saw_data = False
        for chunk in reader:
            if chunk.empty:
                continue
            saw_data = True
            rows_checked += len(chunk)
            result = self._validate_schema(chunk)
            if not result.is_valid:
                if isinstance(result.metadata, dict):
                    result.metadata["rows_checked"] = rows_checked
                return result

        if not saw_data:
            return self._create_result(
                is_valid=False,
                errors=["No data found to validate"],
                metadata={"rows_checked": 0},
            )
        return self._create_result(
            is_valid=True,
            metadata={"rows_checked": rows_checked, "schema_provided": True},
        )

    def _load_data(self, data: Any, sample_size: int) -> Optional[pd.DataFrame]:
        """Load data from input source.

        Args:
            data: Input data (file path or DataFrame)
            sample_size: Maximum number of rows to load

        Returns:
            Pandas DataFrame if successful, None otherwise
        """
        try:
            if isinstance(data, pd.DataFrame):
                return data.head(sample_size)
            elif isinstance(data, (str, Path)):
                path = Path(data)
                suffix = path.suffix.lower()
                if suffix == ".csv":
                    df = pd.read_csv(
                        path, nrows=sample_size, encoding="utf-8", on_bad_lines="warn"
                    )
                    return df
                elif suffix == ".json":
                    # Mirror JSONIngestor.read_data, which accepts BOTH a
                    # top-level array of records AND a top-level single dict
                    # (one record). Its docstring is explicit: "handles both
                    # single-object and array-of-objects formats", and the
                    # implementation wraps a dict as `[data]` before iterating.
                    # Read raw + normalise here so a single-dict JSON file
                    # doesn't get rejected at the preflight gate with
                    # "No data found to validate" while the ingestor itself
                    # would have happily processed it — surfaced by #232.
                    with open(path, "r", encoding="utf-8") as f:
                        raw = json.load(f)
                    if isinstance(raw, dict):
                        raw = [raw]
                    df = pd.DataFrame(raw).head(sample_size)
                    # pd.read_json (unlike pd.read_csv with keep_default_na=True)
                    # preserves "" as the literal empty string. Normalise to NaN
                    # so per-type validators below treat "" identically to JSON
                    # null — matches the JSONIngestor._validate_record convention
                    # `if value is None or value == ""` (#170). Without this, an
                    # INT column with `""` is reported as "non-numeric value"
                    # even though JSONIngestor would have read it as missing.
                    df = df.replace("", np.nan)
                    return df
                else:
                    logger.warning(f"Unsupported file type: {path.suffix}, \n\n{path}")
                    return None
            else:
                logger.warning(f"Unsupported data type: {type(data)}")
                return None

        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            return None

    def _validate_schema(self, df: pd.DataFrame) -> ValidationResult:
        """Validate data against schema.

        This method focuses on validating that data types in the CSV file
        match the data types specified in the schema.

        Args:
            df: Pandas DataFrame to validate

        Returns:
            ValidationResult containing validation status and messages
        """
        # Strip header whitespace so validation matches what the ingestor
        # actually ingests: CSVIngestor.read_data does `chunk.columns.str.strip()`
        # on every chunk, so a header like " age" becomes "age" and is ingested
        # against schema key "age". Without the same strip here, the validator
        # saw " age" (!= "age"), silently skipped the column, and a bad value in
        # it passed pre-flight validation only to fail later at ingest. rename()
        # returns a new frame, so the caller's DataFrame is untouched.
        df = df.rename(columns=lambda c: str(c).strip())

        errors = []
        warnings = []
        metadata = {
            "rows_checked": len(df),
            "columns_checked": len(df.columns),
            "schema_columns": list(self.schema.keys()),
            "file_columns": list(df.columns),
            "type_mismatches": {},
            "compliant_columns": {},
        }

        # Check if data types mentioned in schema match the actual data
        for column in df.columns:
            if column in self.schema:
                expected_type = self.schema[column]
                validation_result = self._validate_column_type(
                    df[column], column, expected_type
                )

                if not validation_result["is_valid"]:
                    errors.extend(validation_result["errors"])
                    metadata["type_mismatches"][column] = {
                        "expected": expected_type,
                        "actual": str(df[column].dtype),
                        "errors": validation_result["errors"],
                    }
                else:
                    metadata["compliant_columns"][column] = {
                        "expected": expected_type,
                        "actual": str(df[column].dtype),
                        "compliant": True,
                    }

                warnings.extend(validation_result["warnings"])

        is_valid = len(errors) == 0

        return self._create_result(
            is_valid=is_valid, errors=errors, warnings=warnings, metadata=metadata
        )

    def _validate_column_type(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate a column against expected type.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected data type

        Returns:
            Dictionary with validation results
        """
        # Extract base type from database type (e.g., VARCHAR(255) -> VARCHAR)
        # Also strip constraints like NOT NULL, UNSIGNED, DEFAULT, etc.
        type_without_constraints = expected_type.strip().split()[0]  # Get first word
        base_type = type_without_constraints.split("(")[0].upper()

        if base_type in self.type_validators:
            return self.type_validators[base_type](series, column_name, expected_type)
        else:
            return {
                "is_valid": False,
                "errors": [f"Unknown data type: {expected_type}"],
                "warnings": [],
            }

    def _validate_varchar(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate VARCHAR column.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected VARCHAR type (e.g., VARCHAR(255))

        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []

        # Extract length constraint
        length_match = re.search(r"VARCHAR\((\d+)\)", expected_type.upper())
        max_length = int(length_match.group(1)) if length_match else None

        # Issue #188: do NOT compare ``astype(str)`` to the original typed
        # value. pd.read_csv infers numeric-looking columns as int64/float64,
        # so for value 100 the check ``"100" != 100`` is True and the column
        # was falsely rejected as "non-string" — blocking legitimate zip
        # codes, numeric IDs kept as text, and 0/1 labels declared VARCHAR.
        # MySQL binds any scalar as a string against a VARCHAR column, so the
        # only real constraints to enforce here are presence/null tolerance
        # (handled by other layers) and length. Flag genuinely non-scalar
        # values (list/dict/set/tuple) — those have no sensible string form
        # and indicate a real shape error — but let every scalar pass.
        non_scalar_mask = series.apply(
            lambda v: isinstance(v, (list, dict, set, tuple))
        )
        non_scalar_count = int(non_scalar_mask.sum())
        if non_scalar_count > 0:
            errors.append(
                f"Column '{column_name}' contains {non_scalar_count} non-scalar value(s) "
                f"(list/dict/set/tuple) that cannot be stored as a string"
            )

        # Check length constraints
        if max_length:
            string_series = series.dropna().astype(str)
            too_long = string_series.str.len() > max_length
            too_long_count = too_long.sum()

            if too_long_count > 0:
                errors.append(
                    f"Column '{column_name}' has {too_long_count} values exceeding max length {max_length}"
                )

        return {"is_valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    def _validate_char(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate CHAR column.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected CHAR type (e.g., CHAR(10))

        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []

        # Extract length constraint
        length_match = re.search(r"CHAR\((\d+)\)", expected_type.upper())
        max_length = int(length_match.group(1)) if length_match else None

        # Issue #188: see _validate_varchar — same astype(str)!=value
        # over-rejection bug. CHAR columns with numeric values (e.g. a
        # 2-character state code "10") were blocked. Same fix: flag only
        # genuinely non-scalar values, let any scalar through.
        non_scalar_mask = series.apply(
            lambda v: isinstance(v, (list, dict, set, tuple))
        )
        non_scalar_count = int(non_scalar_mask.sum())
        if non_scalar_count > 0:
            errors.append(
                f"Column '{column_name}' contains {non_scalar_count} non-scalar value(s) "
                f"(list/dict/set/tuple) that cannot be stored as a string"
            )

        # Check length constraints (CHAR should be fixed length)
        if max_length:
            string_series = series.dropna().astype(str)
            wrong_length = string_series.str.len() != max_length
            wrong_length_count = wrong_length.sum()

            if wrong_length_count > 0:
                errors.append(
                    f"Column '{column_name}' has {wrong_length_count} values with length != {max_length}"
                )

        return {"is_valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    def _validate_text(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate TEXT column.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected TEXT type

        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []

        # Issue #188: see _validate_varchar — same astype(str)!=value bug.
        # TEXT columns with numeric values (notes that happen to be all
        # digits, integer IDs kept as text) were blocked. Same fix:
        # flag only genuinely non-scalar values; any scalar passes.
        non_scalar_mask = series.apply(
            lambda v: isinstance(v, (list, dict, set, tuple))
        )
        non_scalar_count = int(non_scalar_mask.sum())
        if non_scalar_count > 0:
            errors.append(
                f"Column '{column_name}' contains {non_scalar_count} non-scalar value(s) "
                f"(list/dict/set/tuple) that cannot be stored as a string"
            )

        return {"is_valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    @staticmethod
    def _non_finite_error(series, numeric_series, column_name):
        """Return an error string if a numeric column has inf/-inf values.

        ``pd.to_numeric`` treats inf/-inf as valid numbers, so they slip past
        the non-numeric check — but they are not valid finite input: they flow
        through the training preprocessor's scaling unchanged and produce a NaN
        loss. Reject them at the gate, showing the offending values.
        """
        mask = numeric_series.notna() & ~np.isfinite(numeric_series)
        count = int(mask.sum())
        if count == 0:
            return None
        sample = series[mask].head(5).tolist()
        return (
            f"Column '{column_name}' contains {count} non-finite value(s) "
            f"(inf/-inf): {sample}"
        )

    def _validate_int(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate INT column.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected INT type

        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []

        # Coerce to numeric; only values that were *present* but unparseable are
        # "non-numeric". Genuine missing values (NaN/empty) are NOT non-numeric —
        # the ingestor stores them as NULL — so they must not be conflated, or a
        # user can never clear the error (inserting NaN doesn't help). See
        # NumericColumnsValidator, which makes the same distinction.
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_numeric_mask = numeric_series.isna() & series.notna()
        non_numeric_count = int(non_numeric_mask.sum())

        if non_numeric_count > 0:
            sample = series[non_numeric_mask].head(5).tolist()
            errors.append(
                f"Column '{column_name}' contains {non_numeric_count} non-numeric value(s). "
                f"Sample invalid values: {sample}"
            )

        non_finite = self._non_finite_error(series, numeric_series, column_name)
        if non_finite:
            errors.append(non_finite)

        # Check for integer values among the parsed, finite, non-null numbers only
        # (NaN/inf would otherwise be miscounted as "non-integer" via NaN % 1).
        if non_numeric_count == 0:
            finite = numeric_series.notna() & np.isfinite(numeric_series)
            non_integer_mask = finite & (numeric_series % 1 != 0)
            non_integer_count = int(non_integer_mask.sum())
            if non_integer_count > 0:
                errors.append(
                    f"Column '{column_name}' contains {non_integer_count} non-integer values"
                )

        return {"is_valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    def _validate_bigint(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate BIGINT column.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected BIGINT type

        Returns:
            Dictionary with validation results
        """
        return self._validate_int(series, column_name, expected_type)

    def _validate_float(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate FLOAT column.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected FLOAT type

        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []

        # Coerce to numeric; only values that were *present* but unparseable are
        # "non-numeric". Genuine missing values (NaN/empty) are valid for a float
        # column (stored as NULL), so they must not be counted here — otherwise a
        # user can never clear the error (inserting NaN doesn't help). See
        # NumericColumnsValidator, which makes the same distinction.
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_numeric_mask = numeric_series.isna() & series.notna()
        non_numeric_count = int(non_numeric_mask.sum())

        if non_numeric_count > 0:
            sample = series[non_numeric_mask].head(5).tolist()
            errors.append(
                f"Column '{column_name}' contains {non_numeric_count} non-numeric value(s). "
                f"Sample invalid values: {sample}"
            )

        non_finite = self._non_finite_error(series, numeric_series, column_name)
        if non_finite:
            errors.append(non_finite)

        return {"is_valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    def _validate_double(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate DOUBLE column.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected DOUBLE type

        Returns:
            Dictionary with validation results
        """
        return self._validate_float(series, column_name, expected_type)

    def _validate_decimal(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate DECIMAL column.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected DECIMAL type

        Returns:
            Dictionary with validation results
        """
        return self._validate_float(series, column_name, expected_type)

    def _validate_boolean(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate BOOLEAN column.

        Accepts valid boolean representations:
        - True/False (bool dtype)
        - 0/1 (int dtype)
        - "True"/"False", "1"/"0", "yes"/"no" (string dtype)

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected BOOLEAN type

        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []

        # If already boolean dtype, it's valid
        if series.dtype == "bool":
            return {"is_valid": True, "errors": errors, "warnings": warnings}

        # Check for valid boolean representations
        try:
            # Remove NaN/null values for validation
            non_null_series = series.dropna()
            
            if len(non_null_series) == 0:
                # All values are null, which is acceptable
                return {"is_valid": True, "errors": errors, "warnings": warnings}

            # Check if values are valid boolean representations
            if series.dtype in ["int64", "int32", "Int64", "Int32"]:
                # Integer values: should be 0 or 1
                invalid_values = non_null_series[~non_null_series.isin([0, 1])]
                if len(invalid_values) > 0:
                    errors.append(
                        f"Column '{column_name}' contains non-boolean values. "
                        f"Found {len(invalid_values)} invalid value(s): {set(invalid_values.tolist())}"
                    )
            
            elif series.dtype in ["float64", "float32", "Float64", "Float32"]:
                # Float values: should be 0.0 or 1.0 (or 0/1 as floats)
                invalid_values = non_null_series[~non_null_series.isin([0.0, 1.0, 0, 1])]
                if len(invalid_values) > 0:
                    errors.append(
                        f"Column '{column_name}' contains non-boolean values. "
                        f"Found {len(invalid_values)} invalid value(s): {set(invalid_values.tolist())}"
                    )
            
            elif series.dtype == "object" or series.dtype == "string":
                # String values: try to convert and check for valid boolean strings
                # First, try numeric conversion for values like "0", "1", "0.0", "1.0"
                string_series = non_null_series.astype(str).str.strip()
                numeric_series = pd.to_numeric(string_series, errors="coerce")
                
                # Check which values are valid numeric booleans (0, 1, 0.0, 1.0)
                numeric_valid = numeric_series.isin([0, 1, 0.0, 1.0])
                
                # For non-numeric values, check against valid boolean strings
                valid_boolean_strings = {
                    "true", "false", "yes", "no", "y", "n", 
                    "t", "f", "TRUE", "FALSE", "YES", "NO"
                }
                string_lower = string_series.str.lower()
                string_valid = string_lower.isin(valid_boolean_strings)
                
                # A value is valid if it's either a valid numeric boolean OR a valid boolean string
                # (NaN from to_numeric means it wasn't numeric, so check string_valid for those)
                is_valid = numeric_valid | (numeric_series.isna() & string_valid)
                
                invalid_values = string_series[~is_valid]
                if len(invalid_values) > 0:
                    errors.append(
                        f"Column '{column_name}' contains non-boolean values. "
                        f"Found {len(invalid_values)} invalid value(s): {set(invalid_values.tolist())}"
                    )
            
            else:
                # Try to convert to boolean and check if conversion is possible
                try:
                    # Attempt conversion
                    bool_series = pd.to_numeric(non_null_series, errors="coerce")
                    # Check if all values are 0 or 1 after numeric conversion
                    if bool_series.isna().any():
                        errors.append(f"Column '{column_name}' contains non-boolean values")
                    else:
                        invalid_values = bool_series[~bool_series.isin([0, 1])]
                        if len(invalid_values) > 0:
                            errors.append(
                                f"Column '{column_name}' contains non-boolean values. "
                                f"Found {len(invalid_values)} invalid value(s)"
                            )
                except (ValueError, TypeError):
                    errors.append(f"Column '{column_name}' contains non-boolean values")

        except Exception as e:
            logger.debug(f"Error validating boolean column '{column_name}': {str(e)}")
            errors.append(f"Column '{column_name}' contains non-boolean values")

        return {"is_valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    def _validate_date(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate DATE column.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected DATE type

        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []

        # Try to convert to datetime
        try:
            date_series = pd.to_datetime(series, errors="coerce")
            # A NULL is valid for any column; only a value that was present
            # but un-parseable is an "invalid date". Without the
            # ``series.notna()`` mask every missing cell counts as invalid —
            # an unclearable error (mirrors the INT/FLOAT validators).
            invalid_dates = (date_series.isna() & series.notna()).sum()

            if invalid_dates > 0:
                errors.append(
                    f"Column '{column_name}' contains {invalid_dates} invalid date values"
                )
        except:
            errors.append(f"Column '{column_name}' contains invalid date values")

        return {"is_valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    def _validate_datetime(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate DATETIME column.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected DATETIME type

        Returns:
            Dictionary with validation results
        """
        return self._validate_date(series, column_name, expected_type)

    def _validate_timestamp(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate TIMESTAMP column.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected TIMESTAMP type

        Returns:
            Dictionary with validation results
        """
        return self._validate_date(series, column_name, expected_type)

    def _validate_time(
        self, series: pd.Series, column_name: str, expected_type: str
    ) -> Dict[str, Any]:
        """Validate TIME column.

        Args:
            series: Pandas Series to validate
            column_name: Name of the column
            expected_type: Expected TIME type

        Returns:
            Dictionary with validation results
        """
        return self._validate_date(series, column_name, expected_type)

    def _detect_column_type(self, series: pd.Series) -> str:
        """Auto-detect column type.

        Args:
            series: Pandas Series to analyze

        Returns:
            Detected data type
        """
        if series.dtype == "bool":
            return "BOOLEAN"
        elif pd.api.types.is_integer_dtype(series):
            return "INT"
        elif pd.api.types.is_float_dtype(series):
            return "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(series):
            return "DATETIME"
        else:
            return "VARCHAR(255)"
