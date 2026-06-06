"""Time Format Validator Module.

Validates that timestamp column exists and contains valid timestamp values.
"""

import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class TimeFormatValidator(BaseValidator):
    """Validator for timestamp format.

    Ensures:
    1. Column "timestamp" exists in the dataset
    2. Timestamp column in schema is of type TIMESTAMP (not DATE or DATETIME)
    3. All timestamp values are in valid format
    """

    def __init__(
        self,
        name: str = "Time Format Validator",
        schema: Optional[dict] = None,
    ):
        super().__init__(name)
        self.schema = schema or {}

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate timestamp format."""
        try:
            errors = []
            
            # Check schema: timestamp column must exist and be of type TIMESTAMP
            if self.schema:
                if "timestamp" not in self.schema:
                    errors.append(
                        "Schema must contain a 'timestamp' column. "
                        "For time series forecasting, a 'timestamp' column is required."
                    )
                    return self._create_result(is_valid=False, errors=errors)
                
                # Extract base type (before parentheses) to handle precision specifiers like TIMESTAMP(6)
                timestamp_type = self.schema["timestamp"].upper().strip()
                base_type = timestamp_type.split("(")[0].split()[0]
                
                if base_type not in ["TIMESTAMP"]:
                    errors.append(
                        f"Timestamp column in schema must be of type 'TIMESTAMP', "
                        f"but found '{self.schema['timestamp']}'. "
                        f"For time series forecasting, timestamp column must be TIMESTAMP type."
                    )
                    return self._create_result(is_valid=False, errors=errors)
            
            df = self._load_data(data)
            if df is None or df.empty:
                return self._create_result(is_valid=False, errors=["No data found to validate"])

            if "timestamp" not in df.columns:
                return self._create_result(
                    is_valid=False,
                    errors=[f"Required column 'timestamp' not found. Available: {list(df.columns)}"],
                )

            # Parse timestamps (month-first by default).
            timestamps = pd.to_datetime(df["timestamp"], format='mixed', errors="coerce")
            metadata = {"rows_checked": len(df)}

            # Locale-ambiguity guard. "03.04.2026" is Apr 3 read day-first (EU)
            # but Mar 4 read month-first (US); format='mixed' silently picks one
            # and corrupts the whole series with no error. If a value parses
            # successfully BOTH ways but to different dates, it is ambiguous —
            # reject it and steer the user to unambiguous ISO 8601.
            day_first = pd.to_datetime(
                df["timestamp"], format='mixed', dayfirst=True, errors="coerce"
            )
            ambiguous_mask = (
                timestamps.notna() & day_first.notna() & (timestamps != day_first)
            )
            if ambiguous_mask.any():
                ambiguous_count = int(ambiguous_mask.sum())
                samples = df["timestamp"][ambiguous_mask].astype(str).head(5).tolist()
                errors.append(
                    f"Found {ambiguous_count} ambiguous date(s) in 'timestamp' that parse "
                    f"differently day-first vs month-first (e.g. {samples}). Use ISO 8601 "
                    f"(YYYY-MM-DD or YYYY-MM-DD HH:MM:SS) to remove the ambiguity."
                )
                metadata["ambiguous_timestamps"] = ambiguous_count

            # Check for invalid/missing timestamps
            invalid_mask = timestamps.isna()
            if invalid_mask.any():
                invalid_count = invalid_mask.sum()
                invalid_rows = [i+1 for i in df.index[invalid_mask][:10]]
                errors.append(f"Found {invalid_count} invalid timestamp(s) at rows: {invalid_rows}")
                metadata["invalid_timestamps"] = invalid_count

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                metadata=metadata,
            )

        except Exception as e:
            logger.error(f"Time format validation error: {e}")
            return self._create_result(is_valid=False, errors=[f"Validation error: {str(e)}"])
    def _load_data(self, data: Any) -> Optional[pd.DataFrame]:
        try:
            if isinstance(data, (str, Path)):
                file_path = Path(data).expanduser()
                if file_path.exists() and file_path.suffix.lower() == ".csv":
                    # Always load complete file for timestamp validation
                    return pd.read_csv(file_path, encoding="utf-8", on_bad_lines="warn")
            
            return None
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None

