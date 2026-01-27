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
    1. Column "timestamp" exists
    2. All timestamp values are in valid format
    """

    def __init__(self, name: str = "Time Format Validator"):
        super().__init__(name)

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate timestamp format."""
        try:
            df = self._load_data(data, kwargs.get("sample_size"))
            if df is None or df.empty:
                return self._create_result(is_valid=False, errors=["No data found to validate"])

            if "timestamp" not in df.columns:
                return self._create_result(
                    is_valid=False,
                    errors=[f"Required column 'timestamp' not found. Available: {list(df.columns)}"],
                )

            # Parse timestamps (handle DD/MM/YYYY format)
            timestamps = pd.to_datetime(df["timestamp"], dayfirst=True, errors="coerce")
            errors = []
            metadata = {"rows_checked": len(df)}

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

    def _load_data(self, data: Any, sample_size: Optional[int]) -> Optional[pd.DataFrame]:
        """Load data from input source."""
        try:
            if isinstance(data, pd.DataFrame):
                return data.head(sample_size) if sample_size else data
            
            if isinstance(data, (str, Path)) and hasattr(config, 'LABEL_FILE') and config.LABEL_FILE:
                label_file = Path(config.LABEL_FILE).expanduser()
                if label_file.exists() and label_file.suffix.lower() == ".csv":
                    return pd.read_csv(label_file, nrows=sample_size, encoding="utf-8", on_bad_lines="warn")
            
            return None
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None
