"""Time Before Today Validator Module.

Validates that all timestamps are before today.
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


class TimeBeforeTodayValidator(BaseValidator):
    """Validator for timestamps before today.

    Ensures all timestamps are before today's date.
    """

    def __init__(
        self,
        name: str = "Time Before Today Validator",
    ):
        super().__init__(name)

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate timestamps are before today."""
        try:
            df = self._load_data(data)
            if df is None or df.empty:
                return self._create_result(is_valid=False, errors=["No data found to validate"])

            if "timestamp" not in df.columns:
                return self._create_result(
                    is_valid=False,
                    errors=[f"Required column 'timestamp' not found. Available: {list(df.columns)}"],
                )

            # Parse timestamps
            timestamps = pd.to_datetime(df["timestamp"], format='mixed', errors="coerce")
            today = pd.Timestamp.now().normalize()
            errors = []
            metadata = {"rows_checked": len(df), "today": str(today)}

            # Work with valid timestamps only
            valid_timestamps = timestamps[~timestamps.isna()]
            if len(valid_timestamps) > 0:
                future_count = (valid_timestamps.dt.normalize() >= today).sum()
                if future_count > 0:
                    errors.append(f"Found {future_count} timestamp(s) that are not before today")
                    metadata["future_timestamps"] = future_count
                
                metadata.update({
                    "earliest": str(valid_timestamps.min()),
                    "latest": str(valid_timestamps.max()),
                })

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                metadata=metadata,
            )

        except Exception as e:
            logger.error(f"Time before today validation error: {e}")
            return self._create_result(is_valid=False, errors=[f"Validation error: {str(e)}"])

    def _load_data(self, data: Any) -> Optional[pd.DataFrame]:
        """Load complete data from file path for timestamp validation."""
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

