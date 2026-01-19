"""Time to Event Validator Module.

This module provides validation for time to event prediction data.
It validates that the time column is present in the dataset.
"""

import logging
from pathlib import Path
from typing import Any, Optional, List

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class TimeToEventValidator(BaseValidator):
    """Validator for time to event prediction data.

    This validator ensures that:
    1. The time column is present in the dataset
    2. The time column is properly identified from schema or provided explicitly

    Attributes:
        time_column: Name of the time column to validate (default: "time")
        schema: Optional schema to identify the time column
    """

    def __init__(
        self,
        time_column: Optional[str] = None,
        schema: Optional[dict] = None,
        name: str = "Time to Event Validator",
    ):
        """Initialize the time to event validator.

        Args:
            time_column: Name of the time column to validate
            schema: Optional schema dictionary to identify time column
            name: Human-readable name of the validator
        """
        super().__init__(name)
        self.time_column = time_column
        self.schema = schema or {}

        # If time_column not provided, try to find it from schema
        if not self.time_column and self.schema:
            # Look for TIME, DATETIME, TIMESTAMP, or INT/FLOAT type columns that might be time
            for col, col_type in self.schema.items():
                if col_type.upper() in ["TIME", "DATETIME", "TIMESTAMP"]:
                    self.time_column = col
                    break
                # Also check for common time column names
                if col.lower() in ["time", "timestamp", "duration", "time_to_event"]:
                    self.time_column = col
                    break

        # If still not found, default to "time"
        if not self.time_column:
            self.time_column = "time"

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate time to event prediction data.

        This method validates that:
        1. The time column exists in the dataset
        2. The time column is properly identified

        Args:
            data: CSV file path or pandas DataFrame to validate
            **kwargs: Additional validation parameters
                - sample_size: Number of rows to sample for validation (default: None, validates all)

        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            if not PANDAS_AVAILABLE:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        "Pandas not available. Cannot perform time to event validation."
                    ],
                    metadata={"pandas_available": False},
                )

            sample_size = kwargs.get("sample_size", None)

            # Load data
            df = self._load_data(data, sample_size)
            if df is None or df.empty:
                return self._create_result(
                    is_valid=False,
                    errors=["No data found to validate"],
                    metadata={"rows_checked": 0},
                )

            # Determine which time column to use (preserve original configuration)
            time_column_to_use = self.time_column

            # Check if time column exists, try to find it from schema if not found
            if time_column_to_use not in df.columns:
                # Try to find time column from schema that exists in CSV
                found_time_column = None
                if self.schema:
                    for col, col_type in self.schema.items():
                        if col_type.upper() in ["TIME", "DATETIME", "TIMESTAMP"] and col in df.columns:
                            found_time_column = col
                            break
                        # Also check for common time column names
                        if col.lower() in ["time", "timestamp", "duration", "time_to_event"] and col in df.columns:
                            found_time_column = col
                            break

                # If still not found, try common time column names
                if not found_time_column:
                    common_time_names = ["time", "timestamp", "duration", "time_to_event", "time_to_event_prediction"]
                    for col_name in common_time_names:
                        if col_name in df.columns:
                            found_time_column = col_name
                            break

                if found_time_column:
                    logger.info(
                        f"Time column '{time_column_to_use}' not found, using '{found_time_column}' instead"
                    )
                    time_column_to_use = found_time_column
                else:
                    return self._create_result(
                        is_valid=False,
                        errors=[
                            f"Time column '{time_column_to_use}' not found in dataset. "
                            f"Available columns: {list(df.columns)}. "
                            f"Please ensure the schema defines a TIME/DATETIME/TIMESTAMP column that exists in the CSV, "
                            f"or provide the time_column parameter explicitly."
                        ],
                        metadata={
                            "time_column": time_column_to_use,
                            "available_columns": list(df.columns),
                        },
                    )

            errors = []
            warnings = []
            metadata = {
                "time_column": time_column_to_use,
                "rows_checked": len(df),
            }

            # Check if time column has any null values
            null_count = df[time_column_to_use].isna().sum()
            if null_count > 0:
                warnings.append(
                    f"Time column '{time_column_to_use}' contains {null_count} null/missing value(s)"
                )
                metadata["null_count"] = null_count

            is_valid = len(errors) == 0

            return self._create_result(
                is_valid=is_valid,
                errors=errors,
                warnings=warnings,
                metadata=metadata,
            )

        except Exception as e:
            logger.error(f"Error during time to event validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Time to event validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )

    def _load_data(self, data: Any, sample_size: Optional[int]) -> Optional[pd.DataFrame]:
        """Load data from input source.

        Args:
            data: Input data (file path, directory path, or DataFrame)
            sample_size: Maximum number of rows to load (None for all rows)

        Returns:
            Pandas DataFrame if successful, None otherwise
        """
        try:
            if isinstance(data, pd.DataFrame):
                if sample_size:
                    return data.head(sample_size)
                return data
            elif isinstance(data, (str, Path)):
                path = Path(data)
                if path.suffix.lower() == ".csv":
                    df = pd.read_csv(
                        path,
                        nrows=sample_size,
                        encoding="utf-8",
                        on_bad_lines="warn",
                    )
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
