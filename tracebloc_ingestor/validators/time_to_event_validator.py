"""Time to Event Validator Module.

This module provides validation for time to event prediction data.
It validates that:
1. The time column is present in the dataset with the exact name 'time'
2. The time column contains numeric values (int or float)
3. The time values are non-negative
"""

import logging
from pathlib import Path
from typing import Any, Optional

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
    1. The time column with the exact name 'time' (or specified name) is present in the dataset
    2. The time column is required and must exist with the exact specified name
    3. The time column contains numeric values (int or float)
    4. The time values are non-negative

    Attributes:
        time_column: Name of the time column to validate (default: "time", must exist exactly)
        schema: Optional schema dictionary (kept for compatibility, not used for column detection)
    """

    def __init__(
        self,
        time_column: Optional[str] = None,
        schema: Optional[dict] = None,
        name: str = "Time to Event Validator",
    ):
        """Initialize the time to event validator.

        Args:
            time_column: Name of the time column to validate (default: "time")
            schema: Optional schema dictionary (not used for column detection, kept for compatibility)
            name: Human-readable name of the validator
        """
        super().__init__(name)
        # Strictly require 'time' column name
        self.time_column = time_column if time_column is not None else "time"
        self.schema = schema or {}

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate time to event prediction data.

        This method validates that:
        1. The time column with the exact required name exists in the dataset
        2. The time column is present (no fallback to alternative names)
        3. The time column contains numeric values (int or float)
        4. The time values are non-negative

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

            # Strictly require the time column to exist with the exact name
            time_column_to_use = self.time_column

            # Check if time column exists - no fallback, must be exact match
            if time_column_to_use not in df.columns:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"Required time column '{time_column_to_use}' not found in dataset. "
                        f"Available columns: {list(df.columns)}. "
                        f"The dataset must contain a column named '{time_column_to_use}'."
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

            # Validate that time column contains numeric values
            time_series = df[time_column_to_use].copy()
            
            # Try to convert to numeric, non-numeric values will become NaN
            numeric_series = pd.to_numeric(time_series, errors="coerce")
            non_numeric_mask = numeric_series.isna() & ~time_series.isna()
            non_numeric_count = non_numeric_mask.sum()
            
            if non_numeric_count > 0:
                # Get sample of non-numeric values for error message
                non_numeric_values = time_series[non_numeric_mask].head(10).tolist()
                error_msg = (
                    f"Time column '{time_column_to_use}' contains {non_numeric_count} non-numeric value(s). "
                    f"Time values must be numeric (int or float). "
                    f"Sample invalid values: {non_numeric_values}"
                )
                if non_numeric_count > 10:
                    error_msg += f" (and {non_numeric_count - 10} more)"
                errors.append(error_msg)
                metadata["non_numeric_count"] = non_numeric_count
                metadata["non_numeric_sample"] = non_numeric_values

            # Check for negative time values (time should be non-negative)
            if non_numeric_count == 0:
                # Only check for negative values if all values are numeric
                negative_mask = numeric_series < 0
                negative_count = negative_mask.sum()
                
                if negative_count > 0:
                    negative_values = numeric_series[negative_mask].head(10).tolist()
                    error_msg = (
                        f"Time column '{time_column_to_use}' contains {negative_count} negative value(s). "
                        f"Time values must be non-negative. "
                        f"Sample negative values: {negative_values}"
                    )
                    if negative_count > 10:
                        error_msg += f" (and {negative_count - 10} more)"
                    errors.append(error_msg)
                    metadata["negative_count"] = negative_count
                    metadata["negative_sample"] = negative_values
                
                # Add statistics about time values
                valid_times = numeric_series.dropna()
                if len(valid_times) > 0:
                    metadata["min_time"] = float(valid_times.min())
                    metadata["max_time"] = float(valid_times.max())
                    metadata["mean_time"] = float(valid_times.mean())

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
