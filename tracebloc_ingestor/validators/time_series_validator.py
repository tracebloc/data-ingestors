"""Time Series Validator Module.

This module provides validation for time series forecasting data.
It validates that the date column is properly formatted, ordered chronologically,
and all timestamps are in the past.
"""

import logging
from pathlib import Path
from typing import Any, Optional, List
from datetime import datetime

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


class TimeSeriesValidator(BaseValidator):
    """Validator for time series forecasting data.

    This validator ensures that:
    1. The date column is properly formatted as time
    2. The date column is ordered chronologically
    3. All timestamps are less than today's timestamp

    Attributes:
        date_column: Name of the date column to validate (default: "date")
        schema: Optional schema to identify the date column
    """

    def __init__(
        self,
        date_column: Optional[str] = None,
        schema: Optional[dict] = None,
        name: str = "Time Series Validator",
    ):
        """Initialize the time series validator.

        Args:
            date_column: Name of the date column to validate
            schema: Optional schema dictionary to identify date column
            name: Human-readable name of the validator
        """
        super().__init__(name)
        self.date_column = date_column
        self.schema = schema or {}

        # If date_column not provided, try to find it from schema
        if not self.date_column and self.schema:
            # Look for DATE, DATETIME, or TIMESTAMP type columns
            for col, col_type in self.schema.items():
                if col_type.upper() in ["DATE", "DATETIME", "TIMESTAMP"]:
                    self.date_column = col
                    break

        # If still not found, default to "date"
        if not self.date_column:
            self.date_column = "date"

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate time series data.

        This method validates that:
        1. The date column exists and is properly formatted
        2. Dates are ordered chronologically
        3. All dates are less than today's timestamp

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
                        "Pandas not available. Cannot perform time series validation."
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

            # Determine which date column to use (preserve original configuration)
            date_column_to_use = self.date_column
            
            # Check if date column exists, try to find it from schema if not found
            if date_column_to_use not in df.columns:
                # Try to find date column from schema that exists in CSV
                found_date_column = None
                if self.schema:
                    for col, col_type in self.schema.items():
                        if col_type.upper() in ["DATE", "DATETIME", "TIMESTAMP"] and col in df.columns:
                            found_date_column = col
                            break
                
                # If still not found, try common date column names
                if not found_date_column:
                    common_date_names = ["timestamp", "time", "datetime", "date", "ts"]
                    for col_name in common_date_names:
                        if col_name in df.columns:
                            found_date_column = col_name
                            break
                
                if found_date_column:
                    logger.info(
                        f"Date column '{date_column_to_use}' not found, using '{found_date_column}' instead"
                    )
                    date_column_to_use = found_date_column
                else:
                    return self._create_result(
                        is_valid=False,
                        errors=[
                            f"Date column '{date_column_to_use}' not found in dataset. "
                            f"Available columns: {list(df.columns)}. "
                            f"Please ensure the schema defines a DATE/DATETIME/TIMESTAMP column that exists in the CSV."
                        ],
                        metadata={
                            "date_column": date_column_to_use,
                            "available_columns": list(df.columns),
                        },
                    )

            errors = []
            warnings = []
            metadata = {
                "date_column": date_column_to_use,
                "rows_checked": len(df),
            }

            # Validate date format and parse dates
            date_series = df[date_column_to_use].copy()
            parsed_dates = []
            invalid_dates = []

            for idx, date_value in enumerate(date_series):
                if pd.isna(date_value):
                    invalid_dates.append((idx + 1, "Missing/NaN value"))
                    continue

                try:
                    # Try to parse as datetime
                    if isinstance(date_value, str):
                        # Try common date formats
                        parsed_date = pd.to_datetime(date_value, errors="raise")
                    elif isinstance(date_value, (pd.Timestamp, datetime)):
                        parsed_date = pd.Timestamp(date_value)
                    else:
                        # Try to convert to datetime
                        parsed_date = pd.to_datetime(str(date_value), errors="raise")

                    parsed_dates.append((idx + 1, parsed_date))
                except (ValueError, TypeError) as e:
                    invalid_dates.append((idx + 1, f"Invalid date format: {date_value}"))

            # Check for invalid date formats
            if invalid_dates:
                error_messages = [
                    f"Row {row}: {error}" for row, error in invalid_dates[:10]
                ]
                if len(invalid_dates) > 10:
                    error_messages.append(
                        f"... and {len(invalid_dates) - 10} more invalid dates"
                    )
                errors.append(
                    f"Found {len(invalid_dates)} invalid date format(s):\n"
                    + "\n".join(error_messages)
                )
                metadata["invalid_dates"] = invalid_dates

            # If we have valid dates, check ordering and timestamps
            if parsed_dates:
                # Check chronological ordering
                dates_only = [date for _, date in parsed_dates]
                is_ordered = all(
                    dates_only[i] <= dates_only[i + 1]
                    for i in range(len(dates_only) - 1)
                )

                if not is_ordered:
                    # Find out-of-order positions
                    out_of_order = []
                    for i in range(len(dates_only) - 1):
                        if dates_only[i] > dates_only[i + 1]:
                            out_of_order.append(
                                (
                                    parsed_dates[i][0],
                                    parsed_dates[i + 1][0],
                                    dates_only[i],
                                    dates_only[i + 1],
                                )
                            )

                    error_messages = [
                        f"Row {row1} ({date1}) comes after row {row2} ({date2})"
                        for row1, row2, date1, date2 in out_of_order[:10]
                    ]
                    if len(out_of_order) > 10:
                        error_messages.append(
                            f"... and {len(out_of_order) - 10} more out-of-order pairs"
                        )
                    errors.append(
                        f"Date column is not ordered chronologically. "
                        f"Found {len(out_of_order)} out-of-order pair(s):\n"
                        + "\n".join(error_messages)
                    )
                    metadata["out_of_order_pairs"] = out_of_order

                # Check that all dates are less than today
                today = pd.Timestamp.now().normalize()
                future_dates = [
                    (row, date) for row, date in parsed_dates if date >= today
                ]

                if future_dates:
                    error_messages = [
                        f"Row {row}: {date} (must be before today: {today.date()})"
                        for row, date in future_dates[:10]
                    ]
                    if len(future_dates) > 10:
                        error_messages.append(
                            f"... and {len(future_dates) - 10} more future dates"
                        )
                    errors.append(
                        f"Found {len(future_dates)} date(s) that are not less than today's timestamp:\n"
                        + "\n".join(error_messages)
                    )
                    metadata["future_dates"] = future_dates

                metadata["earliest_date"] = str(min(dates_only))
                metadata["latest_date"] = str(max(dates_only))
                metadata["today"] = str(today)

            is_valid = len(errors) == 0

            return self._create_result(
                is_valid=is_valid,
                errors=errors,
                warnings=warnings,
                metadata=metadata,
            )

        except Exception as e:
            logger.error(f"Error during time series validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Time series validation error: {str(e)}"],
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
                # For time series forecasting, always use LABEL_FILE as the dataset file
                if hasattr(config, 'LABEL_FILE') and config.LABEL_FILE:
                    label_file = Path(config.LABEL_FILE).expanduser()
                    if label_file.exists() and label_file.suffix.lower() == ".csv":
                        logger.info(f"Using LABEL_FILE for validation: {label_file}")
                        df = pd.read_csv(
                            label_file,
                            nrows=sample_size,
                            encoding="utf-8",
                            on_bad_lines="warn",
                        )
                        return df
                    else:
                        logger.warning(
                            f"LABEL_FILE ({label_file}) does not exist or is not a CSV file."
                        )
                        return None
                else:
                    logger.warning("LABEL_FILE not configured. Cannot validate time series data.")
                    return None
            else:
                logger.warning(f"Unsupported data type: {type(data)}")
                return None

        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            return None
