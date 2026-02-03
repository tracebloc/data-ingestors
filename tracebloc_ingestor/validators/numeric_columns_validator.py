"""Numeric Columns Validator Module.

Validates that all columns (except timestamp) are numeric and non-null for time series forecasting.
This includes both feature columns and the label column.
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


class NumericColumnsValidator(BaseValidator):
    """Validator for numeric columns in time series forecasting.

    Ensures that all columns except 'timestamp' are numeric and non-null.
    This includes both feature columns and the label column.
    """

    def __init__(
        self,
        name: str = "Numeric Columns Validator",
    ):
        super().__init__(name)

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate that all columns (except timestamp) are numeric and contain no null values.
        
        Simple validation logic:
        1. Load CSV file
        2. Check if any column (except timestamp) has null/NaN values -> error
        3. Check if any schema column datatype is non-numeric -> error
        4. If no errors -> pass
        """
        try:
            df = self._load_data(data)
            if df is None or df.empty:
                return self._create_result(is_valid=False, errors=["No data found to validate"])

            errors = []
            metadata = {"rows_checked": len(df), "columns_checked": len(df.columns)}

            # Exclude timestamp column from validation
            excluded_columns = {"timestamp"}
            columns_to_validate = [col for col in df.columns if col not in excluded_columns]

            if not columns_to_validate:
                return self._create_result(
                    is_valid=True,
                    metadata={**metadata, "message": "No columns to validate (only timestamp column present)"},
                )

            # Step 1: Check for null values in all columns (except timestamp)
            for column in columns_to_validate:
                null_count = df[column].isna().sum()
                
                if null_count > 0:
                    null_rows = [i+1 for i in df.index[df[column].isna()][:10]]
                    error_msg = (
                        f"Column '{column}' contains {null_count} null/missing value(s). "
                        f"Null values found at rows: {null_rows}"
                    )
                    if null_count > 10:
                        error_msg += f" (and {null_count - 10} more)"
                    errors.append(error_msg)
                    metadata[f"{column}_null_count"] = null_count

            # Step 2: Check if all columns are numeric
            for column in columns_to_validate:
                # Try to convert entire column to numeric
                numeric_series = pd.to_numeric(df[column], errors="coerce")
                # Count how many values couldn't be converted to numeric
                non_numeric_count = numeric_series.isna().sum()
                
                # Get the original non-null count
                original_non_null = df[column].notna().sum()
                
                # If some non-null values couldn't be converted, they're non-numeric
                if non_numeric_count > 0 and original_non_null > 0:
                    # Only report non-numeric if original data had non-null values that couldn't convert
                    non_numeric_actual = (numeric_series.isna() & df[column].notna()).sum()
                    
                    if non_numeric_actual > 0:
                        non_numeric_values = df[column][numeric_series.isna() & df[column].notna()].head(10).tolist()
                        error_msg = (
                            f"Column '{column}' contains {non_numeric_actual} non-numeric value(s). "
                            f"Sample invalid values: {non_numeric_values}"
                        )
                        errors.append(error_msg)
                        metadata[f"{column}_non_numeric_count"] = non_numeric_actual

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                metadata=metadata,
            )

        except Exception as e:
            logger.error(f"Numeric columns validation error: {e}")
            return self._create_result(is_valid=False, errors=[f"Validation error: {str(e)}"])

    def _load_data(self, data: Any) -> Optional[pd.DataFrame]:
        """Load data from file path."""
        try:
            if isinstance(data, (str, Path)):
                path = Path(data)
                if path.exists() and path.suffix.lower() == ".csv":
                    return pd.read_csv(path, encoding="utf-8", on_bad_lines="warn")
            
            return None
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None
