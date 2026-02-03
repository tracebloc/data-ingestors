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
        """Validate that all columns (except timestamp) are numeric and non-null.
        
        This includes both feature columns and the label column (if specified).
        """
        try:
            df = self._load_data(data)
            if df is None or df.empty:
                return self._create_result(is_valid=False, errors=["No data found to validate"])

            errors = []
            metadata = {"rows_checked": len(df)}

            # Only exclude timestamp from numeric validation
            # Label column should also be numeric for time series forecasting
            excluded_columns = {"timestamp"}

            # Get columns to validate (all columns except timestamp)
            columns_to_validate = [col for col in df.columns if col not in excluded_columns]

            if not columns_to_validate:
                return self._create_result(
                    is_valid=True,
                    metadata={**metadata, "message": "No columns to validate (only timestamp column present)"},
                )

            non_numeric_columns = []
            null_columns = []
            for column in columns_to_validate:
                null_mask = pd.isna(df[column])
                null_count = null_mask.sum()
                
                if null_count > 0:
                    null_columns.append(column)
                    null_rows = [i+1 for i in df.index[null_mask][:10]]
                    error_msg = (
                        f"Column '{column}' contains {null_count} null/missing value(s). "
                        f"For time series forecasting, all columns (except timestamp) must be non-null. "
                        f"Null values found at rows: {null_rows}"
                    )
                    if null_count > 10:
                        error_msg += f" (and {null_count - 10} more)"
                    errors.append(error_msg)
                    metadata[f"{column}_null_count"] = null_count
                    metadata[f"{column}_null_rows"] = null_rows[:10]

                numeric_series = pd.to_numeric(df[column], errors="coerce")
                non_numeric_mask = numeric_series.isna() & ~pd.isna(df[column]) & (df[column].astype(str).str.strip() != "")
                non_numeric_count = non_numeric_mask.sum()

                if non_numeric_count > 0:
                    non_numeric_columns.append(column)
                    # Get sample of non-numeric values for error message
                    non_numeric_values = df[column][non_numeric_mask].head(10).tolist()
                    error_msg = (
                        f"Column '{column}' contains {non_numeric_count} non-numeric value(s). "
                        f"For time series forecasting, all columns (except timestamp) must be numeric. "
                        f"Sample invalid values: {non_numeric_values}"
                    )
                    if non_numeric_count > 10:
                        error_msg += f" (and {non_numeric_count - 10} more)"
                    errors.append(error_msg)
                    metadata[f"{column}_non_numeric_count"] = non_numeric_count
                    metadata[f"{column}_non_numeric_sample"] = non_numeric_values

            if non_numeric_columns:
                metadata["non_numeric_columns"] = non_numeric_columns
            if null_columns:
                metadata["null_columns"] = null_columns

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
