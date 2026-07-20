"""Numeric Columns Validator Module.

Validates that all columns (except the excluded semantic columns — by default
just ``timestamp``) are numeric for the time-series categories. This includes
both feature columns and the label column.
"""

import logging
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
from ..utils import redaction

from .base import BaseValidator, ValidationResult
from ..config import Config

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class NumericColumnsValidator(BaseValidator):
    """Validator for numeric columns in the time-series categories.

    Ensures that all schema columns except the excluded semantic columns are
    numeric. This includes both feature columns and the label column.

    ``excluded_columns`` defaults to ``{"timestamp"}`` (time_series_
    forecasting's original hardcoded exclusion). time_series_classification
    passes ``{"sequence_id", "timestamp"}`` — its ``sequence_id`` group key
    is legitimately VARCHAR (backend#1054 WS1).
    """

    def __init__(
        self,
        name: str = "Numeric Columns Validator",
        schema: Optional[dict] = None,
        excluded_columns: Optional[Iterable[str]] = None,
    ):
        super().__init__(name)
        self.schema = schema or {}
        self.excluded_columns = frozenset(
            c.lower() for c in (excluded_columns or {"timestamp"})
        )

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate that all schema columns (except timestamp) are numeric and contain no null values.
        
        Simple validation logic:
        1. Load CSV file
        2. Get schema from self.schema (only validate columns present in schema)
        3. Check if any schema column (except timestamp) has null/NaN values -> error
        4. Check if any schema column datatype is non-numeric -> error
        5. If no errors -> pass
        """
        try:
            df = self._load_data(data)
            if df is None or df.empty:
                return self._create_result(is_valid=False, errors=["No data found to validate"])

            errors = []
            metadata = {"rows_checked": len(df), "columns_in_csv": len(df.columns)}

            # Use self.schema - only validate columns that are in the schema
            if not self.schema:
                return self._create_result(
                    is_valid=True,
                    metadata={**metadata, "message": "No schema provided, skipping validation"},
                )

            # Exclude the semantic columns (timestamp; plus sequence_id for
            # grouped categories) from validation. Case-insensitive, matching
            # how the factories strip these columns elsewhere.
            columns_to_validate = [
                col
                for col in df.columns
                if col in self.schema and col.lower() not in self.excluded_columns
            ]

            metadata["columns_in_schema"] = len(self.schema)
            metadata["columns_to_validate"] = len(columns_to_validate)

            if not columns_to_validate:
                return self._create_result(
                    is_valid=True,
                    metadata={**metadata, "message": "No schema columns to validate (only excluded or non-existent columns)"},
                )

            # NOTE on null handling (issue #195): the previous step rejected
            # ANY null in a non-timestamp column as an error. That contract is
            # wrong for time-series forecasting — the shipped template README
            # explicitly documents lag/window feature columns as "blank for
            # the first row" / "blank until enough history accumulates", and
            # the shipped sample CSV ships with exactly those nulls. So the
            # validator was rejecting its own shipped sample, and any real
            # lag_1 / moving_avg_N / rolling_* feature in customer data.
            #
            # Treat nulls as legitimate (stored as SQL NULL), mirroring the
            # null tolerance the rest of DataValidator gained in #167/#168.
            # The downstream model preprocessor handles leading-NaN in
            # lag/window features by design. We still surface the count via
            # metadata for observability, but it isn't an error.
            for column in columns_to_validate:
                null_count = int(df[column].isna().sum())
                if null_count > 0:
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
                        offender_mask = numeric_series.isna() & df[column].notna()
                        offender_rows = df.index[offender_mask][:5].tolist()
                        error_msg = (
                            f"Column '{column}' contains {non_numeric_actual} non-numeric value(s) "
                            f"at {redaction.row_refs(offender_rows, int(non_numeric_actual))}."
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
