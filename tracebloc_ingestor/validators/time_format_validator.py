"""Time Format Validator Module.

Validates that timestamp column exists and contains valid timestamp values.
"""

import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from ..utils import redaction

from .base import BaseValidator, ValidationResult
from ..config import Config

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


# SQL types accepted for the ``timestamp`` column. All three denote calendar
# values and all three are already mapped to a temporal SQLAlchemy type by
# ``Database._get_sqlalchemy_type`` (``DATE`` -> ``Date``, ``DATETIME`` /
# ``TIMESTAMP`` -> ``DateTime``), so accepting them here does not push a failure
# further down the pipeline.
#
# Why not ``TIMESTAMP`` alone (#489): ``schema_inference._infer_datetime`` — the
# source of every INFERRED schema, and therefore of every ingest that does not
# pass an explicit one — returns ``DATETIME`` when a value carries a time of day
# and ``DATE`` otherwise. It can never emit ``TIMESTAMP``. Requiring exactly
# ``TIMESTAMP`` therefore rejected 100% of inferred time-series-forecasting
# schemas no matter what the data looked like: a date-only column failed with
# "found 'DATE'", and adding a time of day only changed the message to "found
# 'DATETIME'". The bundled Python template hard-codes ``"timestamp":
# "TIMESTAMP"``, which is why the template path worked and the gap went
# unnoticed. Genuinely wrong types (VARCHAR, INT, …) are still rejected.
TEMPORAL_TIMESTAMP_TYPES = frozenset({"TIMESTAMP", "DATETIME", "DATE"})


def parse_month_first_with_ambiguity_mask(values):
    """Month-first ``format='mixed'`` parse + the locale-ambiguity mask.

    "03.04.2026" is Apr 3 read day-first (EU) but Mar 4 read month-first
    (US); ``format='mixed'`` silently picks one and corrupts the whole
    series with no error. A value that parses BOTH ways to different dates
    is ambiguous and must be rejected. ISO-8601 values (YYYY-…) are exempt:
    pandas with ``dayfirst=True`` still swaps their components, which would
    falsely flag every ISO date.

    Shared by ``TimeFormatValidator`` (global TSF check) and
    ``PerGroupTimeOrderedValidator`` (per-sequence TSC check) so the guard
    cannot drift between them (review: #359).

    Returns:
        (timestamps, ambiguous_mask) — the month-first parse (NaT where
        unparseable) and the boolean mask of ambiguous positions.
    """
    timestamps = pd.to_datetime(values, format="mixed", errors="coerce")
    day_first = pd.to_datetime(values, format="mixed", dayfirst=True, errors="coerce")
    iso_like = values.astype(str).str.match(r"^\d{4}-")
    ambiguous_mask = (
        timestamps.notna() & day_first.notna() & (timestamps != day_first) & ~iso_like
    )
    return timestamps, ambiguous_mask


class TimeFormatValidator(BaseValidator):
    """Validator for timestamp format.

    Ensures:
    1. Column "timestamp" exists in the dataset
    2. Timestamp column in schema is a calendar type — ``TIMESTAMP``,
       ``DATETIME`` or ``DATE`` (see ``TEMPORAL_TIMESTAMP_TYPES``); text and
       numeric types are rejected
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

                if base_type not in TEMPORAL_TIMESTAMP_TYPES:
                    errors.append(
                        f"Timestamp column in schema must be a date/time type "
                        f"({', '.join(sorted(TEMPORAL_TIMESTAMP_TYPES))}), but found "
                        f"'{self.schema['timestamp']}'. For time series forecasting the "
                        f"timestamp column must hold calendar values, not text or numbers."
                    )
                    return self._create_result(is_valid=False, errors=errors)

            df = self._load_data(data)
            if df is None or df.empty:
                return self._create_result(
                    is_valid=False, errors=["No data found to validate"]
                )

            if "timestamp" not in df.columns:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"Required column 'timestamp' not found. Available: {list(df.columns)}"
                    ],
                )

            # Parse timestamps + locale-ambiguity mask (shared helper — the
            # per-group TSC validator applies the identical guard).
            timestamps, ambiguous_mask = parse_month_first_with_ambiguity_mask(
                df["timestamp"]
            )
            metadata = {"rows_checked": len(df)}
            if ambiguous_mask.any():
                ambiguous_count = int(ambiguous_mask.sum())
                offender_rows = df.index[ambiguous_mask][:5].tolist()
                shapes = sorted(
                    {
                        redaction.mask_shape(v)
                        for v in df["timestamp"][ambiguous_mask].astype(str).head(5)
                    }
                )
                errors.append(
                    f"Found {ambiguous_count} ambiguous date(s) in 'timestamp' that parse "
                    f"differently day-first vs month-first, at "
                    f"{redaction.row_refs(offender_rows, ambiguous_count)} "
                    f"(masked shapes: {shapes}). Use ISO 8601 "
                    f"(YYYY-MM-DD or YYYY-MM-DD HH:MM:SS) to remove the ambiguity."
                )
                metadata["ambiguous_timestamps"] = ambiguous_count

            # Check for invalid/missing timestamps
            invalid_mask = timestamps.isna()
            if invalid_mask.any():
                invalid_count = invalid_mask.sum()
                invalid_rows = [i + 1 for i in df.index[invalid_mask][:10]]
                errors.append(
                    f"Found {invalid_count} invalid timestamp(s) at rows: {invalid_rows}"
                )
                metadata["invalid_timestamps"] = invalid_count

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                metadata=metadata,
            )

        except Exception as e:
            logger.error(f"Time format validation error: {e}")
            return self._create_result(
                is_valid=False, errors=[f"Validation error: {str(e)}"]
            )

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
