"""Per-Group Time Ordered Validator Module (backend#1054 WS1).

Validates that timestamps are ordered chronologically WITHIN each sequence
group. The existing ``TimeOrderedValidator`` checks GLOBAL monotonicity —
correct for forecasting's single merged series, but actively wrong for
grouped time-series-classification files (T4): a file interleaving two
perfectly-ordered patients fails the global check. This validator applies the
same monotonic (non-decreasing) rule per ``sequence_id`` instead.

The ``timestamp`` column may be either a SQL TIMESTAMP or a plain numeric
step index (Decision-2). The TIMESTAMP branch calls the shared
``parse_month_first_with_ambiguity_mask`` helper from ``time_format_validator`` ("03.04.2026" is Apr 3
day-first but Mar 4 month-first — reject rather than silently corrupt); the
numeric branch is a plain ``pd.to_numeric`` cast.
"""

import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from ..utils import redaction
from ..utils.columns import resolve_column

from .base import BaseValidator, ValidationResult
from .time_format_validator import parse_month_first_with_ambiguity_mask
from ..config import Config

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

# Schema base types that select the numeric (step-index) branch.
_NUMERIC_BASE_TYPES = {
    "INT",
    "INTEGER",
    "TINYINT",
    "SMALLINT",
    "MEDIUMINT",
    "BIGINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
    "NUMERIC",
}


class PerGroupTimeOrderedValidator(BaseValidator):
    """Validator for per-sequence timestamp ordering.

    Ensures, for every sequence group:
    1. The time column is present and every value parses (TIMESTAMP or
       numeric step index, selected by the schema's declared type)
    2. TIMESTAMP values are not locale-ambiguous (day-first vs month-first)
    3. The values are monotonic non-decreasing within the group

    Attributes:
        sequence_column: Name of the sequence group column (default:
            "sequence_id")
        time_column: Name of the time column (default: "timestamp")
        schema: Optional schema dictionary — its declared type for the time
            column selects the TIMESTAMP vs numeric branch; without a
            declared type the TIMESTAMP branch is used (the ingest schema
            contract always declares it)
    """

    def __init__(
        self,
        sequence_column: Optional[str] = None,
        time_column: Optional[str] = None,
        schema: Optional[dict] = None,
        name: str = "Per Group Time Ordered Validator",
    ):
        super().__init__(name)
        self.sequence_column = (
            sequence_column if sequence_column is not None else "sequence_id"
        )
        self.time_column = time_column if time_column is not None else "timestamp"
        self.schema = schema or {}

    def _declared_base_type(self) -> Optional[str]:
        """The schema's declared base type for the time column, or None."""
        declared = self.schema.get(self.time_column)
        if declared is None:
            # Schema keys may differ in case from the fixed name.
            key = resolve_column(self.schema.keys(), self.time_column)
            declared = self.schema.get(key) if key else None
        if declared is None:
            return None
        return str(declared).upper().strip().split("(")[0].split()[0]

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate per-sequence timestamp ordering."""
        try:
            df = self._load_data(data)
            if df is None or df.empty:
                return self._create_result(
                    is_valid=False, errors=["No data found to validate"]
                )

            seq_col = resolve_column(df.columns, self.sequence_column)
            if seq_col is None:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"Required sequence column '{self.sequence_column}' "
                        f"not found. Available: {list(df.columns)}"
                    ],
                )
            time_col = resolve_column(df.columns, self.time_column)
            if time_col is None:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"Required column '{self.time_column}' not found. "
                        f"Available: {list(df.columns)}"
                    ],
                )

            errors = []
            metadata = {
                "rows_checked": len(df),
                "sequence_column": seq_col,
                "time_column": time_col,
            }

            base_type = self._declared_base_type()
            if base_type in _NUMERIC_BASE_TYPES:
                values = pd.to_numeric(df[time_col], errors="coerce")
                metadata["time_kind"] = "numeric"
            elif base_type is not None:
                values = self._parse_timestamps(df, time_col, errors, metadata)
                metadata["time_kind"] = "timestamp"
            else:
                # The ingest schema contract REQUIRES a declared type for the
                # time column (ingest.v1.json sequence-columns rule), so this
                # path is unreachable through the pipeline; for direct/test
                # callers default to the TIMESTAMP branch (review: #359 —
                # the former data-sniffing fallback was dead code).
                values = self._parse_timestamps(df, time_col, errors, metadata)
                metadata["time_kind"] = "timestamp"

            # Null / unparseable order keys are errors: a timestep without a
            # valid position cannot be ordered within its sequence.
            invalid_mask = values.isna()
            if invalid_mask.any():
                invalid_count = int(invalid_mask.sum())
                offender_rows = df.index[invalid_mask][:5].tolist()
                errors.append(
                    f"Column '{time_col}' contains {invalid_count} "
                    f"missing/invalid value(s) at "
                    f"{redaction.row_refs(offender_rows, invalid_count)}. "
                    f"Every timestep row needs a valid "
                    f"{metadata['time_kind']} value to order it within its "
                    f"sequence."
                )
                metadata["invalid_timestamps"] = invalid_count

            # Per-group monotonic (non-decreasing) check on the valid rows.
            valid = df.loc[~invalid_mask, [seq_col]].assign(_t=values[~invalid_mask])
            zero = (
                pd.Timedelta(0)
                if pd.api.types.is_datetime64_any_dtype(valid["_t"])
                else 0
            )
            out_of_order_sequences = []
            first_bad_rows = []
            for _seq, group in valid.groupby(seq_col, dropna=True, sort=False):
                if not group["_t"].is_monotonic_increasing:
                    out_of_order_sequences.append(_seq)
                    diffs = group["_t"].diff()
                    bad = group.index[diffs < zero]
                    if len(bad) > 0:
                        first_bad_rows.append(int(bad[0]))
            metadata["sequences_checked"] = int(valid[seq_col].nunique(dropna=True))

            if out_of_order_sequences:
                n = len(out_of_order_sequences)
                errors.append(
                    f"Found {n} sequence(s) with out-of-order "
                    f"'{time_col}' values (first offending "
                    f"{redaction.row_refs(sorted(first_bad_rows)[:5], n)}). "
                    f"Timestep rows must be sorted by '{time_col}' within "
                    f"each '{seq_col}' — sort each sequence's rows "
                    f"ascending and re-run. Interleaving different "
                    f"sequences is fine; ordering is only checked within a "
                    f"sequence."
                )
                metadata["out_of_order_sequences"] = n
            elif not invalid_mask.any():
                metadata["is_ordered"] = True

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                metadata=metadata,
            )

        except Exception as e:
            # #226: never interpolate exception text into errors/logs —
            # parser/dtype messages can embed cell contents. Type + location
            # only; the details stay on-prem.
            logger.error(
                f"Per-group time ordered validation error: "
                f"{type(e).__name__} (message suppressed: it can embed "
                f"cell values, #226)"
            )
            return self._create_result(
                is_valid=False,
                errors=[
                    f"Validation error: unexpected {type(e).__name__} while "
                    f"checking '{self.time_column}' ordering within each "
                    f"'{self.sequence_column}' (exception text suppressed: "
                    f"it can embed cell values, #226)."
                ],
                metadata={
                    "error_type": "validation_exception",
                    "exception_type": type(e).__name__,
                },
            )

    def _parse_timestamps(self, df, time_col, errors, metadata) -> "pd.Series":
        """Parse TIMESTAMP values with the locale-ambiguity guard.

        Delegates to the shared helper in ``time_format_validator`` (one
        guard, two call sites — review: #359); assembles this validator's
        own error message from the mask.
        """
        timestamps, ambiguous_mask = parse_month_first_with_ambiguity_mask(
            df[time_col]
        )
        if ambiguous_mask.any():
            ambiguous_count = int(ambiguous_mask.sum())
            offender_rows = df.index[ambiguous_mask][:5].tolist()
            shapes = sorted(
                {
                    redaction.mask_shape(v)
                    for v in df[time_col][ambiguous_mask].astype(str).head(5)
                }
            )
            errors.append(
                f"Found {ambiguous_count} ambiguous date(s) in "
                f"'{time_col}' that parse differently day-first vs "
                f"month-first, at "
                f"{redaction.row_refs(offender_rows, ambiguous_count)} "
                f"(masked shapes: {shapes}). Use ISO 8601 "
                f"(YYYY-MM-DD or YYYY-MM-DD HH:MM:SS) to remove the "
                f"ambiguity."
            )
            metadata["ambiguous_timestamps"] = ambiguous_count
        return timestamps

    def _load_data(self, data: Any) -> Optional["pd.DataFrame"]:
        """Load the FULL file (whole-group check; safe pre-ingest, T15)."""
        try:
            if isinstance(data, pd.DataFrame):
                return data
            if isinstance(data, (str, Path)):
                file_path = Path(data).expanduser()
                if file_path.exists() and file_path.suffix.lower() == ".csv":
                    return pd.read_csv(file_path, encoding="utf-8", on_bad_lines="warn")

            return None
        except Exception as e:
            # #226: parse errors can quote file content — type only.
            logger.error(
                f"Error loading data: {type(e).__name__} (message "
                f"suppressed: it can embed cell values, #226)"
            )
            return None
