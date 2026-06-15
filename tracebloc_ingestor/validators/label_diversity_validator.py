"""Label Diversity Validator Module.

Classification categories need at least 2 distinct label values to be
learnable — a single-class dataset is not a classification problem (and
the backend's ``/global_meta/prepare/`` endpoint correctly rejects it
with ``HTTP 400: "Please provide atleast 2 labels."``).

Without a preflight check, a degenerate single-label CSV:
  1. passes per-record validation (every cell is fine in isolation),
  2. inserts every row into MySQL,
  3. dies at backend ``prepare_dataset`` with the message above, and
  4. surfaces to the user as the generic "Backend failed to prepare the
     dataset; it was NOT registered" — the actual cause is buried in a
     preceding log line.

This validator catches the degenerate case at the gate, before any DB
or backend round-trip, and names the actual distinct label(s) found in
the message so the user immediately knows what's wrong with the input.

Skipped for regression-family categories (tabular_regression,
time_series_forecasting, time_to_event_prediction) — those have
continuous targets where uniqueness is meaningless — and for
self-supervised categories (masked_language_modeling) which have no
label column at all.
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


class LabelDiversityValidator(BaseValidator):
    """Reject classification datasets with fewer than 2 distinct label values.

    Attributes:
        label_column: CSV column holding the class label. Resolved
            case-insensitively against the actual header.
        min_distinct: Minimum required distinct non-null label values
            (default 2). The backend's contract is exactly 2; the
            parameter is exposed only for future stricter use cases.
    """

    def __init__(
        self,
        label_column: str = "label",
        min_distinct: int = 2,
        name: str = "Label Diversity Validator",
    ):
        super().__init__(name)
        self.label_column = label_column
        self.min_distinct = min_distinct

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        try:
            df = self._load_data(data)
            if df is None or df.empty:
                # An empty input is the empty-CSV / no-data class —
                # other validators surface that with their own clear
                # messages (CSV ingestor raises at read; DataValidator
                # returns "No data found to validate"). Don't double-
                # report here.
                return self._create_result(
                    is_valid=True,
                    metadata={"rows_checked": 0, "label_column": self.label_column},
                )

            col = self._resolve_column(df, self.label_column)
            if col is None:
                # The label column isn't in the CSV — caller's
                # responsibility to surface (DataValidator or the
                # ingestor will reject). Don't double-report.
                return self._create_result(
                    is_valid=True,
                    warnings=[
                        f"label column '{self.label_column}' not found in CSV; "
                        f"skipping label-diversity check"
                    ],
                    metadata={"label_column": self.label_column},
                )

            distinct = df[col].dropna().unique()
            n = len(distinct)
            if n < self.min_distinct:
                # Show the actual values found, capped — a user with a
                # 50k-row degenerate dataset doesn't need the full list,
                # but the first few values plus the count tell them
                # exactly what's wrong with the input.
                sample = list(distinct[:5])
                # Surface counts per distinct value to make "all one
                # class" stand out clearly: "{'X': 10}" vs "{'X': 10000}"
                # both clearly read as single-class but the latter gives
                # the user the full row count for free.
                value_counts = df[col].value_counts(dropna=True).head(5).to_dict()
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"Classification category requires at least "
                        f"{self.min_distinct} distinct label values in column "
                        f"'{col}'; this dataset has {n} distinct value(s): "
                        f"{sample}. Value counts: {value_counts}. If this is "
                        f"intentional (e.g. you have a continuous target), "
                        f"pick a regression-family category like "
                        f"tabular_regression or time_series_forecasting "
                        f"instead."
                    ],
                    metadata={
                        "label_column": col,
                        "distinct_count": n,
                        "value_counts": value_counts,
                    },
                )

            return self._create_result(
                is_valid=True,
                metadata={
                    "label_column": col,
                    "distinct_count": n,
                },
            )

        except Exception as e:  # noqa: BLE001 — mirror sibling validators
            logger.error(f"Error during label-diversity validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Label-diversity validation error: {str(e)}"],
            )

    def _load_data(self, data: Any) -> Optional[pd.DataFrame]:
        """Load just the label column from CSV (memory-efficient) or pass
        through a DataFrame as-is. Mirrors DataValidator's loader shape but
        only reads the one column it needs."""
        try:
            if isinstance(data, pd.DataFrame):
                return data
            if isinstance(data, (str, Path)):
                path = Path(data)
                if not path.exists():
                    return None
                if path.suffix.lower() == ".csv":
                    # Load only the label column — for a 50-feature wide
                    # CSV (or a multi-GB proteomics panel) we don't need
                    # the other columns to count distinct labels. usecols
                    # is resolved case-insensitively against the actual
                    # header by reading the header row first.
                    with open(path, "r", encoding="utf-8") as fh:
                        header_line = fh.readline().rstrip("\n\r")
                    headers = [h.strip() for h in header_line.split(",")]
                    actual = next(
                        (h for h in headers if h.lower() == self.label_column.lower()),
                        None,
                    )
                    if actual is None:
                        return pd.read_csv(path, nrows=1, encoding="utf-8")
                    return pd.read_csv(
                        path, usecols=[actual], encoding="utf-8"
                    )
                if path.suffix.lower() == ".json":
                    return pd.read_json(path, orient="records")
            return None
        except Exception as e:
            logger.error(f"Error loading data for label-diversity check: {e}")
            return None

    @staticmethod
    def _resolve_column(df: pd.DataFrame, name: str) -> Optional[str]:
        """Return the actual column name matching ``name`` case-insensitively."""
        if name in df.columns:
            return name
        lowered = {c.lower(): c for c in df.columns}
        return lowered.get(name.lower())
