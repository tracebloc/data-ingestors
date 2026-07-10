"""Label Constant Within Group Validator Module (backend#1054 WS1).

For sequence-grouped categories (time_series_classification) the label is a
per-SEQUENCE outcome: every timestep row of a ``sequence_id`` must carry the
same label value. A mid-sequence label flip means the file is not one-label-
per-sequence data (it is either mislabeled or actually a per-row task), so it
is rejected with a readable error before any row lands in MySQL.

Validators run on the FULL file pre-ingest (``ingestors/base.py``), so this
whole-group check is safe against chunked reads (T15).
"""

import logging
from pathlib import Path
from typing import Any, Optional

from ..utils.columns import resolve_column

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None

from .base import BaseValidator, ValidationResult
from ..config import Config

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class LabelConstantWithinGroupValidator(BaseValidator):
    """Validator for label constancy within each sequence group.

    This validator ensures that:
    1. The sequence and label columns are present in the dataset
    2. Every row of a given sequence carries the SAME label value (one
       outcome per sequence)

    Attributes:
        sequence_column: Name of the sequence group column (default:
            "sequence_id")
        label_column: Name of the label column (default: "label")
    """

    def __init__(
        self,
        sequence_column: Optional[str] = None,
        label_column: Optional[str] = None,
        name: str = "Label Constant Within Group Validator",
    ):
        """Initialize the label-constancy validator.

        Args:
            sequence_column: Name of the sequence group column (default:
                "sequence_id")
            label_column: Name of the label column (default: "label")
            name: Human-readable name of the validator
        """
        super().__init__(name)
        self.sequence_column = (
            sequence_column if sequence_column is not None else "sequence_id"
        )
        self.label_column = label_column if label_column is not None else "label"

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate that the label is constant within each sequence.

        Args:
            data: CSV file path or pandas DataFrame to validate
            **kwargs: Additional validation parameters (unused; the check is
                whole-group, so the full file is always read)

        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            if not PANDAS_AVAILABLE:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        "Pandas not available. Cannot perform label constancy validation."
                    ],
                    metadata={"pandas_available": False},
                )

            df = self._load_data(data)
            if df is None or df.empty:
                return self._create_result(
                    is_valid=False,
                    errors=["No data found to validate"],
                    metadata={"rows_checked": 0},
                )

            seq_col = resolve_column(df.columns, self.sequence_column)
            if seq_col is None:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"Required sequence column '{self.sequence_column}' "
                        f"not found in dataset. Available columns: "
                        f"{list(df.columns)}."
                    ],
                    metadata={"available_columns": list(df.columns)},
                )
            label_col = resolve_column(df.columns, self.label_column)
            if label_col is None:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"Required label column '{self.label_column}' not "
                        f"found in dataset. Available columns: "
                        f"{list(df.columns)}."
                    ],
                    metadata={"available_columns": list(df.columns)},
                )

            errors = []
            metadata = {
                "sequence_column": seq_col,
                "label_column": label_col,
                "rows_checked": len(df),
            }

            # One outcome per sequence: the label must be a single distinct
            # value within every sequence_id. dropna=False so a sequence
            # mixing a value with nulls is flagged too (a null label on some
            # timesteps is still an inconsistent outcome).
            grouped = df.groupby(seq_col, dropna=True, sort=False)[label_col]
            distinct = grouped.nunique(dropna=False)
            offenders = distinct[distinct > 1]
            metadata["sequences_checked"] = int(len(distinct))

            if len(offenders) > 0:
                # Row references, not sequence ids — ids are potentially PII
                # (#226 policy) and rows are what the customer greps for.
                offender_ids = set(offenders.index)
                offender_mask = df[seq_col].isin(offender_ids)
                first_rows = (
                    df.index[offender_mask].to_series().groupby(df[seq_col]).min()
                )
                sample_rows = sorted(int(r) for r in first_rows[:5])
                errors.append(
                    f"Found {len(offenders)} sequence(s) whose "
                    f"'{label_col}' value changes mid-sequence (first "
                    f"offending sequences start at rows {sample_rows}"
                    f"{' (+%d more)' % (len(offenders) - 5) if len(offenders) > 5 else ''}). "
                    f"Time-series classification assigns ONE label per "
                    f"sequence: every row of a '{seq_col}' must repeat the "
                    f"same label value."
                )
                metadata["inconsistent_sequences"] = int(len(offenders))

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
                f"Error during label constancy validation: "
                f"{type(e).__name__} (message suppressed: it can embed "
                f"cell values, #226)"
            )
            return self._create_result(
                is_valid=False,
                errors=[
                    f"Label constancy validation error: unexpected "
                    f"{type(e).__name__} while checking column "
                    f"'{self.label_column}' grouped by "
                    f"'{self.sequence_column}' (exception text suppressed: "
                    f"it can embed cell values, #226)."
                ],
                metadata={
                    "error_type": "validation_exception",
                    "exception_type": type(e).__name__,
                },
            )

    def _load_data(self, data: Any) -> Optional["pd.DataFrame"]:
        """Load the FULL data from the input source (whole-group check)."""
        try:
            if isinstance(data, pd.DataFrame):
                return data
            elif isinstance(data, (str, Path)):
                path = Path(data)
                if path.suffix.lower() == ".csv":
                    return pd.read_csv(path, encoding="utf-8", on_bad_lines="warn")
                logger.warning(f"Unsupported file type: {path.suffix}, \n\n{path}")
                return None
            else:
                logger.warning(f"Unsupported data type: {type(data)}")
                return None

        except Exception as e:
            # #226: parse errors can quote file content — type only.
            logger.error(
                f"Error loading data: {type(e).__name__} (message "
                f"suppressed: it can embed cell values, #226)"
            )
            return None
