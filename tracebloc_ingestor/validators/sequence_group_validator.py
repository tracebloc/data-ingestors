"""Sequence Group Validator Module (backend#1054 WS1).

Validates the ``sequence_id`` group column for sequence-grouped categories
(time_series_classification). It validates that:
1. The sequence column is present in the dataset (fixed name ``sequence_id``,
   Decision-2; resolved case-/whitespace-insensitively like every other
   configured column — #340 rule).
2. No row has a null/empty sequence id (an unassignable timestep row).
3. ``data_id.strategy=column`` is NOT pointed at the sequence column (trap
   T6): ``data_id`` is UNIQUE, so mapping it from ``sequence_id`` would
   upsert-collapse every sequence to its last row — silently destroying the
   dataset.

"""

import logging
from pathlib import Path
from typing import Any, Optional

from ..utils import redaction
from ..utils.columns import resolve_column
from ..utils.csv_dialect import read_dialect_kwargs, validate_csv_options

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


class SequenceGroupValidator(BaseValidator):
    """Validator for the sequence group column of grouped time-series data.

    This validator ensures that:
    1. The sequence column (default: "sequence_id") is present in the dataset
    2. No row carries a null/empty sequence id
    3. The run's ``data_id`` strategy does not map ``data_id`` from the
       sequence column (T6 — the UNIQUE upsert would collapse each sequence
       to one row)

    Attributes:
        sequence_column: Name of the sequence group column (default:
            "sequence_id")
        unique_id_column: The run's data_id source column when
            ``data_id.strategy=column`` is configured; ``None`` otherwise
        schema: Optional schema dictionary (used only for a VARCHAR-type
            advisory warning)
    """

    def __init__(
        self,
        sequence_column: Optional[str] = None,
        unique_id_column: Optional[str] = None,
        schema: Optional[dict] = None,
        csv_options: Optional[dict] = None,
        name: str = "Sequence Group Validator",
    ):
        """Initialize the sequence group validator.

        Args:
            sequence_column: Name of the sequence group column (default:
                "sequence_id")
            unique_id_column: The configured ``data_id`` source column
                (``data_id.strategy=column``), for the T6 guard
            schema: Optional schema dictionary
            csv_options: The run's pandas read options (delimiter / encoding /
                ...), so the manifest is parsed exactly as CSVIngestor parses
                it. Without it a non-comma or BOM manifest that ingests fine is
                falsely rejected — or passes for the wrong reason — here.
            name: Human-readable name of the validator
        """
        super().__init__(name)
        self.sequence_column = (
            sequence_column if sequence_column is not None else "sequence_id"
        )
        self.unique_id_column = unique_id_column
        self.schema = schema or {}
        self._csv_options = csv_options or {}
        # Fail fast on a malformed dialect value (non-string sep, invalid
        # quoting, ...) at construction, rather than as a generic load/"no data"
        # error mid-scan — same contract as MaskIdColumnValidator (bugbot #376).
        validate_csv_options(self._csv_options)

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate the sequence group column.

        Args:
            data: CSV file path or pandas DataFrame to validate
            **kwargs: Additional validation parameters
                - sample_size: Number of rows to sample (default: None = all;
                  whole-group checks need the full file, so leave unset)

        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            if not PANDAS_AVAILABLE:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        "Pandas not available. Cannot perform sequence group validation."
                    ],
                    metadata={"pandas_available": False},
                )

            # T6 guard first: it is a CONFIG error, independent of the data,
            # so it must surface even for a file that fails to load.
            if self.unique_id_column is not None and (
                str(self.unique_id_column).strip().lower()
                == str(self.sequence_column).strip().lower()
            ):
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"data_id.strategy=column must not use the sequence "
                        f"column '{self.sequence_column}': data_id is UNIQUE "
                        f"per row, so every timestep row of a sequence would "
                        f"upsert onto the same data_id and each sequence "
                        f"would silently collapse to a single row. Use the "
                        f"default UUID strategy (omit data_id) or "
                        f"strategy=content_hash instead."
                    ],
                    metadata={
                        "sequence_column": self.sequence_column,
                        "unique_id_column": self.unique_id_column,
                    },
                )

            df = self._load_data(data, kwargs.get("sample_size", None))
            if df is None or df.empty:
                return self._create_result(
                    is_valid=False,
                    errors=["No data found to validate"],
                    metadata={"rows_checked": 0},
                )

            # Resolve the sequence column with the shared #340 rule so this
            # validator and the read path agree on the actual header.
            resolved = resolve_column(df.columns, self.sequence_column)
            if resolved is None:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"Required sequence column '{self.sequence_column}' "
                        f"not found in dataset. Available columns: "
                        f"{redaction.column_preview(df.columns)}. "
                        f"Time-series classification "
                        f"data must carry a '{self.sequence_column}' column "
                        f"grouping the timestep rows of each sequence "
                        f"(e.g. a patient / device / session id)."
                    ],
                    metadata={
                        "sequence_column": self.sequence_column,
                        "available_columns": list(df.columns),
                    },
                )

            errors = []
            warnings = []
            metadata = {
                "sequence_column": resolved,
                "rows_checked": len(df),
            }

            # Advisory only: the pinned contract types sequence_id VARCHAR;
            # a numeric schema type still groups correctly, so warn, don't
            # reject.
            declared = self.schema.get(resolved) or self.schema.get(
                self.sequence_column
            )
            if declared:
                base_type = str(declared).upper().strip().split("(")[0].split()[0]
                if base_type not in ("VARCHAR", "CHAR", "TEXT"):
                    warnings.append(
                        f"Sequence column '{resolved}' is declared as "
                        f"'{declared}' in the schema; the platform contract "
                        f"types it VARCHAR (it is a group KEY, not a "
                        f"feature). Consider VARCHAR(64)."
                    )

            # No row may have a null/empty sequence id — such a timestep row
            # belongs to no sequence and would silently orphan at training.
            ids = df[resolved]
            null_mask = ids.isna() | (ids.astype(str).str.strip() == "")
            null_count = int(null_mask.sum())
            if null_count > 0:
                offender_rows = df.index[null_mask][:5].tolist()
                errors.append(
                    f"Sequence column '{resolved}' contains {null_count} "
                    f"null/empty value(s) at "
                    f"{redaction.row_refs(offender_rows, null_count)}. "
                    f"Every timestep row must carry the id of the sequence "
                    f"it belongs to."
                )
                metadata["null_count"] = null_count

            # No per-sequence stats here: the shipping number_of_sequences
            # is computed once in ingestors/base.py from the post-insert DB
            # counts (the source of truth); a validator-side copy was
            # unused duplication (review: #359).

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                metadata=metadata,
            )

        except Exception as e:
            # #226: never interpolate exception text into errors/logs —
            # parser/dtype messages can embed cell contents. Type + location
            # only; the details stay on-prem.
            logger.error(
                f"Error during sequence group validation: "
                f"{type(e).__name__} (message suppressed: it can embed "
                f"cell values, #226)"
            )
            return self._create_result(
                is_valid=False,
                errors=[
                    f"Sequence group validation error: unexpected "
                    f"{type(e).__name__} while checking column "
                    f"'{self.sequence_column}' (exception text suppressed: "
                    f"it can embed cell values, #226)."
                ],
                metadata={
                    "error_type": "validation_exception",
                    "exception_type": type(e).__name__,
                },
            )

    def _load_data(
        self, data: Any, sample_size: Optional[int]
    ) -> Optional["pd.DataFrame"]:
        """Load data from input source.

        Args:
            data: Input data (file path or DataFrame)
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
                    # Parse with the run's delimiter / encoding / quoting so the
                    # validator tokenizes the manifest byte-identically to
                    # CSVIngestor (bugbot #371); read-shape kwargs layered on top.
                    return pd.read_csv(
                        path,
                        nrows=sample_size,
                        on_bad_lines="warn",
                        **read_dialect_kwargs(self._csv_options),
                    )
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
