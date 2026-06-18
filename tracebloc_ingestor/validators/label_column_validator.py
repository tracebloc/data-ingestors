"""Label Column Validator Module.

Fail-fast guard for the "configured label column is missing from the CSV
header" class of input error, run at preflight (before the destination table
is created) so a classification dataset whose CSV lacks the configured label
column is rejected EARLY with a clear, actionable message.

Why this exists (adversarial text-classification ingestion, dev ``ingestor:0.3.12``):
a ``text_classification`` CSV whose header is ``filename,extension`` (no
``label``), with ``label: label`` configured in the ingest config, slipped past
every validator:

  1. ``LabelDiversityValidator`` reads the label column lazily; when it's
     absent its loader returns ``None``, which the validator treats as the
     benign empty-input case and PASSES (it explicitly defers the missing-column
     case to "DataValidator or the ingestor").
  2. but the label column is stripped out of the schema before
     ``DataValidator`` sees it, so DataValidator never checks it either.
  3. so every record was cleaned with ``label=None``, sent to the backend, and
     rejected per-row with ``HTTP 400: {"label":["This field may not be
     null."]}`` — a late, confusing failure for what is simply a mis-named /
     missing column in the manifest.

This validator closes that gap for the classification categories that source
their label from a CSV column. Token classification already fails fast on the
same input via ``BIOLabelValidator`` (which resolves both the filename and
label columns and rejects a missing one), so it does not need this validator;
object detection sources its labels from XML annotations, not a CSV column, so
it is deliberately NOT wired to this validator.
"""

import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from .base import BaseValidator, ValidationResult
from ..config import Config

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class LabelColumnValidator(BaseValidator):
    """Reject a classification manifest whose configured label column is absent.

    Attributes:
        label_column: CSV column expected to hold the class label, as
            configured in the ingest YAML (default ``"label"``). Resolved
            case-insensitively against the actual header via
            :meth:`BaseValidator._match_column`, matching how the ingestor and
            the sibling label validators resolve it.
    """

    def __init__(
        self,
        label_column: str = "label",
        name: str = "Label Column",
    ):
        super().__init__(name)
        self.label_column = label_column or "label"

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        try:
            columns = self._read_columns(data)
            if columns is None:
                # Not a CSV manifest / DataFrame we can introspect (e.g. a JSON
                # input or an unreadable file) — not this validator's error to
                # raise; the read path / sibling validators surface those. Pass.
                return self._create_result(
                    is_valid=True,
                    metadata={"checked": False, "label_column": self.label_column},
                )

            if self._match_column(columns, self.label_column) is not None:
                return self._create_result(
                    is_valid=True,
                    metadata={"checked": True, "label_column": self.label_column},
                )

            return self._create_result(
                is_valid=False,
                errors=[
                    f"Configured label column '{self.label_column}' not found in "
                    f"CSV header (columns: {', '.join(map(str, columns))}). Set "
                    f"'label:' in the ingest config to an existing column, or add "
                    f"the '{self.label_column}' column to the CSV."
                ],
                metadata={
                    "label_column": self.label_column,
                    "columns": list(map(str, columns)),
                },
            )

        except Exception as e:  # noqa: BLE001 — mirror sibling validators
            logger.error(f"Error during label-column validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Label-column validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )

    def _read_columns(self, data: Any) -> Optional[list]:
        """Return the column names of the manifest, or ``None`` when the input
        isn't an introspectable CSV / DataFrame.

        Reads only the header row for a CSV path (``nrows=0``) so a wide or
        multi-GB manifest costs almost nothing. A read error (missing file,
        unparseable) returns ``None`` — a benign skip, since the read/transfer
        path raises its own clear error for those.
        """
        if isinstance(data, pd.DataFrame):
            return list(data.columns)
        if isinstance(data, (str, Path)):
            path = Path(data)
            if path.suffix.lower() != ".csv":
                return None
            try:
                header = pd.read_csv(path, nrows=0, encoding="utf-8")
            except Exception:  # noqa: BLE001 — missing/unparseable -> benign skip
                return None
            return list(header.columns)
        return None
