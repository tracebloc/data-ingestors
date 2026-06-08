"""BIO Label Validator Module.

Validates token-classification (NER/POS) labels before ingestion so bad
annotations are caught at upload time rather than failing deep inside
client-side training.

For each row it checks, against the corresponding ``.txt`` file (one
whitespace-tokenized word per token):

1. **Count alignment** — the ``label`` column holds a space-separated string
   of BIO tags, and there must be exactly one tag per word in the ``.txt``.
   A mismatch is the exact condition that makes the client drop tokens to
   ``-100`` (or raise), so we reject it here against the dataset author.
2. **Tag format** — every tag must be ``O`` or ``B-XXX`` / ``I-XXX`` (IOB2).
"""

import logging
import os
import re
from typing import Any, List, Optional

import pandas as pd

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.constants import FileExtension
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

# IOB2: "O", or "B-"/"I-" followed by a non-empty entity type.
_BIO_TAG_RE = re.compile(r"^(?:O|[BI]-\S+)$")

# Cap the number of per-row errors reported so a wholly-malformed dataset
# produces an actionable message instead of tens of thousands of lines.
_MAX_REPORTED_ERRORS = 50


class BIOLabelValidator(BaseValidator):
    """Validate BIO/IOB2 token-classification labels against their .txt files.

    Attributes:
        texts_path: Subdirectory under ``SRC_PATH`` holding the ``.txt`` files
            (mirrors ``FileTypeValidator(path=...)``; ``"texts"`` for token
            classification).
        extension: Expected text-file extension (default ``.txt``).
        filename_column: CSV column naming each sample's file (default
            ``"filename"``; resolved case-insensitively).
        label_column: CSV column holding the space-separated BIO tags
            (default ``"label"``; resolved case-insensitively).
    """

    def __init__(
        self,
        texts_path: str = "texts",
        extension: str = FileExtension.TXT,
        filename_column: str = "filename",
        label_column: str = "label",
        name: str = "BIO Label",
    ):
        super().__init__(name)
        self.texts_path = texts_path
        ext = extension or FileExtension.TXT
        self.extension = ext if ext.startswith(".") else f".{ext}"
        self.filename_column = filename_column
        self.label_column = label_column

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        try:
            df = self._load_data(data)
            if df is None or df.empty:
                return self._create_result(
                    is_valid=False, errors=["No data found to validate"]
                )

            filename_col = self._resolve_column(df, self.filename_column)
            label_col = self._resolve_column(df, self.label_column)
            missing = []
            if filename_col is None:
                missing.append(self.filename_column)
            if label_col is None:
                missing.append(self.label_column)
            if missing:
                return self._create_result(
                    is_valid=False,
                    errors=[f"Missing required column(s): {', '.join(missing)}"],
                )

            texts_dir = os.path.join(config.SRC_PATH, self.texts_path)
            errors: List[str] = []

            for idx, row in df.iterrows():
                if len(errors) >= _MAX_REPORTED_ERRORS:
                    errors.append("... further errors suppressed.")
                    break
                errors.extend(
                    self._validate_row(row, idx, filename_col, label_col, texts_dir)
                )

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                metadata={"rows_checked": len(df)},
            )

        except Exception as e:  # noqa: BLE001 — mirror sibling validators
            logger.error(f"Error during BIO label validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"BIO label validation error: {str(e)}"],
            )

    def _validate_row(
        self, row: pd.Series, idx: Any, filename_col: str, label_col: str, texts_dir: str
    ) -> List[str]:
        row_label = f"Row {idx}"
        filename = str(row[filename_col])
        tags = str(row[label_col]).strip().split()

        # Invalid tag format (independent of the file).
        bad = [t for t in tags if not _BIO_TAG_RE.match(t)]
        errors: List[str] = []
        if bad:
            errors.append(
                f"{row_label} ('{filename}'): invalid BIO tag(s) {bad[:5]}; "
                f"each tag must be 'O' or 'B-<TYPE>' / 'I-<TYPE>'."
            )

        text_path = os.path.join(texts_dir, f"{filename}{self.extension}")
        if not os.path.isfile(text_path):
            errors.append(
                f"{row_label}: text file not found at "
                f"'{self.texts_path}/{filename}{self.extension}'."
            )
            return errors

        try:
            with open(text_path, "r", encoding="utf-8") as f:
                word_count = len(f.read().strip().split())
        except OSError as e:
            errors.append(f"{row_label}: could not read text file: {e}")
            return errors

        if word_count != len(tags):
            errors.append(
                f"{row_label} ('{filename}'): token/label count mismatch — "
                f"{word_count} word(s) in the .txt but {len(tags)} BIO tag(s) "
                f"in the label column. Each word must have exactly one tag."
            )

        return errors

    @staticmethod
    def _resolve_column(df: pd.DataFrame, name: str) -> Optional[str]:
        """Return the actual column name matching ``name`` case-insensitively."""
        if name in df.columns:
            return name
        lowered = {c.lower(): c for c in df.columns}
        return lowered.get(name.lower())
