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
3. **IOB2 sequence (warning)** — an ``I-<TYPE>`` should be preceded by a
   ``B-<TYPE>`` / ``I-<TYPE>`` of the same type (entities start with ``B-``).
   An ``I-`` that opens an entity is malformed under IOB2 but LEGAL under IOB1,
   so it's surfaced as a WARNING rather than a hard reject — failing it would
   wrongly break valid IOB1 datasets.
"""

import logging
import os
import re
from typing import Any, List, Optional, Tuple

import pandas as pd

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.constants import FileExtension
from ..utils import redaction

config = Config()
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

            texts_dir = os.path.join((self._config or config).SRC_PATH, self.texts_path)
            errors: List[str] = []
            warnings: List[str] = []

            for idx, row in df.iterrows():
                if len(errors) >= _MAX_REPORTED_ERRORS:
                    errors.append("... further errors suppressed.")
                    break
                row_errors, row_warnings = self._validate_row(
                    row, idx, filename_col, label_col, texts_dir
                )
                errors.extend(row_errors)
                if len(warnings) < _MAX_REPORTED_ERRORS:
                    warnings.extend(row_warnings)

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings or None,
                metadata={"rows_checked": len(df)},
            )

        except Exception as e:  # noqa: BLE001 — mirror sibling validators
            logger.error(f"Error during BIO label validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"BIO label validation error: {str(e)}"],
            )

    def _validate_row(
        self,
        row: pd.Series,
        idx: Any,
        filename_col: str,
        label_col: str,
        texts_dir: str,
    ) -> Tuple[List[str], List[str]]:
        row_label = f"Row {idx}"
        filename = str(row[filename_col])
        tags = str(row[label_col]).strip().split()

        # Invalid tag format (independent of the file).
        bad = [t for t in tags if not _BIO_TAG_RE.match(t)]
        errors: List[str] = []
        warnings: List[str] = []
        if bad:
            errors.append(
                f"{row_label} ('{filename}'): {len(bad)} invalid BIO tag(s), "
                f"masked shapes {sorted({redaction.mask_shape(t) for t in bad[:5]})}; "
                f"each tag must be 'O' or 'B-<TYPE>' / 'I-<TYPE>'."
            )
        else:
            # IOB2 sequence anomaly (warning) — only meaningful on well-formed
            # tags, and independent of the .txt, so check it before file I/O so
            # it's still surfaced when the file is missing/unreadable below.
            warnings.extend(self._iob2_sequence_warnings(tags, filename, row_label))

        # Resolve the .txt with text_transfer's exact rule (shared
        # _has_extension): append the extension only when the CSV filename
        # has none. Deterministic on purpose — probing the filesystem for
        # alternatives here could validate one file while text_transfer
        # copies a different one with the same BIO labels.
        from ..file_transfer import _has_extension

        resolved = (
            filename if _has_extension(filename) else f"{filename}{self.extension}"
        )
        text_path = os.path.join(texts_dir, resolved)
        if not os.path.isfile(text_path):
            errors.append(
                f"{row_label}: text file not found at "
                f"'{self.texts_path}/{resolved}'."
            )
            return errors, warnings

        try:
            with open(text_path, "r", encoding="utf-8") as f:
                word_count = len(f.read().strip().split())
        except OSError as e:
            errors.append(f"{row_label}: could not read text file: {e}")
            return errors, warnings

        if word_count != len(tags):
            errors.append(
                f"{row_label} ('{filename}'): token/label count mismatch — "
                f"{word_count} word(s) in the .txt but {len(tags)} BIO tag(s) "
                f"in the label column. Each word must have exactly one tag."
            )

        return errors, warnings

    @staticmethod
    def _iob2_sequence_warnings(
        tags: List[str], filename: str, row_label: str
    ) -> List[str]:
        """Flag IOB2 transition anomalies: an ``I-<TYPE>`` not immediately
        preceded by a ``B-<TYPE>`` / ``I-<TYPE>`` of the SAME type — i.e. an
        entity that opens with ``I-`` instead of ``B-`` (orphan ``I-`` at the
        start, ``I-`` right after ``O``, or a type switch like ``B-PER I-ORG``).

        Returned as warnings, not errors: such sequences are malformed under
        IOB2 but LEGAL under IOB1 (a chunk may open with ``I-``), and this
        validator is scheme-agnostic — hard-failing would wrongly reject valid
        IOB1 datasets. The warning lets a user who intended IOB2 spot the
        problem without blocking IOB1 ingests.
        """
        offenders: List[str] = []
        prev = "O"
        for t in tags:
            if t.startswith("I-"):
                etype = t[2:]
                if prev != f"B-{etype}" and prev != f"I-{etype}":
                    offenders.append(t)
            prev = t
        if not offenders:
            return []
        return [
            f"{row_label} ('{filename}'): IOB2 transition anomaly — {offenders[:5]} "
            f"open an entity with 'I-' (not preceded by a 'B-'/'I-' of the same "
            f"type). Valid under IOB1 but malformed under IOB2; if you intended "
            f"IOB2, the entity should start with 'B-'."
        ]

    @staticmethod
    def _resolve_column(df: pd.DataFrame, name: str) -> Optional[str]:
        """Return the actual column name matching ``name`` case-insensitively."""
        if name in df.columns:
            return name
        lowered = {c.lower(): c for c in df.columns}
        return lowered.get(name.lower())
