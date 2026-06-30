"""Contrastive Pairs Validator Module.

Structural check for the ``embeddings`` (self-supervised contrastive) modality.

Unlike the other raw-text NLP modalities (causal language modeling, seq2seq),
which also accept free-form text, an embeddings sample has a STRICT on-disk
shape: each ``.txt`` is a single tab-separated record, either

- a **pair** ``anchor<TAB>positive``                       (2 fields), or
- a **triplet** ``anchor<TAB>positive<TAB>negative``        (3 fields).

There is no label column — the supervision signal is the pairing itself. A file
that isn't exactly 2 or 3 non-empty tab fields (e.g. plain prose with no tab, an
empty field, or several records crammed into one file) is malformed: the
training client would silently mis-split it into the wrong number of views. So
we reject it here, against the dataset author, rather than letting it corrupt
training.

``FileTypeValidator`` only checks the extension and ``TextContentValidator``
only checks UTF-8 decodability — neither sees this structure — so this is the
dedicated structural validator, mirroring ``BIOLabelValidator`` for token
classification. UTF-8 / binary hygiene stays the shared
``TextContentValidator``'s job; emptiness it already warns about, so an
empty / whitespace-only file is left untouched here (no double reporting).
"""

import logging
import os
from typing import Any, List, Optional, Tuple

from ..config import Config
from ..utils.constants import FileExtension
from .base import BaseValidator, ValidationResult

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

# Cap per-row errors so a wholly-malformed dataset yields an actionable summary
# rather than tens of thousands of lines (mirrors BIOLabelValidator).
_MAX_REPORTED_ERRORS = 50


class ContrastivePairsValidator(BaseValidator):
    """Validate that each referenced ``.txt`` is a tab-separated contrastive
    pair or triplet.

    Attributes:
        texts_path: Subdirectory under ``SRC_PATH`` holding the ``.txt`` files
            (``"texts"`` for embeddings; mirrors ``FileTypeValidator(path=...)``).
        extension: Expected text-file extension (default ``.txt``).
        filename_column: CSV column naming each sample's file (default
            ``"filename"``; resolved case-insensitively).
    """

    def __init__(
        self,
        texts_path: str = "texts",
        extension: str = FileExtension.TXT,
        filename_column: str = "filename",
        name: str = "Contrastive Pairs",
    ):
        super().__init__(name)
        self.texts_path = texts_path
        ext = extension or FileExtension.TXT
        self.extension = ext if ext.startswith(".") else f".{ext}"
        self.filename_column = filename_column

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        try:
            df = self._load_data(data)
            if df is None or df.empty:
                # Nothing to inspect; the empty-CSV case is the
                # IngestableRecordsValidator's job, not this one.
                return self._create_result(is_valid=True, metadata={"rows_checked": 0})

            filename_col = self._resolve_column(df, self.filename_column)
            if filename_col is None:
                return self._create_result(
                    is_valid=False,
                    errors=[f"Missing required column: {self.filename_column}"],
                )

            texts_dir = os.path.join((self._config or config).SRC_PATH, self.texts_path)
            errors: List[str] = []
            checked = 0
            for idx, row in df.iterrows():
                if len(errors) >= _MAX_REPORTED_ERRORS:
                    errors.append("... further errors suppressed.")
                    break
                checked += 1
                error = self._validate_row(row, idx, filename_col, texts_dir)
                if error:
                    errors.append(error)

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                metadata={"rows_checked": checked},
            )

        except Exception as e:  # noqa: BLE001 — mirror sibling validators
            logger.error(f"Error during contrastive pairs validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Contrastive pairs validation error: {str(e)}"],
            )

    def _validate_row(
        self,
        row: Any,
        idx: Any,
        filename_col: str,
        texts_dir: str,
    ) -> Optional[str]:
        row_label = f"Row {idx}"
        filename = str(row[filename_col])

        # Resolve the .txt with text_transfer's exact rule (shared
        # _has_extension): append the extension only when the CSV filename has
        # none. Deterministic on purpose — probing the filesystem for
        # alternatives here could validate one file while text_transfer copies a
        # different one.
        from ..file_transfer import _has_extension

        resolved = (
            filename if _has_extension(filename) else f"{filename}{self.extension}"
        )
        text_path = os.path.join(texts_dir, resolved)
        if not os.path.isfile(text_path):
            return (
                f"{row_label}: text file not found at "
                f"'{self.texts_path}/{resolved}'."
            )

        try:
            with open(text_path, "r", encoding="utf-8") as f:
                content = f.read()
        except (OSError, UnicodeDecodeError) as e:
            # Binary / non-UTF-8 content is the shared TextContentValidator's
            # job to report; here we just can't structurally check it, so skip.
            return f"{row_label} ('{filename}'): could not read text file: {e}"

        return self._check_structure(content, filename, row_label)

    @staticmethod
    def _check_structure(
        content: str, filename: str, row_label: str
    ) -> Optional[str]:
        """Return an error message if ``content`` is not a single-line
        tab-separated pair/triplet, else ``None``.

        An empty / whitespace-only file is left to the shared
        TextContentValidator (which warns), so it returns ``None`` here.
        """
        # Drop only surrounding blank lines / trailing newline — NOT interior
        # tabs (a leading/trailing empty field must still be caught below).
        record = content.strip("\r\n")
        if not record.strip():
            return None  # empty — TextContentValidator's warning, not ours.

        # The contract is ONE record per file. A surviving interior line break
        # means several records were crammed into one file (or a field contains
        # a newline) — ambiguous for the single-sample-per-file contract.
        if "\n" in record or "\r" in record:
            return (
                f"{row_label} ('{filename}'): expected a single tab-separated "
                f"record but the file spans multiple lines. Put one "
                f"'anchor<TAB>positive' pair (or "
                f"'anchor<TAB>positive<TAB>negative' triplet) per .txt."
            )

        parts = record.split("\t")
        if len(parts) not in (2, 3):
            return (
                f"{row_label} ('{filename}'): expected 2 (anchor<TAB>positive) "
                f"or 3 (anchor<TAB>positive<TAB>negative) tab-separated fields, "
                f"found {len(parts)}. Separate each field with exactly one tab."
            )

        empties = [i + 1 for i, p in enumerate(parts) if not p.strip()]
        if empties:
            return (
                f"{row_label} ('{filename}'): field(s) {empties} are empty — "
                f"every field (anchor, positive{', negative' if len(parts) == 3 else ''}) "
                f"must be non-empty."
            )

        return None

    @staticmethod
    def _resolve_column(df: Any, name: str) -> Optional[str]:
        """Return the actual column name matching ``name`` case-insensitively."""
        if name in df.columns:
            return name
        lowered = {c.lower(): c for c in df.columns}
        return lowered.get(name.lower())
