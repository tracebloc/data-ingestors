"""Tab-Separated Record Validator base.

Shared structural check for the NLP modalities whose ``.txt`` samples are a
single tab-separated record with a fixed number of non-empty fields:

- ``embeddings`` — a ``anchor<TAB>positive`` pair OR
  ``anchor<TAB>positive<TAB>negative`` triplet (2 or 3 fields), via
  :class:`~tracebloc_ingestor.validators.contrastive_pairs_validator.ContrastivePairsValidator`.
- ``sentence_pair_classification`` — a ``text_a<TAB>text_b`` pair (exactly 2
  fields), via
  :class:`~tracebloc_ingestor.validators.sentence_pair_validator.SentencePairValidator`.

``FileTypeValidator`` only checks the extension and ``TextContentValidator``
only checks UTF-8 decodability — neither sees this structure. This base does the
manifest walk, path resolution (shared ``_safe_join`` — so preflight can't
disagree with what the transfer copies, #239), reading and the record-level
checks (single line, allowed field count, non-empty fields). Subclasses supply
only the modality-specific field contract (allowed counts + the phrasing used in
error messages), so the scaffold lives once instead of being copy-pasted per
modality.

UTF-8 / binary hygiene stays ``TextContentValidator``'s job; emptiness it
already warns about, so an empty / whitespace-only file is left untouched here
(no double reporting), exactly like the per-modality subclasses used to do.
"""

import logging
import os
from typing import Any, List, Optional, Tuple

from ..config import Config
from ..file_transfer import _has_extension, _safe_join
from ..utils.constants import FileExtension
from .base import BaseValidator, ValidationResult

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

# Cap per-row errors so a wholly-malformed dataset yields an actionable summary
# rather than tens of thousands of lines (mirrors BIOLabelValidator).
_MAX_REPORTED_ERRORS = 50


class TabSeparatedRecordValidator(BaseValidator):
    """Validate that each referenced ``.txt`` is a single tab-separated record
    with an allowed number of non-empty fields.

    Template-method base: the manifest walk, path resolution, reading and the
    record-level checks live here; subclasses configure the field contract via
    :attr:`ALLOWED_FIELD_COUNTS` and the three phrasing hooks below.

    Attributes:
        texts_path: Subdirectory under ``SRC_PATH`` holding the ``.txt`` files
            (``"texts"``; mirrors ``FileTypeValidator(path=...)``).
        extension: Expected text-file extension (default ``.txt``).
        filename_column: CSV column naming each sample's file (default
            ``"filename"``; resolved case-insensitively).
    """

    #: Field counts a valid record may have. Subclasses override (e.g. ``(2, 3)``
    #: for pair-or-triplet, ``(2,)`` for exactly-a-pair).
    ALLOWED_FIELD_COUNTS: Tuple[int, ...] = ()
    #: Noun used in the "…" validation-error / log messages (lower-case).
    _ERROR_NOUN: str = "tab-separated record"

    def __init__(
        self,
        texts_path: str = "texts",
        extension: str = FileExtension.TXT,
        filename_column: str = "filename",
        name: str = "Tab-Separated Record",
    ):
        super().__init__(name)
        self.texts_path = texts_path
        ext = extension or FileExtension.TXT
        self.extension = ext if ext.startswith(".") else f".{ext}"
        self.filename_column = filename_column

    # -- modality-specific phrasing hooks --------------------------------------

    def _expected_fields_phrase(self) -> str:
        """The field-count clause used in the wrong-count error, e.g.
        ``"2 (anchor<TAB>positive) or 3 (anchor<TAB>positive<TAB>negative)"``."""
        raise NotImplementedError

    def _multiline_hint(self) -> str:
        """The one-record-per-file remediation hint used in the multi-line
        error, e.g. ``"Put one 'anchor<TAB>positive' pair … per .txt."``."""
        raise NotImplementedError

    def _field_names(self, count: int) -> str:
        """Comma-joined field names for a record of ``count`` fields, used in
        the empty-field error, e.g. ``"anchor, positive"``."""
        raise NotImplementedError

    # -- shared scaffold -------------------------------------------------------

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

            src_root = (self._config or config).SRC_PATH
            errors: List[str] = []
            checked = 0
            for idx, row in df.iterrows():
                if len(errors) >= _MAX_REPORTED_ERRORS:
                    errors.append("... further errors suppressed.")
                    break
                checked += 1
                error = self._validate_row(row, idx, filename_col, src_root)
                if error:
                    errors.append(error)

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                metadata={"rows_checked": checked},
            )

        except Exception as e:  # noqa: BLE001 — mirror sibling validators
            logger.error(f"Error during {self._ERROR_NOUN} validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"{self._ERROR_NOUN.capitalize()} validation error: {str(e)}"],
            )

    def _validate_row(
        self,
        row: Any,
        idx: Any,
        filename_col: str,
        src_root: str,
    ) -> Optional[str]:
        row_label = f"Row {idx}"
        filename = str(row[filename_col])

        # Resolve the .txt with text_transfer's exact rule (shared
        # _has_extension): append the extension only when the CSV filename has
        # none. Deterministic on purpose — probing the filesystem for
        # alternatives here could validate one file while text_transfer copies a
        # different one.
        resolved = (
            filename if _has_extension(filename) else f"{filename}{self.extension}"
        )
        # Resolve as the transfer does (``_safe_join`` under SRC_PATH), so
        # preflight can't disagree with copy behavior: an absolute / ``..``
        # manifest value is rejected by the transfer (#239), so we neither read
        # nor flag a file outside the dataset dir — its missing-ness is the
        # records validator's job. Mirrors TextContentValidator.
        try:
            text_path = _safe_join(src_root, self.texts_path, resolved)
        except ValueError:
            return None
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

    def _check_structure(
        self, content: str, filename: str, row_label: str
    ) -> Optional[str]:
        """Return an error message if ``content`` is not a single-line
        tab-separated record with an allowed field count, else ``None``.

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
                f"record but the file spans multiple lines. {self._multiline_hint()}"
            )

        parts = record.split("\t")
        if len(parts) not in self.ALLOWED_FIELD_COUNTS:
            return (
                f"{row_label} ('{filename}'): expected {self._expected_fields_phrase()} "
                f"tab-separated fields, found {len(parts)}. Separate each field "
                f"with exactly one tab."
            )

        empties = [i + 1 for i, p in enumerate(parts) if not p.strip()]
        if empties:
            return (
                f"{row_label} ('{filename}'): field(s) {empties} are empty — "
                f"every field ({self._field_names(len(parts))}) must be non-empty."
            )

        return None

    @staticmethod
    def _resolve_column(df: Any, name: str) -> Optional[str]:
        """Return the actual column name matching ``name`` case-insensitively."""
        if name in df.columns:
            return name
        lowered = {c.lower(): c for c in df.columns}
        return lowered.get(name.lower())
