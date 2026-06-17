"""Text Content Validator Module.

Content-level check for NLP categories (text_classification,
token_classification, masked_language_modeling). ``FileTypeValidator`` only
checks the file *extension*, so a file named ``doc1.txt`` that actually holds
binary / non-UTF-8 bytes — or is empty — passed validation and was ingested
silently; the dataset author only discovered the corruption at training time.

This samples the referenced text files and asserts their *content* is decodable
UTF-8 text:

- **Binary / non-decodable content** (a NUL byte, or bytes that aren't valid
  UTF-8) is REJECTED — that's silent data corruption.
- **Empty / whitespace-only documents** are WARNED about (they carry no signal
  for training but may be intentional placeholders in some flows).

Sampling + a per-file byte cap keep the pass bounded on large datasets: a
deterministic strided sample of files, and only the first chunk of each file is
read (enough to catch binary content and emptiness).
"""

import codecs
import logging
import os
from typing import Any, List, Optional, Tuple

from ..config import Config
from ..file_transfer import _has_extension, _safe_join
from ..text_profile import _sample
from ..utils.constants import FileExtension
from .base import BaseValidator, ValidationResult

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

# Cap reported messages so a wholly-broken dataset yields an actionable summary
# rather than tens of thousands of lines (mirrors BIOLabelValidator).
_MAX_REPORTED = 50


class TextContentValidator(BaseValidator):
    """Validate that referenced NLP text files hold decodable UTF-8 text.

    Attributes:
        texts_path: Subdirectory under ``SRC_PATH`` holding the text files
            (``"texts"`` for text/token classification, ``"sequences"`` for
            masked language modeling) — mirrors ``FileTypeValidator(path=...)``.
        extension: Expected text-file extension (default ``.txt``).
        filename_column: CSV column naming each sample's file (default
            ``"filename"``; resolved case-insensitively).
        sample_size: Max number of files to inspect (deterministic strided
            sample over the referenced files).
        max_bytes: Max bytes read per file for the content check.
    """

    def __init__(
        self,
        texts_path: str = "texts",
        extension: str = FileExtension.TXT,
        filename_column: str = "filename",
        sample_size: int = 500,
        max_bytes: int = 65_536,
        name: str = "Text Content",
    ):
        super().__init__(name)
        self.texts_path = texts_path
        ext = extension or FileExtension.TXT
        self.extension = ext if ext.startswith(".") else f".{ext}"
        self.filename_column = filename_column
        self.sample_size = sample_size
        self.max_bytes = max_bytes

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        try:
            df = self._load_data(data)
            if df is None or df.empty:
                # Nothing to inspect; the empty-CSV case is the
                # IngestableRecordsValidator's job, not this one.
                return self._create_result(is_valid=True, metadata={"docs_checked": 0})

            filename_col = self._match_column(df.columns, self.filename_column)
            if filename_col is None:
                return self._create_result(
                    is_valid=True,
                    metadata={"docs_checked": 0, "reason": "no_filename_column"},
                )

            src_root = (self._config or config).SRC_PATH
            filenames = [
                str(v).strip() for v in df[filename_col].tolist() if str(v).strip()
            ]

            errors: List[str] = []
            warnings: List[str] = []
            checked = 0
            for filename in _sample(filenames, self.sample_size):
                resolved = (
                    filename
                    if _has_extension(filename)
                    else f"{filename}{self.extension}"
                )
                # Resolve as the transfer does (``_safe_join`` under SRC_PATH):
                # an absolute / ``..`` manifest value is rejected by the transfer
                # (#239), so we neither read nor flag a file outside the dataset
                # dir — skip it (its missing-ness is the records validator's job).
                try:
                    text_path = _safe_join(src_root, self.texts_path, resolved)
                except ValueError:
                    continue
                if not os.path.isfile(text_path):
                    # A missing referenced file is surfaced by
                    # IngestableRecordsValidator / the transfer path, not here.
                    continue
                checked += 1
                level, message = self._inspect(text_path, resolved)
                if level == "error":
                    errors.append(message)
                elif level == "warning":
                    warnings.append(message)
                if len(errors) >= _MAX_REPORTED:
                    errors.append("... further errors suppressed.")
                    break

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                metadata={"docs_checked": checked},
            )

        except Exception as e:  # noqa: BLE001 — mirror sibling validators
            logger.error(f"Error during text content validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Text content validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )

    def _inspect(self, path: str, label: str) -> Tuple[Optional[str], str]:
        """Inspect one file's content. Returns ``(level, message)`` where level
        is ``"error"`` (binary/non-UTF-8), ``"warning"`` (empty/whitespace), or
        ``None`` (clean — message ignored)."""
        try:
            with open(path, "rb") as fh:
                raw = fh.read(self.max_bytes)
        except OSError as e:
            return "error", f"'{self.texts_path}/{label}': could not read file: {e}"

        if not raw.strip():
            return (
                "warning",
                f"'{self.texts_path}/{label}' is empty or whitespace-only — it "
                f"carries no text for training.",
            )

        if b"\x00" in raw:
            return (
                "error",
                f"'{self.texts_path}/{label}' contains a NUL byte (0x00) — the "
                f"file is binary, not UTF-8 text. Re-export it as plain UTF-8 "
                f"text and re-ingest.",
            )

        # Incremental decode. ``final`` is True only when we read the WHOLE file
        # (the read returned fewer bytes than the cap, i.e. EOF) — so a truncated
        # / wrongly-encoded trailing multibyte sequence in a small file is
        # FLUSHED and raises, instead of sitting unvalidated in the decoder
        # buffer. When we stopped at the cap (len(raw) == max_bytes) the file may
        # continue, so we DON'T finalize — a legitimate multibyte char split at
        # the cap must not be mistaken for invalid input.
        final = len(raw) < self.max_bytes
        try:
            codecs.getincrementaldecoder("utf-8")().decode(raw, final=final)
        except UnicodeDecodeError:
            return (
                "error",
                f"'{self.texts_path}/{label}' is not valid UTF-8 text — it looks "
                f"binary or wrongly encoded. Re-export it as plain UTF-8 text "
                f"and re-ingest.",
            )

        return None, ""
