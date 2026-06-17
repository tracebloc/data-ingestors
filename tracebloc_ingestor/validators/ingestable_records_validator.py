"""Ingestable Records Validator Module.

Fail-fast guard for the "0 ingestable records" class of input error, run at
preflight (before the destination table is created) so a dataset that would
ingest nothing is rejected EARLY with a clear, source-truthful message — never
left as an orphan empty table that fails LATE at backend registration with a
misleading "its rows are already in the database" error.

Two distinct ways a CSV manifest yields zero records, both caught here:

1. **Header-only / empty CSV** — the file has a header row (or is empty) but no
   data rows. The existing zero-byte guard (#250, in ``CSVIngestor.read_data``)
   only fired on a *totally* empty file; a header-only CSV slipped through,
   created the table, ingested 0 rows, then failed late. This catches both.
2. **All referenced files missing** (file-bearing categories) — every
   ``filename`` the CSV references is absent under ``SRC_PATH/<subdir>/``, so
   every record is dropped at transfer time and 0 records land. Cross-checks the
   referenced filenames against what's actually staged; the per-directory
   ``FileTypeValidator`` only checks the *extension* of files that ARE present,
   not whether the CSV's filenames resolve to any of them.
"""

import logging
import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from ..config import Config
from ..file_transfer import _has_extension, _safe_join
from ..utils.constants import FileExtension
from .base import BaseValidator, ValidationResult

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class IngestableRecordsValidator(BaseValidator):
    """Reject a CSV manifest that would ingest zero records.

    Attributes:
        file_subdir: Subdirectory under ``SRC_PATH`` holding this category's
            referenced files (``"texts"`` / ``"sequences"`` / ``"images"`` …),
            mirroring ``FileTypeValidator(path=...)``. ``None`` for
            non-file-bearing (tabular / time-series) categories — only the
            header-only / empty-CSV row check runs then.
        extension: Expected file extension, appended to a CSV filename that
            carries none (same rule the transfer uses).
        filename_column: CSV column naming each sample's file (default
            ``"filename"``; resolved case-insensitively).
    """

    def __init__(
        self,
        file_subdir: Optional[str] = None,
        extension: str = FileExtension.TXT,
        filename_column: str = "filename",
        name: str = "Ingestable Records",
    ):
        super().__init__(name)
        self.file_subdir = file_subdir
        ext = extension or FileExtension.TXT
        self.extension = ext if ext.startswith(".") else f".{ext}"
        self.filename_column = filename_column

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        try:
            # Only CSV manifests are checked here. Non-CSV inputs (e.g. JSON)
            # are covered by their own validators; a non-path input is a direct
            # caller we don't second-guess.
            if not isinstance(data, (str, Path)) or Path(data).suffix.lower() != ".csv":
                return self._create_result(is_valid=True, metadata={"checked": False})

            path = Path(data)

            # 1) Header-only / empty CSV -> zero data rows.
            row_result = self._check_has_rows(path)
            if not row_result.is_valid:
                return row_result

            # 2) File-bearing: every referenced file missing -> zero records.
            if self.file_subdir:
                return self._check_referenced_files(path)

            return self._create_result(is_valid=True, metadata={"checked": True})

        except Exception as e:  # noqa: BLE001 — mirror sibling validators
            logger.error(f"Error during ingestable-records validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Ingestable records validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )

    def _check_has_rows(self, path: Path) -> ValidationResult:
        """Fail when the CSV has a header but no data rows (or is empty)."""
        try:
            # nrows=1 is enough to tell "has >=1 data row" from "header only",
            # without materialising a large file. dtype=str / keep_default_na
            # keep the read cheap and inference-free — we only count rows here.
            head = pd.read_csv(
                path,
                nrows=1,
                encoding="utf-8",
                dtype=str,
                keep_default_na=False,
            )
        except pd.errors.EmptyDataError:
            head = None

        if head is None or len(head) == 0:
            return self._create_result(
                is_valid=False,
                errors=[
                    f"No data rows found in CSV '{path.name}': the file has a "
                    f"header but no data rows (0 ingestable records). Add at "
                    f"least one data row and re-ingest."
                ],
                metadata={"data_rows": 0},
            )
        return self._create_result(is_valid=True, metadata={"data_rows": "at_least_1"})

    def _check_referenced_files(self, path: Path) -> ValidationResult:
        """Fail when NONE of the CSV's referenced files exist on disk.

        Early-exits on the first file that resolves, so a healthy dataset costs
        one ``stat``; only an all-missing dataset walks the whole column — which
        is exactly the zero-records case we want to surface up front.
        """
        src_root = (self._config or config).SRC_PATH
        subdir = os.path.join(src_root, self.file_subdir)

        filename_col = self._resolve_filename_column(path)
        if filename_col is None:
            # No filename column to cross-check — not this validator's error to
            # raise (the read/transfer path surfaces a missing column). Pass.
            return self._create_result(
                is_valid=True,
                metadata={"checked": False, "reason": "no_filename_column"},
            )

        checked = 0
        try:
            chunks = pd.read_csv(
                path,
                usecols=[filename_col],
                encoding="utf-8",
                chunksize=50_000,
                dtype=str,
                keep_default_na=False,
            )
            for chunk in chunks:
                for raw_name in chunk[filename_col]:
                    filename = str(raw_name).strip()
                    if not filename:
                        continue
                    checked += 1
                    resolved = (
                        filename
                        if _has_extension(filename)
                        else f"{filename}{self.extension}"
                    )
                    # Resolve EXACTLY as the transfer does (``_safe_join`` under
                    # SRC_PATH): an absolute / ``..`` manifest value that would
                    # otherwise let a plain join "find" a file outside the
                    # dataset dir is rejected by the transfer (#239), so it is
                    # NOT an ingestable file here either — skip it rather than
                    # let it mask the zero-record case this validator guards.
                    try:
                        candidate = _safe_join(src_root, self.file_subdir, resolved)
                    except ValueError:
                        continue
                    if os.path.isfile(candidate):
                        return self._create_result(
                            is_valid=True,
                            metadata={
                                "referenced_files_checked": checked,
                                "found_at_least_one": True,
                            },
                        )
        except pd.errors.EmptyDataError:
            checked = 0

        return self._create_result(
            is_valid=False,
            errors=[
                f"No referenced data files could be found under "
                f"'{self.file_subdir}/'; nothing to ingest (0 ingestable "
                f"records). Checked {checked} filename reference(s) against "
                f"'{subdir}/' and none exist on disk. Verify the files are "
                f"staged at that path and that the '{self.filename_column}' "
                f"column matches the staged filenames."
            ],
            metadata={"referenced_files_checked": checked, "found_at_least_one": False},
        )

    def _resolve_filename_column(self, path: Path) -> Optional[str]:
        """Case-insensitively resolve the filename column from the CSV header."""
        try:
            header = pd.read_csv(path, nrows=0, encoding="utf-8").columns
        except Exception:  # noqa: BLE001 — header probe is best-effort
            return None
        return self._match_column(header, self.filename_column)
