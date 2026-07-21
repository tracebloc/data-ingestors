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
3. **No EXACT ``filename`` column** (file-bearing categories, #372) — the file
   column is misnamed (``image_id``) or only a case variant (``Filename``). The
   cluster's transfer read is a case-sensitive ``record.get("filename")``, so it
   resolves to no key and every row is dropped cluster-side with "No filename
   found in record" (exit 9) AFTER the full upload. Rejected up front here so
   the ingestor's own preflight agrees with its transfer read for every client;
   mirrors the CLI's ``CheckImageFilenameColumn`` (cli#373).
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from ..config import Config
from ..file_transfer import _has_extension, _safe_join
from ..utils import redaction
from ..utils.constants import FileExtension
from ..utils.csv_dialect import read_dialect_kwargs, validate_csv_options
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
            ``"filename"``; required as an EXACT lowercase match, #372).
        csv_options: the run's pandas read options (delimiter / encoding /
            quoting). Threaded so every read here tokenizes the manifest the
            SAME way the ingest write path does — otherwise a non-comma / BOM
            manifest reads as one mashed column and the exact-``filename`` check
            (#372) false-rejects a valid dataset. Mirrors the grouped validators
            (#376). A malformed value is rejected at construction.
    """

    def __init__(
        self,
        file_subdir: Optional[str] = None,
        extension: str = FileExtension.TXT,
        filename_column: str = "filename",
        name: str = "Ingestable Records",
        csv_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(name)
        self.file_subdir = file_subdir
        ext = extension or FileExtension.TXT
        self.extension = ext if ext.startswith(".") else f".{ext}"
        self.filename_column = filename_column
        self._csv_options = csv_options or {}
        # Fail fast at construction on a malformed dialect, not mid-scan —
        # same contract as the grouped validators (#376).
        validate_csv_options(self._csv_options)

    def _dialect_kwargs(self) -> Dict[str, Any]:
        """pandas ``read_csv`` dialect kwargs (sep / encoding / quoting) matching
        the ingest write path, via the shared :func:`read_dialect_kwargs` (#376).
        Callers merge their own read-shape kwargs (``nrows`` / ``usecols`` / …)
        on top."""
        return read_dialect_kwargs(self._csv_options)

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

            # 2) File-bearing: enforce the filename-column contract, then the
            #    all-referenced-files-missing check.
            if self.file_subdir:
                # 2a) No EXACT lowercase `filename` column -> doomed at the
                #     cluster's case-sensitive transfer read; reject up front
                #     (#372) rather than let it fail late after upload.
                col_result = self._check_filename_column(path)
                if not col_result.is_valid:
                    return col_result
                # 2b) Column present but every referenced file missing -> zero
                #     ingestable records.
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
                **self._dialect_kwargs(),
                nrows=1,
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

        Runs only after the manifest is confirmed to carry an EXACT lowercase
        filename column (:meth:`_check_filename_column`), so ``filename_col`` is
        always present here.
        """
        src_root = (self._config or config).SRC_PATH
        subdir = os.path.join(src_root, self.file_subdir)

        filename_col = self._exact_filename_column(self._read_header(path))
        checked = 0
        try:
            chunks = pd.read_csv(
                path,
                **self._dialect_kwargs(),
                usecols=[filename_col],
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

    def _read_header(self, path: Path) -> list:
        """The CSV header as a plain list of raw column names; ``[]`` if the
        header can't be read (best-effort probe)."""
        try:
            return list(
                pd.read_csv(path, **self._dialect_kwargs(), nrows=0).columns
            )
        except Exception:  # noqa: BLE001 — header probe is best-effort
            return []

    def _exact_filename_column(self, header: list) -> Optional[str]:
        """The header naming each file, matched EXACTLY — whitespace-trimmed but
        case-SENSITIVE (#372).

        The cluster reads each file with a case-sensitive
        ``record.get("filename")`` at transfer, over a record whose keys are the
        CSV header after pandas strips surrounding whitespace
        (``chunk.columns.str.strip()``). So ``filename`` and ``" filename "``
        resolve, but ``Filename`` / ``FILENAME`` do NOT — matching what the
        transfer will actually find, and the CLI's ``imageFileColIndex``
        (cli#373). Returns the raw header (un-stripped) so callers can index the
        frame with it; ``None`` when no exact column is present.
        """
        for col in header:
            if str(col).strip() == self.filename_column:
                return col
        return None

    def _check_filename_column(self, path: Path) -> ValidationResult:
        """Reject a file-bearing manifest with no EXACT lowercase filename
        column, before any upload (#372).

        File-bearing tasks match each row to its file by the ``filename`` column,
        and the cluster's transfer read is case-sensitive — a wrong name
        (``image_id``) or a case variant (``Filename``) resolves to no key, so
        every row is dropped cluster-side with "No filename found in record"
        (exit 9) AFTER the full upload. Previously this validator matched the
        column case-insensitively and DEFERRED when it was absent ("not this
        validator's error to raise"), which let both cases pass preflight and
        fail late. Rejecting here — fail-fast, in the dry-run — makes the
        ingestor's own preflight agree with its transfer read for every client,
        and mirrors the CLI's up-front ``CheckImageFilenameColumn`` (cli#373).
        """
        header = self._read_header(path)
        if self._exact_filename_column(header) is not None:
            return self._create_result(is_valid=True, metadata={"checked": True})

        # Cap the column list in the MESSAGE (redaction.column_preview, as #372
        # did for mask_id_validator) but keep the FULL header in metadata so
        # CLI/programmatic consumers don't lose it — the learned Bugbot rule
        # ("cap the list, keep the full set in metadata").
        full_columns = [str(c) for c in header]
        cols = redaction.column_preview(header) if header else "<none>"
        # A case variant (``Filename``) still resolves case-insensitively — give
        # a targeted "rename to lowercase" hint rather than "no column at all".
        variant = self._match_column(header, self.filename_column)
        if variant is not None:
            variant = str(variant).strip()
            return self._create_result(
                is_valid=False,
                errors=[
                    f"Column {variant!r} must be lowercase "
                    f"'{self.filename_column}': the cluster reads each file with "
                    f"a case-sensitive record.get('{self.filename_column}') at "
                    f"transfer, so {variant!r} would upload, then fail with 'No "
                    f"filename found in record'. Rename it to "
                    f"'{self.filename_column}' and re-run."
                ],
                metadata={
                    "reason": "filename_column_case_variant",
                    "columns": full_columns,
                },
            )
        return self._create_result(
            is_valid=False,
            errors=[
                f"No '{self.filename_column}' column in the manifest (columns: "
                f"{cols}) — file-bearing tasks match each row to its file by "
                f"that column, and the cluster drops every row without it ('No "
                f"filename found in record'). Rename your file column to "
                f"'{self.filename_column}' and re-run."
            ],
            metadata={"reason": "filename_column_missing", "columns": full_columns},
        )
