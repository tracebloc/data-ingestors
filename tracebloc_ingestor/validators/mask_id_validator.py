"""Mask-ID Column Validator Module.

Fail-fast guard for the semantic-segmentation ``mask_id`` contract (backend#816),
run at preflight (before the destination table is created).

Why this exists: the training client resolves each mask file from a MySQL
column named ``mask_id`` (``segmentation_dataset_pytorch.py`` does
``str(row["mask_id"])`` → filename, with NO naming-convention fallback). So a
semseg ingest whose manifest either lacks the ``mask_id`` column, or leaves it
empty on some rows, produces a table the client cannot read: every affected
mask lookup crashes with ``FileNotFoundError`` at train time — a late, opaque
failure for what is simply a missing / unpopulated link column in the manifest.

Rather than silently mutating the dataset schema at ingest time (auto-adding
the column), the contract is REQUIRED and ENFORCED here: the manifest must
declare ``mask_id`` and populate it on every ingestable row. Per-category
knowledge (which sidecar link column a category needs) stays on the modality
spec/registry (backend#796); this validator is the reusable enforcement of a
"sidecar link column" check, parametrized by column name so other file-linked
modalities can reuse it.
"""

import logging
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.coercion import NA_SENTINELS

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

# How many offending rows to name in the error before truncating, so the
# message stays actionable on a large manifest without dumping thousands of ids.
_MAX_REPORTED_ROWS = 10

# Sentinel distinguishing "no schema argument was supplied" (bare construction /
# unit test — skip the declaration check, validate only the CSV) from an
# explicitly-passed schema, INCLUDING ``None`` / ``{}`` (a real ingest with no
# schema at all, which MUST be rejected because ``mask_id`` can't be a stored
# column then). The semseg factory always passes the resolved schema, so the
# real ingest path always runs the declaration check.
_SCHEMA_UNSET = object()


class MaskIdColumnValidator(BaseValidator):
    """Reject a semantic-segmentation manifest that would NOT produce a
    populated ``mask_id`` DB column — i.e. the column is undeclared in the
    schema, absent from the manifest CSV, or empty on any ingestable row.

    The stored table carries ``mask_id`` only when it is BOTH declared in the
    ingest schema (``RecordProcessor`` keeps a column iff it's in the schema — an
    undeclared ``mask_id`` is silently dropped, the exact regression backend#816
    reported) AND present + populated in the manifest CSV. This validator
    enforces the whole contract at preflight so the failure is a clear "fix the
    manifest / schema" message, rather than a ``FileNotFoundError`` deep in the
    training client, which reads the column with no naming-convention fallback.

    Attributes:
        column: CSV / schema column the training client reads to locate each
            sidecar (mask) file. Resolved case- and whitespace-insensitively via
            :meth:`BaseValidator._match_column`, matching how the ingestor and
            sibling column validators resolve it. Parametrized so this reads as a
            reusable sidecar-link-column check, not a semseg-only special case.
        schema: the resolved ingest schema, used to enforce that ``column`` is a
            DECLARED (hence stored) column. Omit it (bare construction) to skip
            the declaration check and validate only the CSV; the semseg factory
            always passes it, so the real ingest path always enforces it.
    """

    def __init__(
        self,
        column: str = "mask_id",
        schema: Any = _SCHEMA_UNSET,
        name: str = "Mask Id Column",
    ):
        super().__init__(name)
        self.column = column or "mask_id"
        # Only enforce the schema-declaration half of the contract when a schema
        # was actually supplied (see _SCHEMA_UNSET). An explicit None / {} counts
        # as "supplied" and fails the check — a semseg ingest with no schema
        # can't store mask_id at all.
        self._check_declared = schema is not _SCHEMA_UNSET
        self._schema = {} if schema is _SCHEMA_UNSET or schema is None else schema

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        try:
            # (1) Declaration: mask_id must be a schema column, else the ingestor
            # drops it (RecordProcessor keeps a column iff it's in the schema) and
            # the stored table has no mask_id at all — the exact backend#816 shape
            # a CSV-only check waves through. Checked first: it's independent of
            # the manifest contents and is the root cause when it fails.
            if self._check_declared and (
                self._match_column(list(self._schema.keys()), self.column) is None
            ):
                return self._create_result(
                    is_valid=False,
                    errors=[self._undeclared_message()],
                    metadata={
                        "column": self.column,
                        "schema_columns": list(map(str, self._schema.keys())),
                    },
                )

            columns = self._read_columns(data)
            if columns is None:
                # Not a CSV manifest / DataFrame we can introspect (e.g. a JSON
                # input or an unreadable file) — not this validator's error to
                # raise; the read path / sibling validators surface those. Pass,
                # exactly like LabelColumnValidator.
                return self._create_result(
                    is_valid=True,
                    metadata={"checked": False, "column": self.column},
                )

            resolved = self._match_column(columns, self.column)
            if resolved is None:
                return self._create_result(
                    is_valid=False,
                    errors=[self._missing_column_message(columns)],
                    metadata={
                        "column": self.column,
                        "columns": list(map(str, columns)),
                    },
                )

            empty_rows = self._empty_value_rows(data, resolved)
            if empty_rows:
                return self._create_result(
                    is_valid=False,
                    errors=[self._empty_value_message(empty_rows)],
                    metadata={"column": self.column, "empty_rows": empty_rows},
                )

            return self._create_result(
                is_valid=True,
                metadata={"checked": True, "column": self.column},
            )

        except Exception as e:  # noqa: BLE001 — mirror sibling validators
            logger.error(f"Error during mask-id-column validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Mask-id-column validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )

    def _undeclared_message(self) -> str:
        declared = ", ".join(map(str, self._schema.keys())) or "(none)"
        return (
            f"Semantic segmentation requires '{self.column}' to be a DECLARED "
            f"schema column (e.g. schema={{'{self.column}': 'VARCHAR(255)'}}), but "
            f"the ingest schema declares: {declared}. An undeclared '{self.column}' "
            f"is dropped at ingest, so the stored table has no '{self.column}' "
            f"column and the training client raises FileNotFoundError for every "
            f"mask at train time (backend#816). Declare '{self.column}' in the "
            f"schema, matching templates/semantic_segmentation/."
        )

    def _missing_column_message(self, columns: List[Any]) -> str:
        return (
            f"Required column '{self.column}' not found in semantic-segmentation "
            f"manifest CSV header (columns: {', '.join(map(str, columns))}). The "
            f"training client reads '{self.column}' to locate each mask file "
            f"(backend#816) — without it, every mask lookup raises "
            f"FileNotFoundError at train time. Add a '{self.column}' column "
            f"mapping each row to its mask filename. See "
            f"templates/semantic_segmentation/ for the expected manifest layout."
        )

    def _empty_value_message(self, empty_rows: List[int]) -> str:
        shown = ", ".join(str(r) for r in empty_rows[:_MAX_REPORTED_ROWS])
        more = (
            f" (and {len(empty_rows) - _MAX_REPORTED_ROWS} more)"
            if len(empty_rows) > _MAX_REPORTED_ROWS
            else ""
        )
        return (
            f"Required column '{self.column}' is empty/NULL on row(s) {shown}{more} "
            f"of the semantic-segmentation manifest. The training client reads "
            f"'{self.column}' to locate each mask file (backend#816) — a blank "
            f"value makes it derive a garbage filename and raise FileNotFoundError "
            f"at train time. Populate '{self.column}' on every row with its mask "
            f"filename. See templates/semantic_segmentation/ for the expected "
            f"manifest layout."
        )

    def _read_columns(self, data: Any) -> Optional[list]:
        """Return the column names of the manifest, or ``None`` when the input
        isn't an introspectable CSV / DataFrame.

        Reads only the header row for a CSV path (``nrows=0``) so a wide or
        multi-GB manifest costs almost nothing. A read error (missing file,
        unparseable) returns ``None`` — a benign skip, since the read/transfer
        path raises its own clear error for those. Mirrors
        :meth:`LabelColumnValidator._read_columns`.
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

    def _empty_value_rows(self, data: Any, resolved_column: str) -> List[int]:
        """Return the 1-based data-row numbers whose ``resolved_column`` value the
        ingestor would store as an unresolvable ``mask_id``. Empty list means
        every row is populated.

        A value is "empty" when it is a missing cell (NaN), an ``NA_SENTINELS``
        token, or stringifies to only whitespace. This mirrors the ingestor
        BYTE-FOR-BYTE: ``mask_id`` is a declared schema column, so ``CSVIngestor``
        reads it with ``build_csv_na_values`` (the curated ``NA_SENTINELS`` set)
        under ``keep_default_na=False`` and stores every sentinel as SQL NULL —
        after which the client derives a garbage filename and raises
        ``FileNotFoundError``. Reading with pandas' DEFAULT NA set instead would
        DIVERGE from that curated set (it misses lowercase ``"none"`` and adds
        tokens like ``"#NA"``/``"-nan"`` the ingestor keeps verbatim), so a
        ``mask_id`` of ``"none"`` would pass here and then be nulled at ingest —
        the exact preflight/write mismatch this gate exists to prevent. Hence the
        raw read (``keep_default_na=False``, ``dtype=str``) + explicit
        ``NA_SENTINELS`` membership. The whitespace-only case is flagged too
        because the client strips before use (``str(row["mask_id"]).strip()``).
        Row numbers are 1-based over data rows (header excluded), matching how a
        user counts records in the manifest.
        """
        if isinstance(data, pd.DataFrame):
            df = data
        else:
            try:
                # Raw read (no NA coercion, dtype=str) so sentinel tokens survive
                # to be matched against NA_SENTINELS — identical to the ingestor.
                df = pd.read_csv(
                    data, encoding="utf-8", keep_default_na=False, dtype=str
                )
            except Exception:  # noqa: BLE001 — unreadable -> defer to read path
                return []

        if resolved_column not in df.columns:
            return []

        na_tokens = set(NA_SENTINELS)
        series = df[resolved_column]
        offending: List[int] = []
        for i, value in enumerate(series, start=1):
            if pd.isna(value) or str(value) in na_tokens or str(value).strip() == "":
                offending.append(i)
        return offending
