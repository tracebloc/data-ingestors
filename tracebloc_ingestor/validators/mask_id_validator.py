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
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.coercion import NA_SENTINELS

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

# Sentinel distinguishing "no schema argument was supplied" (bare construction /
# unit test — skip the declaration check, validate only the CSV) from an
# explicitly-passed schema, INCLUDING ``None`` / ``{}`` (a real ingest with no
# schema at all, which MUST be rejected because ``mask_id`` can't be a stored
# column then). The semseg factory always passes the resolved schema, so the
# real ingest path always runs the declaration check.
_SCHEMA_UNSET = object()

# The ONLY csv_options forwarded to the validator's reads: the keys that control
# how the manifest is TOKENIZED into columns + cells, so its parse matches
# CSVIngestor's. A whitelist, not a blacklist — frame-RESTRUCTURING keys
# (index_col, header, names, usecols, skiprows, nrows, ...) are excluded, because
# the scan pins its own usecols/dtype/chunksize and forwarding a restructuring
# key would collide with those (a swallowed error that fail-opens the gate) or
# change which column mask_id is. New/unknown csv_options are dropped by default.
_READ_DIALECT_KEYS = frozenset(
    {
        "sep",
        "delimiter",
        "quotechar",
        "doublequote",
        "escapechar",
        "quoting",
        "skipinitialspace",
        "encoding",
        "encoding_errors",
        "lineterminator",
        "comment",
        "engine",
    }
)

# Cap on how many offending row indices the empty-value scan keeps: only a few
# are shown in the message (redaction.row_refs slices to 5) and the rest are a
# scalar count, so bounding the sample keeps an all-empty giant manifest from
# accumulating millions of ints — the OOM the chunked read exists to avoid.
_EMPTY_ROWS_SAMPLE_CAP = 20


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
        csv_options: the run's pandas read options (delimiter / encoding / ...),
            so the manifest is parsed exactly as :class:`CSVIngestor` parses it.
            Without it a non-comma or BOM manifest that ingests fine is falsely
            rejected here. The semseg factory passes ``options["csv_options"]``.
    """

    def __init__(
        self,
        column: str = "mask_id",
        schema: Any = _SCHEMA_UNSET,
        csv_options: Optional[Dict[str, Any]] = None,
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
        self._csv_options = csv_options or {}

    def _read_kwargs(self) -> Dict[str, Any]:
        """The pandas read options needed to parse the manifest exactly as
        CSVIngestor does (csv_ingestor.py:499/533) — delimiter, encoding AND the
        quoting dialect (quotechar / escapechar / quoting / ...), so preflight and
        the write path resolve the same columns + values from the same bytes. A
        sep or quotechar mismatch would split fields differently and desync them.
        """
        # Lazy import: csv_ingestor -> base -> validators_mapping ->
        # modalities.validators -> this module, so a top-level import would cycle.
        from ..ingestors.csv_ingestor import _bom_safe_encoding

        # Forward only the tokenizing dialect keys CSVIngestor honors (see
        # _READ_DIALECT_KEYS) — never a frame-restructuring key.
        opts = {k: v for k, v in self._csv_options.items() if k in _READ_DIALECT_KEYS}
        opts["encoding"] = _bom_safe_encoding(opts.get("encoding"))
        if "sep" not in opts and "delimiter" not in opts:
            opts["sep"] = ","
        return opts

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        try:
            # (1) Declaration: mask_id must be a schema column, else the ingestor
            # drops it (RecordProcessor keeps a column iff it's in the schema) and
            # the stored table has no mask_id at all — the exact backend#816 shape
            # a CSV-only check waves through. Checked first: it's independent of
            # the manifest contents and is the root cause when it fails.
            declared_key = (
                self._match_column(list(self._schema.keys()), self.column)
                if self._check_declared
                else None
            )
            if self._check_declared and declared_key is None:
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
                # Not a CSV manifest / DataFrame we can introspect (e.g. an
                # unreadable file) — not this validator's error to raise; the read
                # path / sibling validators surface those. Pass, like
                # LabelColumnValidator.
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

            # (2) Congruence: the CSV header must be an EXACT schema key. Column
            # detection above is case/whitespace-insensitive (user-friendly), but
            # the write path keeps a column ONLY on an exact match — csv_ingestor
            # ._validate_csv does set(schema) - set(stripped_headers), and
            # RecordProcessor filters `k in self.schema`. So a 'Mask_ID' header vs
            # a 'mask_id' schema key (or a trailing space) passes detection yet is
            # dropped / hard-rejected mid-ingest AFTER the table is created (#260).
            # Catch that divergence here with an actionable rename hint.
            if self._check_declared and resolved.strip() not in self._schema:
                return self._create_result(
                    is_valid=False,
                    errors=[self._case_mismatch_message(resolved, declared_key)],
                    metadata={
                        "column": self.column,
                        "csv_header": str(resolved),
                        "schema_columns": list(map(str, self._schema.keys())),
                    },
                )

            empty_rows, empty_count = self._empty_value_rows(data, resolved)
            if empty_count:
                return self._create_result(
                    is_valid=False,
                    errors=[self._empty_value_message(empty_rows, empty_count)],
                    metadata={
                        "column": self.column,
                        "empty_rows": empty_rows,
                        "empty_count": empty_count,
                    },
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

    def _case_mismatch_message(self, csv_header: str, schema_key: Optional[str]) -> str:
        return (
            f"The manifest's '{csv_header}' column does not exactly match its "
            f"schema declaration '{schema_key}' — the ingestor keeps a column "
            f"only on an exact-name match and the training client reads it by "
            f"name, so a case/whitespace difference makes the ingest fail mid-run "
            f"with 'Schema columns not present in CSV'. Rename the schema key and "
            f"the CSV header to the identical spelling (the template uses "
            f"lowercase '{self.column}'). See templates/semantic_segmentation/."
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

    def _empty_value_message(self, empty_rows: List[int], empty_count: int) -> str:
        from ..utils import redaction

        refs = redaction.row_refs(empty_rows, empty_count)
        return (
            f"Required column '{self.column}' is empty/NULL at {refs} of the "
            f"semantic-segmentation manifest. The training client reads "
            f"'{self.column}' to locate each mask file (backend#816) — a blank "
            f"value makes it derive a garbage filename and raise FileNotFoundError "
            f"at train time. Populate '{self.column}' on every row with its mask "
            f"filename. See templates/semantic_segmentation/ for the expected "
            f"manifest layout."
        )

    def _read_columns(self, data: Any) -> Optional[list]:
        """Return the manifest column names, or ``None`` when the input isn't an
        introspectable CSV / DataFrame.

        Reads only the header row for a path (``nrows=0``) so a wide or multi-GB
        manifest costs almost nothing, using the run's delimiter/encoding
        (:meth:`_read_kwargs`) so the parse matches CSVIngestor's. Any manifest
        extension is accepted — the ingestor reads the configured path regardless
        of suffix, so gating on ``.csv`` would silently skip this check for a
        ``.txt`` manifest. A read error (missing / unparseable file) returns
        ``None`` — a benign skip, since the read/transfer path raises its own
        clear error for those.
        """
        if isinstance(data, pd.DataFrame):
            return list(data.columns)
        if isinstance(data, (str, Path)):
            try:
                header = pd.read_csv(Path(data), nrows=0, **self._read_kwargs())
            except Exception:  # noqa: BLE001 — missing/unparseable -> benign skip
                return None
            return list(header.columns)
        return None

    def _empty_value_rows(self, data: Any, resolved_column: str):
        """Return ``(sample_indices, total_count)`` for the rows whose
        ``resolved_column`` value the ingestor would store as an unresolvable
        ``mask_id``. ``total_count == 0`` means every row is populated;
        ``sample_indices`` is a bounded (``_EMPTY_ROWS_SAMPLE_CAP``) 0-based
        prefix — enough for the error message — so an all-empty giant manifest
        can't accumulate millions of ints (the OOM the chunked read avoids, #137).

        A value is "empty" when it is a missing cell (NaN), an ``NA_SENTINELS``
        token, or stringifies to only whitespace. This mirrors the ingestor
        BYTE-FOR-BYTE: ``mask_id`` is a declared schema column, so ``CSVIngestor``
        reads it with ``build_csv_na_values`` (the curated ``NA_SENTINELS`` set)
        under ``keep_default_na=False`` and stores every sentinel as SQL NULL —
        after which the client derives a garbage filename and raises
        ``FileNotFoundError``. Reading with pandas' DEFAULT NA set instead would
        DIVERGE (it misses lowercase ``"none"`` and adds ``"#NA"``/``"-nan"`` the
        ingestor keeps), so a ``"none"`` mask_id would pass here then be nulled at
        ingest. The whitespace-only case is flagged too because the client strips
        (``str(row["mask_id"]).strip()``).

        Reads ONLY the resolved column, in chunks (``usecols`` + ``chunksize``) —
        like ``ingestable_records_validator`` / ``CSVIngestor._count_records`` —
        with the run's dialect (:meth:`_read_kwargs`). Selects the SCHEMA-exact
        ``resolved_column`` (not a case-insensitive match) so that when two
        headers collide under case/whitespace normalization it inspects the same
        column the ingestor stores, not the wrong one. Indices are 0-based,
        matching ``redaction.row_refs``.
        """
        na_tokens = set(NA_SENTINELS)

        def _is_empty(value: Any) -> bool:
            return pd.isna(value) or str(value) in na_tokens or str(value).strip() == ""

        def _series(frame):
            # A frame carrying duplicate columns of this name yields a DataFrame;
            # take the first (the ingestor rejects exact-duplicate headers, so
            # this only guards the reusable DataFrame path).
            col = frame[resolved_column]
            return col.iloc[:, 0] if isinstance(col, pd.DataFrame) else col

        sample: List[int] = []
        total = 0

        def _accumulate(series, offset: int) -> int:
            nonlocal total
            for i, value in enumerate(series):
                if _is_empty(value):
                    if len(sample) < _EMPTY_ROWS_SAMPLE_CAP:
                        sample.append(offset + i)
                    total += 1
            return offset + len(series)

        if isinstance(data, pd.DataFrame):
            if resolved_column not in data.columns:
                return sample, total
            _accumulate(_series(data), 0)
            return sample, total

        # No local try/except: a body-read failure (malformed encoding / quoting)
        # propagates to validate()'s handler, which rejects with the REAL parse
        # error. Swallowing it here would either fail-open (miss empties past the
        # failure) or reject with a misleading PARTIAL count that masks the actual
        # structural fault. The header read already succeeded, so reaching here
        # means the file is readable — a body error is a genuine data fault worth
        # surfacing, and the ingestor's own read (same dialect, on_bad_lines=
        # "error") would trip on it too.
        offset = 0
        reader = pd.read_csv(
            data,
            usecols=[resolved_column],
            keep_default_na=False,
            dtype=str,
            chunksize=50_000,
            **self._read_kwargs(),
        )
        for chunk in reader:
            offset = _accumulate(_series(chunk), offset)
        return sample, total
