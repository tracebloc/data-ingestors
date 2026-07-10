"""Per-record transform (structural refactor — backend#796, P5c).

Turns one raw source record (a CSV row / JSON object) into the cleaned,
DB-ready dict the ingestor writes: schema-filtered + NA-normalised columns,
the resolved ``data_id`` (from ``unique_id_column`` or a generated UUID), the
label (after the configured label policy), ``data_intent``, optional
annotation, and the framework columns (ingestor_id / filename / extension).

Extracted verbatim from ``BaseIngestor.process_record`` / ``_map_unique_id``;
``BaseIngestor`` composes it (the ``_record_processor`` property) and its
public ``process_record`` delegates here. The attribute names match the
ingestor's so the bodies are byte-for-byte unchanged.

The cleaned record holds the schema-declared DB columns + framework columns.
``semantic_segmentation``'s ``mask_id`` points at a per-row mask FILE: when the
template DECLARES it in the schema (``schema={"mask_id": "VARCHAR(255)"}``) it
is a real column and is KEPT here and stored — the training client reads it
from MySQL to locate masks (backend#816), so dropping it breaks semseg
training. When ``mask_id`` is NOT declared it is not a DB column;
``map_file_transfer`` lends it from the raw source record for the copy and
strips it (not stored). Either way ``_process_batch`` no longer needs the
former blanket ``mask_id`` pop (#212) — the cleaned record carries only the
columns the schema declares.
"""

import hashlib
import json
import logging
import uuid
from typing import Any, Dict, Optional

import pandas as pd

from ..utils import label_policy as label_policy_module
from ..utils.constants import Intent

logger = logging.getLogger(__name__)


class RecordProcessor:
    """Stateless-per-record transform configured with the run's column /
    label / intent settings. One instance per ingest (built by
    ``BaseIngestor._record_processor`` from the ingestor's config)."""

    def __init__(
        self,
        schema: Dict[str, Any],
        intent: Any,
        label_column: Optional[str],
        annotation_column: Optional[str],
        unique_id_column: Optional[str],
        label_policy: Any,
        ingestor_id: str,
        data_id_strategy: str = "uuid",
        table_salt: Optional[str] = None,
    ):
        self.schema = schema
        self.intent = intent
        self.label_column = label_column
        self.annotation_column = annotation_column
        self.unique_id_column = unique_id_column
        # #225: content-hash strategy needs the per-table salt; failing at
        # construction beats minting unsalted (correlatable) ids at row time.
        if data_id_strategy == "content_hash" and not unique_id_column and not table_salt:
            raise ValueError(
                "data_id_strategy='content_hash' requires a table_salt"
            )
        self.data_id_strategy = data_id_strategy
        self.table_salt = table_salt
        self.label_policy = label_policy
        self.ingestor_id = ingestor_id

    def _map_unique_id(
        self, record: Dict[str, Any], cleaned_record: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Maps the unique ID from the source record to data_id in the cleaned record.

        Args:
            record: Original record with all fields
            cleaned_record: Processed record with schema fields

        Returns:
            Updated cleaned record if valid, None if invalid unique ID
        """

        # validate intent is valid
        if not self.intent or self.intent not in Intent.get_all_intents():
            logger.warning(
                f"Invalid intent: {self.intent}. Must be one of: {Intent.get_all_intents()}"
            )
            return None

        # Validate label_column exists if specified
        columns_to_validate = [
            (self.label_column, "label_column"),
            (self.annotation_column, "annotation_column"),
        ]
        columns_not_found = False
        for column, column_name in columns_to_validate:
            if column and column not in record:
                logger.warning(
                    f"Specified {column_name} '{column}' not found in record"
                )
                columns_not_found = True

        if columns_not_found:
            logger.warning(
                f"Record is missing required column(s) {columns_not_found}; "
                f"present columns: {sorted(record.keys())}"
            )

        if self.label_column:
            # Apply the configured label policy at the latest possible moment
            # before the API client builds its payload. For classification-class
            # categories ``label_policy="passthrough"`` is a no-op; for
            # regression-class categories ``"bucket"`` replaces the raw target
            # with a stable hash-bucket ID so the value never leaks to the
            # central backend (#44 / parent client#85).
            #
            # Coerce numpy / pandas scalar types to native Python before the
            # policy runs. After the INT-cast switch to nullable ``Int64``,
            # itertuples yields ``numpy.int64`` (the old ``downcast='integer'``
            # incidentally produced plain ``int``) — and mysql-connector-python
            # refuses to bind numpy scalars, failing the passthrough path with
            # "Python type numpy.int64 cannot be converted" on every row of any
            # INT label column (tabular_classification on the e2e job). The
            # other policies (e.g. ``bucket``) stringify their output so they
            # never hit this; the fix lives here so passthrough also yields a
            # binder-friendly value.
            label_val = record.get(self.label_column)
            if hasattr(label_val, "item") and not isinstance(label_val, str):
                try:
                    label_val = label_val.item()
                except (ValueError, AttributeError):
                    pass
            # Strip surrounding whitespace from string label values before
            # the policy runs — protects against silent label-set
            # corruption (issue #261) where ``"  A  "`` and ``"A"`` would
            # otherwise land as distinct classes in MySQL. A user
            # copy-pasting from Excel / another tool routinely has
            # whitespace they can't see; the framework's contract for
            # the label column is "the class identifier", and class
            # identifiers don't carry whitespace semantics. The strip
            # mirrors what the framework already does for the
            # ``data_id`` column (line below) and for column headers
            # (``chunk.columns.str.strip()`` in csv_ingestor).
            #
            # Non-string labels (INT class IDs, BIOLabelValidator's
            # space-separated tags, etc.) pass through unchanged.
            if isinstance(label_val, str):
                label_val = label_val.strip()
            cleaned_record["label"] = label_policy_module.apply(
                label_val, self.label_policy
            )

        if self.intent:
            cleaned_record["data_intent"] = self.intent

        if self.annotation_column:
            cleaned_record["annotation"] = record.get(self.annotation_column)

        if not self.unique_id_column:
            if self.data_id_strategy == "content_hash":
                # #225: deterministic id — a retried k8s Job reproduces the
                # same ids, so the data_id UNIQUE upsert re-claims the prior
                # attempt's rows instead of duplicating them. The source
                # filename MUST participate: for file-bearing categories the
                # schema-filtered dict is just {label, data_intent, ...}, so
                # without it two different images with the same label would
                # collide and the upsert would collapse the dataset. It is
                # merged here (not read from cleaned_record) because process()
                # attaches filename only AFTER this method returns.
                cleaned_record["data_id"] = self._content_hash(
                    {**cleaned_record, "filename": record.get("filename")}
                )
            else:
                cleaned_record["data_id"] = str(uuid.uuid4())
            return cleaned_record

        unique_id = record.get(self.unique_id_column)
        if unique_id is not None and str(unique_id).strip():
            cleaned_record["data_id"] = str(unique_id).strip()
            return cleaned_record
        else:
            logger.warning(
                f"Missing or invalid value in unique-id column "
                f"{self.unique_id_column!r}; record skipped (content not "
                f"logged — #226)."
            )
            return None

    def _content_hash(self, cleaned_record: Dict[str, Any]) -> str:
        """
        Deterministic ``data_id`` from the record's cleaned content (#225).

        SHA-256 over the per-table salt + a canonical JSON serialization of
        every cleaned field (sorted keys, ASCII, compact separators,
        ``default=str`` so dates/decimals serialize stably post-cast). The
        record is hashed AFTER schema cleaning and label policy, so the
        digest reflects exactly what will be stored — identical source
        records produce identical ids across process restarts.

        Privacy: the salt (random per table, cluster-MySQL-only) makes the
        digest unlinkable across tables/clusters and defeats dictionary
        attacks on low-cardinality records; only 10 opaque ``{data_id,
        label}`` samples ever leave the cluster in the ingest summary.
        """
        canonical = json.dumps(
            cleaned_record,
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(
            (self.table_salt + canonical).encode("utf-8")
        ).hexdigest()

    def process(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single record"""
        try:
            # Clean data according to schema, excluding label_column, annotation_column, and unique_id_column
            # These are handled separately and should not be ingested as regular columns
            columns_to_exclude = set()
            if self.label_column:
                columns_to_exclude.add(self.label_column)
            if self.annotation_column:
                columns_to_exclude.add(self.annotation_column)
            if self.unique_id_column:
                columns_to_exclude.add(self.unique_id_column)
            # Preserve missing-data semantics: any null-like value becomes
            # Python None so the DB binder writes SQL NULL. Treats four
            # representations uniformly:
            #   - Python None         (explicit absence, JSON null)
            #   - float NaN / pd.NaT  (from pd.read_csv / pd.to_datetime)
            #   - pd.NA               (from pandas StringDtype after #172)
            #   - literal "" string   (JSON empty string — JSONIngestor reads
            #                         via json.load, not pd.read_json, so ""
            #                         survives to here; CSVs never hit this
            #                         case because keep_default_na=True turns
            #                         "" into NaN at read time)
            # Mirrors the missing-data convention in
            # JSONIngestor._validate_record (#170): `value is None or
            # value == ""`. pd.isna returns False for ordinary
            # strings/numbers/bools so existing values aren't touched.
            # Booleans must NOT be stringified — mysql-connector-python writes
            # True/False directly as TINYINT 1/0, but `str(True)` is the
            # four-character string "True", which MySQL rejects against a BOOL
            # column with `Incorrect integer value: 'True' for column 'active'
            # at row 1`. This must catch BOTH Python `bool` AND `numpy.bool_`:
            # a CSV BOOL column comes back from pandas/itertuples as numpy.bool_,
            # and `isinstance(np.True_, bool)` is False — so the previous
            # `isinstance(v, bool)` check missed it and every CSV boolean was
            # stringified to "True"/"False" and rejected by MySQL. `is_bool`
            # covers both; convert to a plain Python bool so the binder writes
            # 1/0. Checked FIRST so a bool never reaches the `v == ""` compare
            # (numpy scalar-vs-str comparison would warn) and pd.NA (is_bool
            # False) falls through to the null branch. The rest of the pipeline
            # expects strings, so everything non-bool/non-null is stringified.
            cleaned_record = {
                k.strip(): (
                    bool(v)
                    if pd.api.types.is_bool(v)
                    else None if pd.isna(v) or v == "" else str(v).strip()
                )
                for k, v in record.items()
                if k in self.schema and k not in columns_to_exclude
            }
            # Map unique ID if specified
            cleaned_record = self._map_unique_id(record, cleaned_record)


            if cleaned_record is None:
                return None

            # Add ingestor_id to the record
            cleaned_record["ingestor_id"] = self.ingestor_id
            cleaned_record["filename"] = record.get("filename")
            cleaned_record["extension"] = record.get("extension")
            # The cleaned record carries ONLY schema-declared DB columns +
            # framework columns. A sidecar's link column is a table column IFF
            # the schema declares it: semantic_segmentation MUST declare mask_id
            # (backend#816, enforced at preflight by MaskIdColumnValidator), so a
            # declared mask_id IS stored — kept by the `k in self.schema` filter
            # above — and the training client reads it to locate each mask. An
            # UNdeclared sidecar pointer is not a table column: it is lent to the
            # transfer by ``map_file_transfer`` for the copy only, without riding
            # the DB-bound record through process -> transfer -> batch (the
            # cross-layer leak this split removes — P5).
            return cleaned_record

        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            return None
