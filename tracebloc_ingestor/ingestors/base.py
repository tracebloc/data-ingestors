from abc import ABC, abstractmethod
from typing import Dict, Any, Generator, List, Optional, NamedTuple
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
import logging
from tqdm import tqdm
import uuid

from ..database import Database
from ..api.client import APIClient
from ..utils.constants import (
    RESET,
    BOLD,
    GREEN,
    RED,
    YELLOW,
    CYAN,
    DataFormat,
    TaskCategory,
    COLOR_MODE_CHANNELS,
    canonical_color_mode,
)
from ..utils import label_policy as label_policy_module
from ..utils import redaction
from ..utils.columns import resolve_column
from ..utils.correlation import resolve_correlation_id
from ..utils.validators_mapping import map_validators
from ..file_transfer import map_file_transfer, reclaim_source
from ..text_profile import compute_text_profile
from ..schema_inference import canonical_dtype
from ..reporting import ConsoleRenderer
from . import preflight
from .batch_writer import BatchWriter
from .record_processor import RecordProcessor
from .table_lock import TableLock

# Per-category behavior flags now live in the ModalityRegistry (the single
# source of truth — backend#796, P3a), derived from one ModalitySpec per
# category instead of three hand-maintained frozensets here. Imported under
# the previous names so the ``category in <set>`` checks below are unchanged
# (and their None/unknown -> False semantics are preserved).
from ..modalities.registry import (
    FILE_BEARING_CATEGORIES as _FILE_BEARING_CATEGORIES,
    NLP_CATEGORIES as _NLP_CATEGORIES,
    REGISTRY as _MODALITY_REGISTRY,
    TABULAR_FAMILY_CATEGORIES as _TABULAR_FAMILY_CATEGORIES,
)

# Logger for this module. Level is set by `setup_logging()` on the root
# logger when the user script calls it; child loggers inherit that level.
logger = logging.getLogger(__name__)

__all__ = ["BaseIngestor", "IngestionSummary"]

# Image bit depths the combine-time contract accepts (mirrors the check in
# cli.conventions.resolve). Values outside this set must not reach the emitted
# attributes even when bit_depth bypasses resolve() via spec.file_options.
_SUPPORTED_BIT_DEPTHS = (8, 16)

# The framework's standard prediction-target column. The user's declared
# label_column is mapped onto it (database.create_table creates a fixed
# ``label`` column), so this — not the original CSV name — is the target key in
# the physical-table schema emitted to the backend.
_TARGET_COLUMN = "label"

# Post-normalization value encodings the ingested (physical) table uses, stated
# per column in the enriched schema so combine-time alignment can confirm they
# agree (backend#1037's WARN checks) — di#360. The ingestor maps every
# recognized NA token to SQL NULL and stores booleans as MySQL 1/0, so these are
# uniform across every dataset it produces; a dataset ingested under a different
# convention would surface as a mismatch.
_NULL_ENCODING = "null"
_BOOL_ENCODING = "1/0"

# Text alignment facts (encoding/language/normalization) apply to the text
# categories only — the NLP modalities minus embeddings, whose per-category fact
# is the embedding-specific ``positive_definition`` instead. Mirrors the
# backend contract's ``_TEXT`` group; the contract rejects a text fact declared
# on the embeddings category, so scoping here keeps ingest from being rejected.
_TEXT_CATEGORIES = _NLP_CATEGORIES - {TaskCategory.EMBEDDINGS}

# Survival (time-to-event) duration units the combine-time contract accepts.
_TIME_UNITS = ("days", "weeks", "months", "years")

# file_options keys that are ingestor-internal bridges, never wire payload.
# ``schema`` is a validator artifact (the canonical schema ships as the
# top-level ``schema`` arg). The rest are alignment facts bridged from config
# resolution onto file_options ONLY so the emit hooks can place them on their
# canonical channels: ``column_descriptors`` (unit/ordinal) merges onto the
# enriched schema's columns (``_schema_payload``); the scalar facts are
# copied under ``attributes`` (``_scalar_attribute_metadata``) — the only
# place the backend reads them (dataset_validators reads
# ``meta_data.attributes``). Shipping the raw keys beside the canonical
# copies duplicated them at meta_data top level (bugbot on #383, both
# rounds).
_META_DATA_INTERNAL_KEYS = frozenset(
    {
        "schema",
        "column_descriptors",
        "color_mode",
        "bit_depth",
        "language",
        "normalization",
        "time_unit",
        "event_indicator",
        "positive_definition",
    }
)


def _valid_event_indicator(v: Any) -> bool:
    """A survival ``event_indicator`` is ``{event: int, censored: int}`` — the
    contract's shape (backend#1037). bool is rejected (it's an int subclass)."""
    return (
        isinstance(v, dict)
        and not isinstance(v.get("event"), bool)
        and not isinstance(v.get("censored"), bool)
        and isinstance(v.get("event"), int)
        and isinstance(v.get("censored"), int)
    )


def _rows_state_clause(inserted_records: int) -> str:
    """Phrase the parenthetical about what's already in the database for a
    registration-failure message, truthfully for the row count actually
    ingested.

    The registration steps run AFTER the rows are committed, so when some rows
    landed the message must warn they're already persisted. But when zero rows
    were ingested (e.g. a header-only CSV or an all-files-missing manifest that
    slipped to this point), the old fixed "(its rows are already in the
    database)" wording was simply false and sent users hunting for rows that
    don't exist. Switch on the count so the message can never claim phantom rows.
    """
    if inserted_records > 0:
        return f"its {inserted_records} already-ingested row(s) remain in the database"
    return "no rows were ingested, so nothing was left in the database"


def _validator_label(name: str) -> str:
    """Human-readable label for a validator's preflight log/error lines.

    Validator ``.name`` values are inconsistent: some already end in
    "Validator" (e.g. "Data Validator") while many do not (e.g. "Ingestable
    Records", "Text Content", "BIO Label", "Keypoint Annotation"). Normalize at
    the log site so every label reads "<Name> Validator" exactly once —
    appending the word only when it's missing, so suffixed names don't double
    ("Data Validator Validator") and unsuffixed names don't drop it.
    """
    name = (name or "").strip()
    return name if name.lower().endswith("validator") else f"{name} Validator"


# NOTE: _TABULAR_FAMILY_CATEGORIES and _FILE_BEARING_CATEGORIES are imported
# above from modalities.registry (derived from the per-category ModalitySpec
# flags — backend#796 P3a). The ``self.category in <set>`` checks throughout
# this module use them unchanged; the rationale for each set now lives on
# ModalitySpec's field docstrings.


class IngestionSummary(NamedTuple):
    """Data class to hold ingestion summary statistics.

    Attributes:
        total_records: Total number of records processed
        processed_records: Number of records successfully processed
        inserted_records: Number of records inserted into database
        api_sent_records: Number of records sent to API
        failed_records: Number of records that failed processing
        skipped_records: Number of records that were skipped for non-file
            reasons (e.g. missing label / invalid intent / processing error)
        file_transfer_failures: Number of records whose source file (image,
            annotation, mask, text) was missing or unreadable, so the
            record was dropped before the DB / API write. Tracked
            separately from ``skipped_records`` so operators can
            distinguish data-loss from validation skips (issue #99).
    """

    ingestor_id: str
    total_records: int
    processed_records: int
    inserted_records: int
    api_sent_records: int
    failed_records: int
    skipped_records: int
    file_transfer_failures: int = 0

    @property
    def has_failures(self) -> bool:
        """True if any non-trivial failure occurred — DB insert short of
        total, API short of inserted, file-transfer skipped any record,
        a record was dropped during processing (skipped_records), or
        processing errored. Used to gate the "completed successfully"
        banner so customers can't mistake a partial run for a clean one.
        """
        return (
            self.failed_records > 0
            or self.file_transfer_failures > 0
            or self.skipped_records > 0
            or self.inserted_records < self.total_records
            or self.api_sent_records < self.inserted_records
        )


class BaseIngestor(ABC):
    """Base class for all data ingestors.

    This abstract base class provides the core functionality for ingesting data from various sources
    into a database and optionally sending it to an API. It handles batching, retries, and progress tracking.

    Attributes:
        ingestor_id: Unique identifier for this ingestor instance
        correlation_id: End-to-end run id from the TRACEBLOC_INGEST_CORRELATION_ID
            env var (the CLI's idempotency key, stamped by jobs-manager), or
            None outside jobs-manager-spawned Jobs (backend#1028 item 3)
        database: Database instance for data storage
        engine: SQLAlchemy engine instance
        api_client: API client for sending data
        table_name: Name of the target database table
        schema: Database schema definition
        unique_id_column: Column name for unique identifiers
        label_column: Column name for labels
        intent: Data intent (training/testing)
        annotation_column: Column name for annotations
        category: Data category
    """

    def __init__(
        self,
        database: Database,
        api_client: APIClient,
        table_name: str,
        schema: Dict[str, str] = {},
        unique_id_column: Optional[str] = None,
        label_column: Optional[str] = None,
        intent: Optional[str] = None,
        annotation_column: Optional[str] = None,
        category: Optional[str] = None,
        data_format: Optional[str] = None,
        file_options: Optional[Dict[str, Any]] = None,
        label_policy: str = label_policy_module.PASSTHROUGH,
        data_id_strategy: str = "content_hash",
    ):
        """Initialize the base ingestor.

        Args:
            database: Database instance for data storage
            api_client: API client instance for data transmission
            table_name: Name of the target table
            schema: Database schema definition
            unique_id_column: Name of the column to use as unique identifier
            label_column: Name of the column to use as label
            intent: Is the data for training or testing
            annotation_column: Name of the column to use as annotation
            category: Category of the data
            data_format: Format of the data
            file_options: File options to run before ingestion
            label_policy: ``"passthrough"`` (default; classification — the
                label value crosses the cluster boundary unchanged) or
                ``"bucket"`` (regression-class — each label is replaced
                with a stable hash-bucket ID before the API payload is
                built, so raw target values never leak). Schema-validated
                upstream by the YAML entrypoint; templates pass the
                appropriate constant from :mod:`tracebloc_ingestor.utils.label_policy`.
        Raises:
            ValueError: If unique_id_column is not provided
        """
        self.ingestor_id = str(uuid.uuid4())
        # #225: deterministic content-hash data_id. Now the default (#350):
        # landed dark in v0.5.7, soaked opt-in through v0.6.0/v0.7.x, flipped
        # here so a retried k8s Job re-claims its rows via the data_id UNIQUE
        # upsert instead of duplicating them. Pass ``data_id_strategy="uuid"``
        # to opt back into fresh-per-record UUIDs.
        # unique_id_column wins if both are set (schema can't express both;
        # belt-and-suspenders). The salt fetch is DEFERRED to ingest() —
        # mirroring #260's deferred create_table — so a validator-rejected
        # run leaves no salt row behind. get_or_create is atomic
        # (INSERT IGNORE), so it needs no table lock even then.
        self.data_id_strategy = data_id_strategy
        if (
            category == TaskCategory.OBJECT_DETECTION
            and data_id_strategy == "content_hash"
            and not unique_id_column
        ):
            # Objdet manifests list one row PER OBJECT: duplicate
            # (filename, label) rows are distinct objects, but they produce
            # identical content digests, so the data_id UNIQUE upsert keeps
            # only one of them (bugbot High on #383). The YAML resolver
            # defaults objdet to uuid; this guards the direct-constructor
            # path, which can't distinguish an explicit choice from the
            # signature default — hence a warning, not an override.
            logger.warning(
                "object_detection with data_id_strategy='content_hash': "
                "duplicate (filename, label) manifest rows collapse into one "
                "stored row. If each row is one object (the usual manifest "
                "shape), pass data_id_strategy='uuid'."
            )
        self._table_salt: Optional[str] = None
        self.database = database
        self.engine: Engine = database.engine
        self.api_client = api_client
        self.table_name = table_name
        # RFC-0003 D16/D19 (tracebloc/backend#1205): under per-ingestion
        # storage every run writes its own immutable table — named
        # ds_<uuid4().hex> (35 chars), a pure function of ingestor_id.
        # HEX, not the hyphenated uuid (D3 amendment on backend#1204): the
        # trainer's strict table grammar (_SQL_IDENTIFIER_RE) and this
        # package's own TableNameValidator both forbid hyphens in TABLE
        # names, and loosening two security validators would be worse than
        # deriving a hyphen-free name. ingestor_id itself keeps its
        # hyphenated format everywhere. ``table_name`` stays the user-facing
        # dataset label (summary URL segment, staging dirs, table lock,
        # default title). The row store — create/insert/count/schema/journal/
        # compensating-delete — sees ONLY ``physical_table_name``. Flag off
        # => both names are the label and behavior is byte-for-byte today's.
        # The ``is True`` comparison is deliberate: test doubles use bare
        # MagicMocks for config, and a truthy Mock must not silently flip
        # the storage model.
        self.per_ingestion_tables = database.config.PER_INGESTION_TABLES is True
        self.physical_table_name = (
            f"ds_{uuid.UUID(self.ingestor_id).hex}"
            if self.per_ingestion_tables
            else table_name
        )
        # Under PER_INGESTION_TABLES the file tree keys on the SAME physical
        # ds_<hex> handle as the row store, not the shared label tree: point
        # DEST_PATH — which every file-copy primitive, the text profiler, the
        # duplicate validator, and the source-reclaim path all read off this
        # run's config — at the handle, so file-bearing assets land in
        # STORAGE_PATH/ds_<hex>/. That matches the per-dataset scoped mount
        # (client-runtime#203) and the engine's get_dataset_path resolution
        # (tracebloc-engine#569), closing the #203-phase-2 file half. It lifts
        # the earlier refusal: a same-label re-ingest now writes to its OWN
        # handle instead of overwriting an earlier dataset's files. Flag off =>
        # set_dest_table is never called and DEST_PATH stays
        # STORAGE_PATH/<label>, byte-for-byte today's behavior.
        if self.per_ingestion_tables:
            self.database.config.set_dest_table(self.physical_table_name)
        self.schema = schema
        self.unique_id_column = unique_id_column
        self.label_column = label_column
        self.intent = intent
        self.annotation_column = annotation_column
        self.category = category
        self.data_format = data_format
        self.file_options = file_options or {}
        self.label_policy = label_policy

        # backend#1028 item 3: end-to-end correlation id. When spawned by
        # jobs-manager, the Job env carries the CLI's idempotency key — the
        # same string the Job name is derived from and the
        # tracebloc.io/ingestion-run label holds. Kept ALONGSIDE the
        # per-process ingestor_id (row scoping — label counts, the #227
        # compensating delete — must stay per-process across Job retries);
        # riding file_options puts it in the registration payload's
        # meta_data, so the backend dataset row carries it too. None when
        # the env is absent/invalid — behaviour is then exactly as before.
        self.correlation_id = resolve_correlation_id()
        if self.correlation_id:
            self.file_options["correlation_id"] = self.correlation_id
            logger.info(
                "Correlation id %s (ingestor_id %s)",
                self.correlation_id,
                self.ingestor_id,
            )

        # Default behavior is UUID-generated data_id (no source column leaves
        # the cluster). Opting into source-column mapping is allowed but loud:
        # warn at startup naming the column whose values will be sent to the
        # central backend, so reviewers can audit the privacy implication.
        if self.unique_id_column:
            logger.warning(
                f"{YELLOW}Source-column data_id mapping enabled: values from "
                f"column '{self.unique_id_column}' will be sent to the central "
                f"backend as 'data_id'. To prevent source PII leakage (e.g. "
                f"patient_id, user_id), omit unique_id_column to use "
                f"server-side UUIDs instead.{RESET}"
            )

        # Remove label_column, annotation_column, and unique_id_column from schema
        # These are handled separately and should not be ingested as regular columns.
        # Resolve each against the schema keys case-/whitespace-insensitively (the
        # #340 rule) rather than by exact key: label_column isn't pinned to the real
        # header until _resolve_label_column runs mid-ingest, so a manifest
        # ``label.column: Price`` (or ``" price "``) against a header ``price`` would
        # otherwise miss here and leave the target's SOURCE column in the physical
        # table. It would then reflect back into the enriched schema, and its
        # uploader unit/ordinal descriptor would attach to that feature-named column
        # instead of the framework ``label`` column that carries role:"target" — so
        # the backend's combine-time target descriptor checks would miss the declared
        # unit/ordinal (bugbot medium). The same exact-vs-resolved gap applied to the
        # annotation / unique_id columns.
        table_schema = schema.copy()
        for _special in (
            self.label_column,
            self.annotation_column,
            self.unique_id_column,
        ):
            if not _special:
                continue
            _physical = resolve_column(table_schema.keys(), _special)
            if _physical:
                del table_schema[_physical]

        # Canonical feature-column ordering (#763, mode A). Tabular-family
        # trainers read features positionally and the averaging service sums
        # weights by tensor position, so every edge must agree on which feature
        # lives at which position. Left alone, the order is whatever each edge's
        # template ``schema`` dict happened to declare, so two sites can
        # silently average misaligned features (pos 0 = "age" at one site,
        # "bmi" at another) with no error. Sorting the cleaned feature columns
        # into one deterministic order *here* -- the ingest layer, the single
        # point that defines the stored table schema every edge's trainer later
        # reads back via its schema-ordered SELECT -- makes the cross-site
        # contract hold by construction, with no runtime feature_columns
        # broadcast/reindex needed. Only tabular-family categories are
        # reordered; image/keypoint schemas (e.g. a "Visibility" column) keep
        # their declared order.
        if self.category in _TABULAR_FAMILY_CATEGORIES:
            table_schema = {key: table_schema[key] for key in sorted(table_schema)}

        # Add cleaned schema to file_options for validators / downstream metadata.
        # Always overwrite so a schema passed in by the template (which may still
        # contain the label/annotation/unique_id columns) is sanitized before
        # being sent to the backend as part of meta_data.
        if schema:
            self.file_options["schema"] = table_schema
            # number_of_columns is only meaningful for tabular-family
            # categories — that's where the validator + backend metadata
            # consume it. Image categories may also carry a schema (e.g.
            # keypoint_detection's "Visibility" column) but the count
            # would be misleading there, so don't inject it.
            if self.category in _TABULAR_FAMILY_CATEGORIES:
                self.file_options["number_of_columns"] = len(table_schema)

        # Defer table creation until after ``validate_data()`` passes so a
        # validator-rejected ingest leaves no orphaned table behind (#260).
        # Creating it in ``__init__`` meant a rejected ingest left a stale
        # empty table that the next retry's stale-table guard tripped on,
        # forcing the user to manually DROP before re-running. Stash the
        # cleaned schema; ``_ingest_with_lock`` creates the table once
        # validation succeeds.
        self.table = None
        self._table_schema = table_schema

    @property
    def _record_processor(self) -> RecordProcessor:
        """The ingestor's per-record transform collaborator (P5c). Built from
        the run's column / label / intent config; a fresh instance per access
        is fine — it just holds those refs. ``process_record`` delegates here."""
        return RecordProcessor(
            schema=self.schema,
            intent=self.intent,
            label_column=self.label_column,
            annotation_column=self.annotation_column,
            unique_id_column=self.unique_id_column,
            label_policy=self.label_policy,
            ingestor_id=self.ingestor_id,
            data_id_strategy=self.data_id_strategy,
            table_salt=self._table_salt,
        )

    def process_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single record into the cleaned, DB-ready dict.

        Delegates to the :class:`RecordProcessor` collaborator (structural
        refactor P5c); kept as the ingestor's method since the ingest loop and
        tests call ``self.process_record`` / ``ing.process_record``.
        """
        return self._record_processor.process(record)

    def _resolve_label_column(self, columns: Any) -> bool:
        """Pin the configured label column to the actual header spelling (#340).

        The label column is validated case-/whitespace-insensitively
        (``LabelColumnValidator`` via the shared ``resolve_column`` rule), but
        the read path pulls it out of each record by exact key — so a manifest
        ``label: label`` against a header ``Label`` passes preflight and then
        reads ``None`` for every row (silent all-NULL labels). Resolving the
        configured name to the real header once, then reading with it, keeps
        the read path and the validators in agreement.

        Uses the SAME rule as the validators (single source of truth), so a
        column that passed the label-presence check resolves identically here.

        Returns ``True`` once resolution is settled — the name matched (exactly
        or after remap) or no column is configured — and ``False`` when the
        configured name is not present in ``columns`` yet. The caller pins on
        the first record that *contains* the label: for CSV that's always the
        first record (a stable full header), and for JSON — whose records may
        be sparse (a leading object can omit the label) — it retries on later
        records instead of giving up after the first. A name genuinely absent
        from every record is left untouched so the existing missing-column
        handling still fires.
        """
        if not self.label_column:
            return True
        resolved = resolve_column(columns, self.label_column)
        if resolved is None:
            return False
        if resolved != self.label_column:
            logger.info(
                f"Resolved label column {self.label_column!r} to header "
                f"{resolved!r} (case/whitespace-insensitive match)."
            )
            self.label_column = resolved
        return True

    @property
    def _grouping(self):
        """The category's sequence-grouping trait (``ModalitySpec.grouping``,
        backend#1054 Decision-4), or ``None`` for per-row categories /
        unknown categories. Read trait-style from the registry — never via a
        category string comparison — so a future grouped category is a
        registry entry, not a base.py edit. Gates the sequence-unit label
        counts, the composite ``(group, time)`` index, and the post-insert
        group-integrity pass."""
        spec = _MODALITY_REGISTRY.get(self.category)
        return spec.grouping if spec is not None else None

    @property
    def _table_lock(self) -> TableLock:
        """The run's table lock (P5b). ``TableLock`` owns the file-lock
        lifecycle — compute path, atomic acquire with stale-reclaim, release —
        and the ingestor just composes it. A fresh instance per access is fine:
        the lock state lives in the on-disk file, not the object (``acquire``
        returns the path, ``release`` takes it back)."""
        return TableLock(self.table_name, self.ingestor_id)

    def validate_data(self, source: Any) -> bool:
        """Validate data before ingestion using configured validators.

        Args:
            source: The data source to validate

        Returns:
            True if all validations pass, False otherwise

        Raises:
            ValueError: If validation fails
        """
        # Pre-flight: SRC_PATH must be a real absolute directory or every
        # file_transfer falls through and surfaces as N copies of
        # "Source image not found: images/x.jpg" — blames the data when
        # the real cause is "SRC_PATH was never staged / set" (#772 P2).
        # File-bearing categories only (tabular has nothing under SRC_PATH).
        if self.category in _FILE_BEARING_CATEGORIES:
            preflight.check_src_path(self.database.config)

        # Pre-flight: a non-UTF-8 CSV otherwise surfaces as a misleading
        # "No data found" (validators read UTF-8 and swallow decode errors).
        # Catch it once here with a clear, actionable message.
        preflight.check_csv_encoding(source)

        # Pass the configured label_column through (without permanently
        # mutating file_options / metadata) so label-aware validators like
        # BIOLabelValidator check the right column when a custom name is used.
        validators = map_validators(
            self.category,
            {
                **self.file_options,
                "label_column": self.label_column,
                # file_options["schema"] has the label/annotation/id columns
                # stripped (they're framework columns, not table columns), but
                # CSVIngestor reads the file with NA/dtype rules from the FULL
                # schema — so the label column DOES get NA-sentinel treatment at
                # ingest. Pass the full schema so LabelDiversityValidator counts
                # distinct labels the same way the data is actually ingested,
                # rather than letting "null"/"NA" inflate the distinct count and
                # sneak an effectively single-class dataset past the gate
                # (bugbot #252).
                "full_schema": self.schema,
                # The run's csv read options (delimiter / encoding / ...), so a
                # CSV-reading validator parses the manifest BYTE-IDENTICALLY to
                # CSVIngestor. Without it a non-comma or BOM manifest that ingests
                # fine is falsely rejected at preflight (delimiter is a supported
                # option — schema/ingest.v1.json). getattr: only CSVIngestor
                # carries csv_options; JSON/other ingestors default to {}.
                "csv_options": getattr(self, "csv_options", {}),
                # The run's data_id source column (data_id.strategy=column),
                # for SequenceGroupValidator's T6 guard: mapping data_id
                # from the sequence column would upsert-collapse every
                # sequence to one row (backend#1054 WS1). None for the
                # default UUID / content_hash strategies; non-grouped
                # factories ignore the key.
                "unique_id_column": self.unique_id_column,
            },
            # Inject the run's resolved Config so path-reading validators
            # (SRC_PATH / DEST_PATH / TABLE_NAME) use it instead of a
            # module-global Config() that reads os.environ (P4b).
            self.database.config,
        )
        logger.info(f"Running {len(validators)} validator(s) on data source")
        all_valid = True
        validation_errors = []

        for validator in validators:
            label = _validator_label(validator.name)
            try:
                logger.info(f"{CYAN}Running validator: {label}{RESET}")
                result = validator.validate(source)

                if not result.is_valid:
                    all_valid = False
                    validation_errors.append(f"{BOLD}{label} failed: {RESET} \n {RED}")
                    validation_errors.extend(result.errors)
                    validation_errors.append(f"{RESET}")

                # Log warnings if any
                for warning in result.warnings:
                    logger.warning(
                        f"{YELLOW}Validation warning - {label}: {warning}{RESET}"
                    )
                if result.is_valid:
                    # Normalize the label so it always reads "<Name> Validator"
                    # exactly once: names that already end in "Validator" (e.g.
                    # "Data Validator") don't double, and names that don't (e.g.
                    # "Ingestable Records") keep the suffix.
                    print(f"{GREEN}{label} successfully passed{RESET}")
            except Exception as e:
                all_valid = False
                validation_errors.append(f"{label} error: {str(e)}")

        if not all_valid:
            error_summary = "\n".join(validation_errors)
            raise ValueError(f"{RED}{error_summary}{RESET}")

        print(f"{GREEN}All validations passed successfully{RESET}")
        return True

    @abstractmethod
    def read_data(self, source: Any) -> Generator[Dict[str, Any], None, None]:
        """Read data from the input source"""
        pass

    def _collect_run_metadata(self) -> Dict[str, Any]:
        """Data-derived metadata to merge onto the global-metadata channel after
        a successful ingest, computed from what the run already scanned.

        The base emits nothing; subclasses override to contribute (e.g.
        ``CSVIngestor`` returns ``{"attributes": {"feature_stats": …}}``, #360).
        Kept as a hook so the base engine stays format-agnostic rather than
        branching on category the way the ``text_profile`` injection does.

        The returned dict is applied by ``_apply_run_metadata``: its
        ``attributes`` key (a shared per-dataset namespace — feature_stats today,
        text/image facts in later #360 slices) is merged *into* any existing
        ``attributes`` rather than replacing it; other keys ``.update`` normally.
        """
        return self._scalar_attribute_metadata()

    def _scalar_attribute_metadata(self) -> Dict[str, Any]:
        """Data-format-derived scalar attributes for combine-time alignment
        (di#360). Format-agnostic so any ingestor (CSV or JSON manifest)
        contributes them — image/text datasets are manifest-based and inherit
        this base hook. Subclasses that override ``_collect_run_metadata`` (e.g.
        ``CSVIngestor``) must fold this in via ``super()``.

        - **Image:** ``resolution`` from the run's uniform ``target_size`` (the
          resolution validator enforces every image to it, so no image read is
          needed here), emitted as ``[height, width]`` (``target_size`` is
          ``[width, height]``); ``color_mode`` + derived ``channels`` and
          ``bit_depth`` from ``file_options`` when the user supplies them for the
          vision run (RGB/grayscale only — the values the contract accepts). Not
          auto-detected: PIL modes like RGBA/CMYK aren't in the contract enum.
        - **Text:** ``encoding`` is ``"utf-8"`` (text is staged/validated as
          UTF-8, the canonical value); ``language`` and ``normalization`` are
          uploader-declared when present. Text categories only (not embeddings,
          which the contract scopes to its own ``positive_definition``).
        - **Survival (time-to-event):** uploader-declared ``time_unit`` and
          ``event_indicator`` (``{event, censored}``), re-checked against the
          contract's accepted shapes before emission.
        - **Embeddings:** uploader-declared ``positive_definition`` (what counts
          as a positive pair) — the embeddings-specific alignment fact.
        """
        attributes: Dict[str, Any] = {}

        if self.data_format == DataFormat.IMAGE:
            target = self.file_options.get("target_size")
            if isinstance(target, (list, tuple)) and len(target) == 2:
                width, height = int(target[0]), int(target[1])
                attributes["resolution"] = [height, width]

            # color_mode is user-provided in file_options for vision use cases;
            # emit only the canonical RGB/grayscale values and derive channels.
            color_mode = canonical_color_mode(self.file_options.get("color_mode"))
            if color_mode:
                attributes["color_mode"] = color_mode
                attributes["channels"] = COLOR_MODE_CHANNELS[color_mode]

            # Only the contract-accepted depths (8/16) — mirrors color_mode's
            # canonicalisation. conventions.resolve() rejects other values, but a
            # bit_depth set directly in spec.file_options or a modality spec
            # bypasses that gate, so re-check here before it reaches the contract.
            bit_depth = self.file_options.get("bit_depth")
            if (
                isinstance(bit_depth, int)
                and not isinstance(bit_depth, bool)
                and bit_depth in _SUPPORTED_BIT_DEPTHS
            ):
                attributes["bit_depth"] = bit_depth

        if self.category in _TEXT_CATEGORIES:
            # encoding is the canonical UTF-8 (text is staged/validated as UTF-8).
            attributes["encoding"] = "utf-8"
            # language + text normalization are uploader-declared alignment facts
            # (bridged into file_options by conventions.resolve); they drive the
            # edge's cross-client text handling and the backend's BLOCK check on
            # a language mismatch, so emit them when declared.
            language = self.file_options.get("language")
            if isinstance(language, str) and language.strip():
                attributes["language"] = language.strip()
            normalization = self.file_options.get("normalization")
            if isinstance(normalization, str) and normalization.strip():
                attributes["normalization"] = normalization.strip()

        if self.category == TaskCategory.TIME_TO_EVENT_PREDICTION:
            # Survival alignment facts, uploader-declared (di#360): the duration
            # unit and the event/censoring encoding. Both are BLOCK-guarded on
            # mismatch by the backend, so a merge silently flipping event vs
            # censored — or comparing days against months — is caught. Re-checked
            # against the contract's shape here (a value set directly in
            # spec.file_options bypasses conventions.resolve's validation).
            time_unit = self.file_options.get("time_unit")
            if time_unit in _TIME_UNITS:
                attributes["time_unit"] = time_unit
            event_indicator = self.file_options.get("event_indicator")
            if _valid_event_indicator(event_indicator):
                attributes["event_indicator"] = {
                    "event": int(event_indicator["event"]),
                    "censored": int(event_indicator["censored"]),
                }

        if self.category == TaskCategory.EMBEDDINGS:
            # The embeddings-specific alignment fact (uploader-declared): what
            # defines a positive pair. Its own contract field, distinct from the
            # text facts above (which the contract does not allow on embeddings).
            positive_definition = self.file_options.get("positive_definition")
            if isinstance(positive_definition, str) and positive_definition.strip():
                attributes["positive_definition"] = positive_definition.strip()

        return {"attributes": attributes} if attributes else {}

    def _apply_run_metadata(self) -> None:
        """Merge ``_collect_run_metadata()`` into ``file_options`` just before the
        payload ships. Split out from ``_ingest`` so the merge — in particular the
        shallow-merge of the shared ``attributes`` namespace — is unit-testable
        without a full ingest run."""
        run_meta = self._collect_run_metadata()
        attributes = run_meta.pop("attributes", None)
        if attributes:
            self.file_options.setdefault("attributes", {}).update(attributes)
        self.file_options.update(run_meta)

    def _schema_payload(self, schema_dict: Dict[str, str]) -> Dict[str, Any]:
        """The ``schema`` value shipped on the global-metadata channel.

        Default — the legacy flat ``{col: SQL_type}`` map, unchanged, which the
        current backend consumes.

        Enriched (``EMIT_ENRICHED_SCHEMA`` — data-ingestors#360 slice 1b, gated
        for the backend#1037 cutover) — ``{col: {"dtype": <canonical>}}`` with the
        framework ``label`` column carrying ``role: "target"`` for supervised
        tasks. That lets combine-time alignment identify the prediction target
        from the contract (backend#1037's ``role``-based check) rather than
        inferring it. ``dtype`` is the CANONICAL logical type
        (``schema_inference.canonical_dtype``), not the raw storage type, so a
        merged dataset compares column types by logical family — ``VARCHAR(255)``
        vs ``VARCHAR(100)`` (or ``INT`` vs ``BIGINT``) no longer read as a
        divergence. Physical-table shape otherwise: keys are exactly what
        ``get_table_schema`` reflected, so ``label`` is the target key — the same
        key ``feature_stats`` re-keys the regression-class target under, so schema
        ``role: "target"`` and the target's stats line up on one column name.
        """
        if not self.database.config.EMIT_ENRICHED_SCHEMA:
            return schema_dict
        enriched: Dict[str, Any] = {
            col: {"dtype": canonical_dtype(sql_type)}
            for col, sql_type in schema_dict.items()
        }
        # Value encodings of the ingested data: missing is SQL NULL for every
        # column; booleans are stored MySQL 1/0. Uniform by construction, stated
        # per column so #1037 can verify cross-dataset consistency.
        for desc in enriched.values():
            desc["null_encoding"] = _NULL_ENCODING
            if desc["dtype"] == "bool":
                desc["bool_encoding"] = _BOOL_ENCODING
        # ``label`` is the standard column the user's label_column is mapped onto
        # (database.create_table). It's always present, but is only a prediction
        # target for SUPERVISED tasks — self-supervised runs (no label_column)
        # must not claim one.
        if self.label_column and _TARGET_COLUMN in enriched:
            enriched[_TARGET_COLUMN]["role"] = "target"

        # Merge uploader-declared per-column descriptors that CAN'T be inferred
        # from the data — ``unit`` and ``ordinal`` (di#360). These activate the
        # backend's combine-time descriptor checks (e.g. a ``unit`` mismatch —
        # merging a USD column with an EUR one — is otherwise silent). Declared
        # by CSV column name; entries for a column absent from the schema (or the
        # target, which the physical schema keys as ``label``) are ignored.
        declared = self.file_options.get("column_descriptors") or {}
        for col, desc in declared.items():
            if not isinstance(desc, dict):
                continue
            # Resolve the declared name to a physical column FIRST (the #340
            # resolve_column rule, case-/whitespace-insensitive). A descriptor for
            # the target is keyed by its CSV source name (e.g. "demand_mw"), which
            # isn't a physical column (the schema keys the target as ``label``), so
            # only THEN map it to ``label``. Resolving physically first means a
            # real feature that merely case-matches the label's source name is
            # routed to itself, not misrouted onto the target.
            target = resolve_column(enriched.keys(), col)
            if (
                not target
                and self.label_column
                and resolve_column([col], self.label_column)
            ):
                target = _TARGET_COLUMN if _TARGET_COLUMN in enriched else None
            if not target:
                continue
            if desc.get("unit") is not None:
                enriched[target]["unit"] = desc["unit"]
            if desc.get("ordinal") is not None:
                enriched[target]["ordinal"] = desc["ordinal"]
        return enriched

    def _meta_data_payload(self) -> Dict[str, Any]:
        """The ``meta_data`` value shipped on the global-metadata channel:
        ``file_options`` minus the ingestor-internal bridges
        (``_META_DATA_INTERNAL_KEYS``), so a single, unambiguous copy of the
        schema and the per-column descriptors is on the wire (G8, #360).
        Split out from ``_ingest`` so the filter is unit-testable without a
        full ingest run."""
        return {
            k: v
            for k, v in self.file_options.items()
            if k not in _META_DATA_INTERNAL_KEYS
        }

    def _count_records(self, source: Any) -> Optional[int]:
        """
        Try to count total records in the source for progress tracking.
        Subclasses should override this if they can provide a more efficient count.

        Args:
            source: The data source

        Returns:
            Total number of records if countable, None otherwise
        """
        try:
            # Default implementation tries to count by iterating
            return sum(1 for _ in self.read_data(source))
        except Exception as e:
            logger.debug(f"Unable to count records: {str(e)}")
            return None

    def ingest(self, source: Any, batch_size: int = 50) -> List[Dict[str, Any]]:
        """
        Ingest data from the source with progress tracking

        Args:
            source: The input data source
            batch_size: Number of records to process in each batch

        Returns:
            List of failed records
        """
        # Concurrent-ingest guard (backend/#772 P2). Two ingests targeting
        # the same `table_name` used to race ``create_table`` and
        # interleave upserts; the second submission would see a
        # partially-populated table and fail mid-run, with the original
        # ingestor unaware. Acquire an exclusive file-lock keyed by the
        # table name; on conflict, fail fast naming the holder. The lock
        # is released in the finally below — that wraps the ENTIRE
        # post-acquire body so every exit path (#221 bugbot) releases,
        # including ones the inner ``except Exception`` doesn't catch
        # (Session() construction failure, _count_records exceptions,
        # KeyboardInterrupt, etc.).
        # Fail fast on a config error (bad intent) before acquiring a table
        # lock or touching the DB — it would otherwise skip every row (#234).
        preflight.check_intent(self.intent)
        lock = self._table_lock
        _lock_path = lock.acquire()
        try:
            return self._ingest_with_lock(source, batch_size)
        finally:
            lock.release(_lock_path)

    def _ingest_with_lock(
        self, source: Any, batch_size: int = 50
    ) -> List[Dict[str, Any]]:
        """Inner ingest body invoked once the table lock is held. Split
        out from ``ingest`` so the lock-release lives in a finally that
        covers every exit path (#221 bugbot — HIGH)."""
        # Validate data before ingestion
        logger.info(f"{CYAN}Starting data validation before ingestion...{RESET}")
        try:
            self.validate_data(f"{source}")
            logger.info(f"{GREEN}Data validation completed successfully{RESET}")
        except ValueError as e:
            raise e
        except Exception as e:
            raise e

        # Create the destination table now that validation has accepted the
        # input (#260). Deferring to here ensures a validator-rejected ingest
        # leaves no orphaned empty table behind that the next retry's
        # stale-table guard would trip on.
        # #225: fetch the per-table salt only now — the run survived
        # validation, so it will actually write rows (mirrors the deferred
        # create_table below, #260); a rejected run leaves no salt row.
        if (
            self.data_id_strategy == "content_hash"
            and not self.unique_id_column
            and self._table_salt is None
        ):
            # Per-ingestion mode mints a fresh salt row per run (each
            # immutable table gets its own salt); the I6 delete sweep
            # (tracebloc/backend#1209) reaps salt rows together with their
            # ds_ tables, so they don't accumulate indefinitely.
            self._table_salt = self.database.get_or_create_table_salt(
                self.physical_table_name
            )

        if self.table is None:
            # Grouped categories get a composite (group, time) secondary
            # index (backend#1054 WS1) so the engine's "fetch all rows of
            # the sampled sequences, ordered" reads don't full-scan.
            grouping = self._grouping
            index_columns = (
                [grouping.group_column, grouping.time_column]
                if grouping is not None
                else None
            )
            self.table = self.database.create_table(
                self.physical_table_name,
                self._table_schema,
                index_columns=index_columns,
                must_not_exist=self.per_ingestion_tables,
            )

        # Reconcile-on-start (backend#1028 item 2): a prior attempt that died
        # HARD (OOMKilled / SIGKILL) never reached the #227 compensating
        # delete in the except-branch below, so its rows are still in the
        # table while its dataset was never registered — and this run (the
        # k8s Job retry re-ingests the SAME source into the SAME table) would
        # otherwise duplicate every one of them under fresh data_ids. Reclaim
        # those orphans BEFORE processing: the run journal knows exactly
        # which ingestor_ids started here and never registered. Runs under
        # the table lock, like the rest of this method. Defensive try/except:
        # a reconcile failure must never block an otherwise healthy ingest —
        # the fallback is simply today's status quo (the orphans stay), and
        # this run's own counts are unaffected because every summary query
        # is scoped to its ingestor_id.
        try:
            # Under per-ingestion tables this is a guaranteed no-op (the
            # table was created fresh this run; a retry is a NEW table, so
            # reclaim never protects per-ingestion runs); dead-run husk
            # tables are I6's sweep instead (tracebloc/backend#1209). Kept
            # unconditional to preserve a single code path for legacy.
            self.database.reclaim_dead_run_rows(
                self.physical_table_name, self.ingestor_id
            )
        except Exception as reclaim_error:
            logger.critical(
                f"Orphan-row reconciliation failed for table "
                f"{self.physical_table_name!r}: {redaction.safe_db_error(reclaim_error)}. "
                f"Continuing the ingest — rows left by a previous hard-killed "
                f"run may remain in the table (backend#1028)."
            )
        # Journal this run as STARTED before its first row insert, so that if
        # THIS process dies hard at any later point, the next attempt's
        # reconcile pass can recognise (and reclaim) whatever rows it left
        # behind. Deliberately NOT wrapped: if MySQL can't take this one-row
        # insert, the batch inserts below would fail anyway — better to fail
        # now, before any row lands, than to insert rows the journal never
        # heard about.
        self.database.record_ingest_started(
            self.physical_table_name, self.ingestor_id, self.category
        )

        batch = []
        failed_records = []

        # Statistics tracking
        stats = {
            "ingestor_id": self.ingestor_id,
            "total_records": 0,
            "processed_records": 0,
            "inserted_records": 0,
            "api_sent_records": 0,
            "failed_records": 0,
            "skipped_records": 0,
            "file_transfer_failures": 0,
        }

        # Try to get total count for progress bar
        total = self._count_records(source)
        stats["total_records"] = total or 0

        # Flipped only after send_ingest_summary returns: gates the #227
        # compensating delete so a dataset that DID register never has its
        # rows deleted by a late failure (e.g. in summary rendering).
        dataset_registered = False

        with Session(self.engine) as session:
            try:
                pbar = tqdm(total=total, desc="Ingesting records", unit="records")

                _label_resolved = False
                for record in self.read_data(source):
                    stats["total_records"] += 0 if total else 1

                    # #340: pin the label column to the actual header spelling
                    # before it is processed. The validators matched the label
                    # case-/whitespace-insensitively, so a header like ``Label``
                    # for a config ``label: label`` passed preflight; without
                    # this the read path (exact key) would then read None for
                    # every row. Pin on the first record that CONTAINS the label
                    # — for CSV that's the first record (stable header); for
                    # sparse JSON a leading object may omit it, so keep trying.
                    if not _label_resolved:
                        _label_resolved = self._resolve_label_column(record.keys())

                    try:
                        processed_record = self.process_record(record)
                        if processed_record:
                            stats["processed_records"] += 1

                            if self.category in _FILE_BEARING_CATEGORIES:
                                processed_record = map_file_transfer(
                                    self.category,
                                    processed_record,
                                    self.file_options,
                                    self.database.config,
                                    # Raw record carries per-row sidecar
                                    # pointers (mask_id) that never belong on
                                    # the cleaned DB record (#212, P5).
                                    source_record=record,
                                )
                                # Skip record if file transfer failed. Tracked as
                                # `file_transfer_failures` (not `skipped_records`)
                                # so the summary can flag the silent-data-loss
                                # pattern from issue #99 — a missing source
                                # would otherwise let the DB / API write succeed
                                # and falsely report 100% success.
                                if processed_record is None:
                                    stats["file_transfer_failures"] += 1
                                    filename = record.get("filename", "Unknown")
                                    logger.warning(
                                        f"Skipping record due to file transfer failure: {filename}"
                                    )
                                    # Also surface the failure to the caller
                                    # so cli.run.main exits non-zero — without
                                    # this, a 100%-failed run would still
                                    # return [] and the K8s job marker would
                                    # be `Succeeded` (the silent-data-loss
                                    # pattern from #99).
                                    failed_records.append(
                                        {
                                            "record": record,
                                            "error": "file_transfer_failed",
                                        }
                                    )
                                    # Advance the progress bar so an
                                    # all-transfer-failure run doesn't leave
                                    # tqdm stuck at 0/N — without this the
                                    # `continue` skips the batch update that
                                    # would normally tick the bar.
                                    pbar.update(1)
                                    continue

                            batch.append(processed_record)

                            if len(batch) >= batch_size:
                                try:
                                    self._batch_writer.flush(
                                        batch, session, stats, failed_records
                                    )
                                finally:
                                    pbar.update(len(batch))
                                    batch = []
                        else:
                            # process_record returned None: the record was
                            # dropped (blank/invalid unique_id, or an error
                            # inside process_record). Count it AND surface it
                            # as a failed record so the run exits non-zero —
                            # a dropped record is silent data loss, not a
                            # clean skip. Before #234 these reached only
                            # skipped_records, never failed_records, so
                            # run_ingestion returned [] and the K8s Job was
                            # marked Succeeded despite losing rows. The
                            # specific reason was already logged by
                            # process_record / _map_unique_id.
                            stats["skipped_records"] += 1
                            failed_records.append(
                                {
                                    "record": record,
                                    "error": "record_dropped_in_processing",
                                }
                            )
                            pbar.update(1)  # Update progress bar for skipped records
                    except Exception as e:
                        # Count processing errors (including missing columns) as failed records
                        stats["failed_records"] += 1
                        failed_records.append({"record": record, "error": str(e)})
                        pbar.update(1)

                # Process remaining records
                if batch:
                    try:
                        self._batch_writer.flush(batch, session, stats, failed_records)
                    finally:
                        pbar.update(len(batch))

                session.commit()
                pbar.close()

                # Post-insert group-integrity pass (backend#1054 WS1, T5).
                # Row-drop-and-continue is correct per-row, but for grouped
                # categories a dropped row means its SEQUENCE is now missing
                # a timestep — training would read a truncated series as if
                # complete. Collect the sequence ids touched by any dropped/
                # failed record and remove those sequences' surviving rows,
                # so a sequence is stored whole or not at all. The dropped
                # rows stay in failed_records — the run still exits non-zero.
                grouping = self._grouping
                if grouping is not None and failed_records:
                    # Failed-row dicts carry the RAW CSV header spellings, so
                    # the fixed trait name must be resolved against each
                    # record's actual keys with the shared #340 rule
                    # (case-/whitespace-insensitive, resolve_column) — a
                    # header drifting only in case or whitespace would
                    # otherwise read None here, leave partial_ids empty, and
                    # keep a truncated sequence's surviving rows in MySQL.
                    partial_id_set = set()
                    for failure in failed_records:
                        record = failure.get("record")
                        if not isinstance(record, dict):
                            continue
                        key = resolve_column(record.keys(), grouping.group_column)
                        seq_id = record.get(key) if key is not None else None
                        if seq_id is not None and str(seq_id).strip():
                            partial_id_set.add(str(seq_id))
                    partial_ids = sorted(partial_id_set)
                    if partial_ids and stats["inserted_records"]:
                        removed = self.database.delete_sequences(
                            self.physical_table_name,
                            self.ingestor_id,
                            partial_ids,
                            group_column=grouping.group_column,
                        )
                        stats["inserted_records"] = max(
                            stats["inserted_records"] - removed, 0
                        )
                        logger.warning(
                            f"{YELLOW}Group-integrity pass (T5): dropped "
                            f"row(s) left {len(partial_ids)} partial "
                            f"sequence(s); removed their {removed} "
                            f"already-inserted row(s) so no truncated "
                            f"sequence is ever trained on.{RESET}"
                        )

                # Query accurate label counts from the DB (excludes any rows
                # that failed insertion) and collect a small preview sample.
                # The trait's ``count_unit`` selects the unit (review: #359 —
                # this is its behavioral consumer): "sequences" counts one
                # dataset item per sequence (backend#1054 Decision-3/T2); a
                # future grouped category counting rows falls through to the
                # standard row counts like every non-grouped category.
                if grouping is not None and grouping.count_unit == "sequences":
                    label_counts = self.database.get_label_sequence_counts(
                        self.physical_table_name,
                        self.ingestor_id,
                        group_column=grouping.group_column,
                    )
                    # number_of_sequences rides the meta_data channel (the
                    # serializer is unchanged — counts only, no id-lists).
                    # Labels are constant per sequence, so the per-label
                    # sequence counts sum to the sequence total.
                    self.file_options["number_of_sequences"] = sum(
                        label_counts.values()
                    )
                else:
                    label_counts = self.database.get_label_counts(
                        self.physical_table_name, self.ingestor_id
                    )

                if not stats["inserted_records"]:
                    logger.warning(
                        "No records were inserted for this ingestor run; "
                        "skipping ingest summary."
                    )
                elif not label_counts:
                    counts_helper = (
                        "get_label_sequence_counts"
                        if grouping is not None and grouping.count_unit == "sequences"
                        else "get_label_counts"
                    )
                    raise RuntimeError(
                        f"Inserted {stats['inserted_records']} row(s) but "
                        f"{counts_helper} returned nothing for "
                        f"ingestor_id={self.ingestor_id!r}. "
                        "The dataset was not registered with the backend; "
                        "this run's rows will be removed by the "
                        "compensating delete (#227)."
                    )
                else:
                    samples = self.database.get_samples(
                        self.physical_table_name, self.ingestor_id
                    )
                    dataset_title = (
                        self.api_client.config.TITLE
                        or f"{self.table_name} - {self.ingestor_id[:8]}"
                    )
                    schema_dict = self.database.get_table_schema(
                        self.physical_table_name
                    )

                    if self.category in _NLP_CATEGORIES:
                        text_profile = compute_text_profile(self.database.config)
                        if text_profile:
                            self.file_options["text_profile"] = text_profile

                    # Per-ingestor data-derived metadata computed during the run
                    # (e.g. the CSV ingestor's numeric feature_stats under
                    # attributes.feature_stats, #360). Merged onto the global-
                    # metadata channel here, alongside text_profile, so it is
                    # part of the payload the flip-then-send below ships.
                    self._apply_run_metadata()

                    # Flip the run journal to REGISTERED (backend#1028 item 2)
                    # BEFORE the remote summary call, not after. The local flip
                    # and the backend registration can't be made atomic, so
                    # whichever runs SECOND owns the crash/failure window. With
                    # the flip second (its previous position) a failed-and-
                    # swallowed UPDATE — or a hard kill after send returned —
                    # left the journal at registered=0 while the dataset WAS
                    # registered, so the next ingest's reclaim pass deleted a
                    # registered dataset's rows (silent, unrecoverable loss —
                    # bugbot High). Flipping FIRST inverts the failure mode: a
                    # crash between this commit and send completing just leaves
                    # the dataset unregistered, and the k8s retry re-ingests
                    # from source — a recoverable duplicate the 409-idempotent
                    # resend and the #227 delete already tolerate — never a
                    # deletion of registered rows. If send then FAILS, the
                    # except-branch undoes this flip (mark_ingest_unregistered)
                    # so reclaim can still recover the rows.
                    self.database.mark_ingest_registered(
                        self.physical_table_name, self.ingestor_id
                    )

                    self.api_client.send_ingest_summary(
                        table_name=self.table_name,
                        physical_table=(
                            self.physical_table_name
                            if self.per_ingestion_tables
                            else None
                        ),
                        ingestor_id=self.ingestor_id,
                        labels=label_counts,
                        dataset_title=dataset_title,
                        data_format=self.data_format,
                        data_intent=self.intent,
                        category=self.category,
                        schema=self._schema_payload(schema_dict),
                        samples=samples,
                        meta_data=self._meta_data_payload(),
                    )
                    dataset_registered = True
                    stats["api_sent_records"] = stats["inserted_records"]

                summary = IngestionSummary(**stats)
                self._log_summary(summary)

            except Exception as e:
                # Rows commit PER BATCH during the loop, so by the time any
                # failure lands here — mid-loop, or a genuine 4xx/5xx from
                # send_ingest_summary — this run's rows are already durable
                # in MySQL while its dataset will never be registered
                # (session.rollback() only discards uncommitted state).
                # Compensate by deleting exactly this run's rows, identified
                # by the per-process ingestor_id, so a failed run leaves the
                # table clean for a re-run (#227) instead of leaving orphans
                # that inflate the heartbeat's availability report and
                # occupy disk with no delete path (the live wound in #336).
                # A 409 from the summary call is idempotent success inside
                # the client and never reaches here; dataset_registered
                # guards the late-failure case so a REGISTERED dataset's
                # rows are never deleted. Residual two-generals window: if
                # the backend registered but the client-side call still
                # raised (e.g. a 2xx whose body failed to parse, after
                # retries), this delete removes rows of a backend-registered
                # dataset — narrow (POST is retried; 409 absorbs re-sends),
                # and a re-run re-ingests from source. Staged files stay —
                # they are keyed by filename and idempotently overwritten on
                # re-run. A cleanup failure must not mask the original
                # error: log CRITICAL and re-raise the original (the
                # leftover rows are the pre-#227 status quo, and remain
                # sweepable by ingestor_id).
                try:
                    session.rollback()
                except Exception as rollback_error:
                    # A dead connection can make the rollback itself throw —
                    # exactly the conditions this block fires under. It must
                    # not mask the original error or skip the cleanup.
                    logger.warning(f"session.rollback() failed: {rollback_error}")
                logger.error(f"Error during ingestion: {str(e)}")
                if stats.get("inserted_records") and not dataset_registered:
                    # The journal was flipped to REGISTERED just before the
                    # summary call (see above). Since registration did NOT
                    # complete, undo that optimistic flip FIRST — so if the
                    # compensating delete below fails, a later reclaim pass
                    # still sees registered=0 and can recover these rows
                    # instead of stranding them as a phantom registered entry
                    # (backend#1028). Best-effort: a reset failure must not
                    # mask the original error or skip the delete.
                    try:
                        self.database.mark_ingest_unregistered(
                            self.physical_table_name, self.ingestor_id
                        )
                    except Exception as journal_reset_error:
                        logger.critical(
                            f"Failed to reset the run journal to unregistered "
                            f"for ingestor_id={self.ingestor_id!r} in table "
                            f"{self.physical_table_name!r}: "
                            f"{redaction.safe_db_error(journal_reset_error)}. "
                            f"If the compensating delete also fails, these "
                            f"rows may not be reclaimed automatically "
                            f"(backend#1028)."
                        )
                    try:
                        deleted = self.database.delete_by_ingestor_id(
                            self.physical_table_name, self.ingestor_id
                        )
                        logger.error(
                            f"Compensating delete removed {deleted} "
                            f"unregistered row(s) for "
                            f"ingestor_id={self.ingestor_id!r} — the table "
                            "is clean; re-running is safe (#227)."
                        )
                    except Exception as cleanup_error:
                        logger.critical(
                            f"Compensating delete FAILED for "
                            f"ingestor_id={self.ingestor_id!r} in table "
                            f"{self.physical_table_name!r}: "
                            f"{redaction.safe_db_error(cleanup_error)}. "
                            f"{stats.get('inserted_records', 0)} unregistered "
                            "row(s) remain orphaned (#227)."
                        )
                raise e

        # Reclaim the staged source tree now that the load is fully verified
        # (rows committed + dataset registered above) AND clean (no failed
        # records). The per-record copies in ``map_file_transfer`` leave a
        # second copy of every file-bearing dataset on the shared PVC; without
        # this each ingest ~doubled PVC usage (#346). Gated on a CLEAN success
        # so a partial/failed run keeps its source for retry/inspection —
        # matching the compensating-delete branch above, which deliberately
        # leaves staged files in place ("overwritten on re-run"). Still inside
        # the table lock (released in ``ingest``'s finally), so it can't race a
        # concurrent ingest of the same table. ``reclaim_source`` is fully
        # guarded + best-effort: it never deletes a dir that contains the table
        # dir and never fails an already-successful load.
        if dataset_registered and not failed_records:
            reclaim_source(self.database.config)

        return failed_records

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup when used as context manager"""
        pass

    @property
    def _batch_writer(self) -> BatchWriter:
        """The ingestor's batch write-path collaborator (P5d). Owns DB insert +
        failure accounting; built from the run's database / table. A fresh
        instance per access is fine — it just holds those refs."""
        return BatchWriter(
            self.database, self.api_client, self.physical_table_name, self.ingestor_id
        )

    def _log_summary(self, summary: IngestionSummary):
        """Render the ingestion summary box.

        Delegates to :class:`tracebloc_ingestor.reporting.ConsoleRenderer` so the
        presentation (ANSI colours, emoji, the box layout) lives in one place,
        out of the ingestion logic (structural refactor — backend#796, P2).
        Kept as a method (rather than calling the renderer at the call site) so
        existing callers and tests that reference ``BaseIngestor._log_summary``
        keep working unchanged; the rendered output is byte-for-byte identical.
        """
        ConsoleRenderer().render_summary(summary)
