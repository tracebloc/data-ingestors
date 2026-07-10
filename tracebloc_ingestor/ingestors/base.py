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
)
from ..utils import label_policy as label_policy_module
from ..utils.columns import resolve_column
from ..utils.validators_mapping import map_validators
from ..file_transfer import map_file_transfer
from ..text_profile import compute_text_profile
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
    TABULAR_FAMILY_CATEGORIES as _TABULAR_FAMILY_CATEGORIES,
)

# Logger for this module. Level is set by `setup_logging()` on the root
# logger when the user script calls it; child loggers inherit that level.
logger = logging.getLogger(__name__)

__all__ = ["BaseIngestor", "IngestionSummary"]


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
        data_id_strategy: str = "uuid",
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
        # #225: deterministic content-hash data_id (opt-in, landed dark).
        # unique_id_column wins if both are set (schema can't express both;
        # belt-and-suspenders). The salt fetch is DEFERRED to ingest() —
        # mirroring #260's deferred create_table — so a validator-rejected
        # run leaves no salt row behind. get_or_create is atomic
        # (INSERT IGNORE), so it needs no table lock even then.
        self.data_id_strategy = data_id_strategy
        self._table_salt: Optional[str] = None
        self.database = database
        self.engine: Engine = database.engine
        self.api_client = api_client
        self.table_name = table_name
        self.schema = schema
        self.unique_id_column = unique_id_column
        self.label_column = label_column
        self.intent = intent
        self.annotation_column = annotation_column
        self.category = category
        self.data_format = data_format
        self.file_options = file_options or {}
        self.label_policy = label_policy

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
        # These are handled separately and should not be ingested as regular columns
        table_schema = schema.copy()
        if self.label_column and self.label_column in table_schema:
            del table_schema[self.label_column]
        if self.annotation_column and self.annotation_column in table_schema:
            del table_schema[self.annotation_column]
        if self.unique_id_column and self.unique_id_column in table_schema:
            del table_schema[self.unique_id_column]

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
            try:
                logger.info(f"{CYAN}Running validator: {validator.name}{RESET}")
                result = validator.validate(source)

                if not result.is_valid:
                    all_valid = False
                    validation_errors.append(
                        f"{BOLD}{validator.name} Validator failed: {RESET} \n {RED}"
                    )
                    validation_errors.extend(result.errors)
                    validation_errors.append(f"{RESET}")

                # Log warnings if any
                for warning in result.warnings:
                    logger.warning(
                        f"{YELLOW}Validation warning - {validator.name}: {warning}{RESET}"
                    )
                if result.is_valid:
                    print(
                        f"{GREEN}{validator.name} Validator successfully passed{RESET}"
                    )
            except Exception as e:
                all_valid = False
                validation_errors.append(f"Validator {validator.name} error: {str(e)}")

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

        Called once, just before ``send_ingest_summary`` ships the payload, and
        merged into ``file_options``. The base emits nothing; subclasses override
        to contribute (e.g. ``CSVIngestor`` returns per-column ``feature_stats``,
        #360). Kept as a hook so the base engine stays format-agnostic rather than
        branching on category the way the ``text_profile`` injection does.
        """
        return {}

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
            self._table_salt = self.database.get_or_create_table_salt(
                self.table_name
            )

        if self.table is None:
            self.table = self.database.create_table(self.table_name, self._table_schema)

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

                # Query accurate label counts from the DB (excludes any rows that
                # failed insertion) and collect a small preview sample.
                label_counts = self.database.get_label_counts(
                    self.table_name, self.ingestor_id
                )

                if not stats["inserted_records"]:
                    logger.warning(
                        "No records were inserted for this ingestor run; "
                        "skipping ingest summary."
                    )
                elif not label_counts:
                    raise RuntimeError(
                        f"Inserted {stats['inserted_records']} row(s) but "
                        f"get_label_counts returned nothing for "
                        f"ingestor_id={self.ingestor_id!r}. "
                        "The dataset was not registered with the backend; "
                        "this run's rows will be removed by the "
                        "compensating delete (#227)."
                    )
                else:
                    samples = self.database.get_samples(
                        self.table_name, self.ingestor_id
                    )
                    dataset_title = (
                        self.api_client.config.TITLE
                        or f"{self.table_name} - {self.ingestor_id[:8]}"
                    )
                    schema_dict = self.database.get_table_schema(self.table_name)

                    if self.category in _NLP_CATEGORIES:
                        text_profile = compute_text_profile(self.database.config)
                        if text_profile:
                            self.file_options["text_profile"] = text_profile

                    # Per-ingestor data-derived metadata computed during the run
                    # (e.g. the CSV ingestor's numeric feature_stats, #360). Merged
                    # onto the global-metadata channel here, alongside text_profile,
                    # just before the payload ships.
                    self.file_options.update(self._collect_run_metadata())

                    self.api_client.send_ingest_summary(
                        table_name=self.table_name,
                        ingestor_id=self.ingestor_id,
                        labels=label_counts,
                        dataset_title=dataset_title,
                        data_format=self.data_format,
                        data_intent=self.intent,
                        category=self.category,
                        schema=schema_dict,
                        samples=samples,
                        meta_data=self.file_options,
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
                    try:
                        deleted = self.database.delete_by_ingestor_id(
                            self.table_name, self.ingestor_id
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
                            f"{self.table_name!r}: {cleanup_error}. "
                            f"{stats.get('inserted_records', 0)} unregistered "
                            "row(s) remain orphaned (#227)."
                        )
                raise e

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
            self.database, self.api_client, self.table_name, self.ingestor_id
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
