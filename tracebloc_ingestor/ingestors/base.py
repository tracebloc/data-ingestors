from abc import ABC, abstractmethod
from typing import Dict, Any, Generator, List, Optional, NamedTuple
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
import logging
import os
import pandas as pd
from tqdm import tqdm
import uuid
from pathlib import Path

from ..database import Database
from ..api.client import APIClient
from ..config import Config
from ..utils.constants import (
    Intent,
    TaskCategory,
    RESET,
    BOLD,
    GREEN,
    RED,
    YELLOW,
    BLUE,
    CYAN,
)
from ..utils import label_policy as label_policy_module
from ..utils.validators_mapping import map_validators
from ..file_transfer import map_file_transfer

# Logger for this module. Level is set by `setup_logging()` on the root
# logger when the user script calls it; child loggers inherit that level.
logger = logging.getLogger(__name__)

__all__ = ["BaseIngestor", "IngestionSummary"]


# Tabular-family categories carry `number_of_columns` in file_options;
# image / text categories do not (a schema may still be supplied — e.g.
# keypoint_detection's "Visibility" column — but a column count there
# would be a misleading metric).
_TABULAR_FAMILY_CATEGORIES = frozenset({
    TaskCategory.TABULAR_CLASSIFICATION,
    TaskCategory.TABULAR_REGRESSION,
    TaskCategory.TIME_SERIES_FORECASTING,
    TaskCategory.TIME_TO_EVENT_PREDICTION,
})

# File-bearing categories resolve every per-row sidecar against
# ``config.SRC_PATH``. If that path is empty/unset/missing, every file
# lookup silently falls through to a relative path (``"" + "images/x.jpg"``
# -> ``"images/x.jpg"``), file_transfer skips every record, and the user
# sees N copies of "Source image not found: images/x.jpg" — blaming the
# data when the real cause is "SRC_PATH was never staged on the PVC".
# Tabular / time-series have no sidecar dirs under SRC_PATH, so they're
# excluded from the guard (the CSV path itself is checked elsewhere).
_SRC_PATH_REQUIRED_CATEGORIES = frozenset({
    TaskCategory.IMAGE_CLASSIFICATION,
    TaskCategory.OBJECT_DETECTION,
    TaskCategory.KEYPOINT_DETECTION,
    TaskCategory.SEMANTIC_SEGMENTATION,
    TaskCategory.INSTANCE_SEGMENTATION,
    TaskCategory.TEXT_CLASSIFICATION,
    TaskCategory.TOKEN_CLASSIFICATION,
    TaskCategory.MASKED_LANGUAGE_MODELING,
})


# Self-supervised categories have no `label` column — the CSV manifest just
# points at sidecar files and the model creates its own targets at training
# time (e.g. masked_language_modeling masks tokens on-the-fly). The backend
# correspondingly stores no edge-label metadata for these datasets, so the
# `send_generate_edge_label_meta` call is a no-op at best and a misleading
# HTTP 400 ("No data found for table X") at worst — see issue #213.
# The schema (schema/ingest.v1.json) now rejects `label:` on these categories
# at submission time; this set + the gate below are the defensive in-ingestor
# half (script-driven runs that bypass the schema still skip the wasted call).
_SELF_SUPERVISED_CATEGORIES = frozenset({
    TaskCategory.MASKED_LANGUAGE_MODELING,
})


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
        or processing errored. Used to gate the "completed successfully"
        banner so customers can't mistake a partial run for a clean one.
        """
        return (
            self.failed_records > 0
            or self.file_transfer_failures > 0
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
        max_retries: Maximum number of retry attempts
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
        max_retries: int = 3,
        unique_id_column: Optional[str] = None,
        label_column: Optional[str] = None,
        intent: Optional[str] = None,
        annotation_column: Optional[str] = None,
        category: Optional[str] = None,
        data_format: Optional[str] = None,
        file_options: Optional[Dict[str, Any]] = None,
        label_policy: str = label_policy_module.PASSTHROUGH,
    ):
        """Initialize the base ingestor.

        Args:
            database: Database instance for data storage
            api_client: API client instance for data transmission
            table_name: Name of the target table
            schema: Database schema definition
            max_retries: Maximum number of retry attempts
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
        self.database = database
        self.engine: Engine = database.engine
        self.api_client = api_client
        self.table_name = table_name
        self.schema = schema
        self.max_retries = max_retries
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

        # Ensure table exists
        self.table = self.database.create_table(table_name, table_schema)

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
                f"Record {record} does not contain the required columns: {columns_not_found}"
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
            cleaned_record["label"] = label_policy_module.apply(
                label_val, self.label_policy
            )

        if self.intent:
            cleaned_record["data_intent"] = self.intent

        if self.annotation_column:
            cleaned_record["annotation"] = record.get(self.annotation_column)

        if not self.unique_id_column:
            # logger.warning("No unique ID column specified, generating unique ID mapping")
            cleaned_record["data_id"] = str(uuid.uuid4())
            return cleaned_record

        unique_id = record.get(self.unique_id_column)
        if unique_id is not None and str(unique_id).strip():
            cleaned_record["data_id"] = str(unique_id).strip()
            return cleaned_record
        else:
            logger.warning(f"Missing or invalid unique ID for record: {record}")
            return None

    def process_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
                    bool(v) if pd.api.types.is_bool(v)
                    else None if pd.isna(v) or v == ""
                    else str(v).strip()
                )
                for k, v in record.items()
                if k in self.schema and k not in columns_to_exclude
            }
            # Map unique ID if specified
            cleaned_record = self._map_unique_id(record, cleaned_record)

            logger.info(f"Cleaned record: {cleaned_record}")

            if cleaned_record is None:
                return None

            # Add ingestor_id to the record
            cleaned_record["ingestor_id"] = self.ingestor_id
            cleaned_record["filename"] = record.get("filename")
            cleaned_record["extension"] = record.get("extension")
            # Preserve mask_id for semantic_segmentation ONLY. The
            # cleaned_record comprehension above filters by ``k in
            # self.schema``, but for the documented 8-line schema-less
            # example yaml that filter drops every CSV column including
            # mask_id — which file_transfer.py:401 needs to locate the
            # per-row mask file. Without this, every record was skipped at
            # file-transfer with "No mask_id found in record" despite
            # #207's FilePairingValidator pass.
            #
            # Scoped to SEMANTIC_SEGMENTATION because mask_id is a runtime
            # indirection only — there's no `mask_id` column on the
            # standard tracebloc table (see database.py:standard_columns),
            # so putting it on every category's cleaned_record would break
            # SQL inserts on tables that don't have it (#212 bugbot).
            # _process_batch additionally pops it before insert so even
            # the semseg path doesn't try to bind it as a column.
            if self.category == TaskCategory.SEMANTIC_SEGMENTATION:
                cleaned_record["mask_id"] = record.get("mask_id")
            return cleaned_record

        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            return None

    @staticmethod
    def _check_src_path() -> None:
        """Fail fast with a clear message when ``config.SRC_PATH`` isn't set
        or doesn't exist (#772 P2).

        Every file-bearing category resolves its per-row sidecars against
        ``config.SRC_PATH`` (file_transfer.py:105 etc.). If the env var
        / ConfigMap key is empty, ``os.path.join("", "images", "x.jpg")``
        returns the relative path ``"images/x.jpg"`` — every file lookup
        fails and the user sees N copies of
        ``Source image not found: images/x.jpg`` blaming the data, when
        the real cause is "SRC_PATH was never set / the PVC wasn't
        staged before the Job ran". One clear error at preflight beats
        one cryptic error per row.
        """
        # Late import: keep this module free of a Config singleton at
        # import time so unit tests can monkeypatch the env per test.
        from ..config import Config
        src = Config().SRC_PATH
        if not src or not str(src).strip():
            raise RuntimeError(
                f"{RED}SRC_PATH is empty. Set it to the cluster-PVC path where "
                f"your data is staged (e.g. /data/shared/<dataset>/). The "
                f"chart's data-staging recipe (kubectl cp or init-container "
                f"sync) must run before the ingest Job — see "
                f"tracebloc/client/ingestor/README.md.{RESET}"
            )
        if not os.path.isabs(src):
            raise RuntimeError(
                f"{RED}SRC_PATH={src!r} is not an absolute path. The ingestor "
                f"resolves every sidecar file against it via os.path.join, "
                f"and a relative SRC_PATH silently falls through to the "
                f"working directory — every file lookup then fails with a "
                f"misleading 'Source image not found'. Use an absolute path "
                f"(e.g. /data/shared/<dataset>/).{RESET}"
            )
        if not os.path.isdir(src):
            raise RuntimeError(
                f"{RED}SRC_PATH={src!r} does not exist or is not a directory. "
                f"Did the data-staging step (kubectl cp / init-container sync) "
                f"run before the ingest Job? Verify with "
                f"`kubectl exec <pod> -- ls {src}`.{RESET}"
            )

    @staticmethod
    def _check_csv_encoding(source: Any) -> None:
        """Fail fast with a clear message if a CSV source is not valid UTF-8.

        Every validator reads CSVs as UTF-8 and swallows decode errors into a
        misleading "No data found"; a non-UTF-8 export (e.g. a Latin-1/Windows
        CSV with umlauts) would otherwise crash or mislead. Probe once, up front.
        """
        if not isinstance(source, (str, Path)):
            return
        path = Path(source)
        if path.suffix.lower() != ".csv" or not path.exists():
            return
        try:
            with open(path, "r", encoding="utf-8") as fh:
                while fh.read(1 << 20):  # decode in 1 MB chunks; raises on a bad byte
                    pass
        except UnicodeDecodeError as exc:
            raise ValueError(
                f"{RED}'{path.name}' is not valid UTF-8 — a non-UTF-8 byte was found at "
                f"byte {exc.start}. Re-save the file as UTF-8 (in Excel: Save As → "
                f"'CSV UTF-8 (Comma delimited)'), then re-ingest.{RESET}"
            ) from exc

    # Stale-lock cutoff (seconds). A crashed ingest leaves the lock file
    # behind; if the lock is older than this we log + remove + reacquire
    # so a customer isn't blocked indefinitely waiting for the writer
    # whose pod has long since been garbage-collected. 12h covers any
    # reasonable ingest (including multi-GB proteomics).
    _TABLE_LOCK_STALE_SECONDS = 12 * 3600

    def _table_lock_path(self) -> Optional[str]:
        """Where the lock file lives — at the top of STORAGE_PATH (the
        parent of every per-table DEST_PATH), so it's durable across pod
        restarts on the cluster PVC. Returns None when STORAGE_PATH is
        unset or not a directory (test configs / local runs without a
        staging dir) — caller treats that as "no lock available, skip".
        """
        # Late import: keep this module free of a Config singleton at
        # import time so unit tests can monkeypatch the env per test.
        from ..config import Config
        storage = Config().STORAGE_PATH
        if not storage or not os.path.isdir(storage):
            return None
        return os.path.join(storage, f".tracebloc-ingest-{self.table_name}.lock")

    def _acquire_table_lock(self) -> Optional[str]:
        """Acquire an exclusive lock for ``self.table_name`` (#772 P2).

        Two ingests targeting the same table used to race
        ``create_table`` / interleave upserts. Atomic ``O_EXCL`` create
        either succeeds (lock acquired) or fails with ``FileExistsError``
        (another ingest is in flight). On conflict, read the existing
        lock's metadata and surface it in the error so ops can find the
        other run; if the lock is older than the stale-cutoff, remove
        and reacquire.

        Returns the lock path (or None if no STORAGE_PATH is configured)
        so ``_release_table_lock`` can remove the right file.
        """
        import json as _json
        import socket as _socket
        from datetime import datetime as _datetime

        lock_path = self._table_lock_path()
        if lock_path is None:
            return None

        lock_info = {
            "ingestor_id": self.ingestor_id,
            "table_name": self.table_name,
            "pid": os.getpid(),
            "hostname": _socket.gethostname(),
            "started_at": _datetime.utcnow().isoformat() + "Z",
        }
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            # Read the existing lock so the error names the holder. Also
            # check staleness and self-recover if so.
            existing_info: Dict[str, Any] = {}
            try:
                with open(lock_path, "r") as f:
                    existing_info = _json.load(f)
            except Exception:
                pass
            age = None
            try:
                started = _datetime.fromisoformat(
                    existing_info.get("started_at", "").rstrip("Z")
                )
                age = (_datetime.utcnow() - started).total_seconds()
            except Exception:
                # Lock metadata is corrupt (truncated file, malformed
                # JSON, missing/un-parseable started_at). Fall back to
                # the file's mtime as the age signal (#221 bugbot: a
                # corrupt lock used to never auto-expire because age
                # stayed None). Use time.time() rather than
                # _datetime.utcnow().timestamp() — the latter is
                # timezone-broken (naive datetime treated as local) so
                # the cutoff would shift by the local UTC offset on
                # non-UTC systems.
                import time as _time
                try:
                    mtime = os.path.getmtime(lock_path)
                    age = _time.time() - mtime
                except OSError:
                    pass
            if age is not None and age > self._TABLE_LOCK_STALE_SECONDS:
                logger.warning(
                    f"{YELLOW}Stale lock at {lock_path} (age={age:.0f}s, "
                    f"holder={existing_info!r}) — removing and reacquiring. "
                    f"The holder's pod likely crashed before its finally "
                    f"could run.{RESET}"
                )
                try:
                    os.remove(lock_path)
                except FileNotFoundError:
                    pass
                return self._acquire_table_lock()
            raise RuntimeError(
                f"{RED}Another ingest is already running for table "
                f"'{self.table_name}' (lock at {lock_path}). "
                f"Holder: {existing_info!r}. Wait for it to finish, or — "
                f"if its pod crashed — remove the lock file manually. "
                f"(The lock auto-clears after "
                f"{self._TABLE_LOCK_STALE_SECONDS}s.){RESET}"
            )
        try:
            with os.fdopen(fd, "w") as f:
                _json.dump(lock_info, f)
        except Exception:
            # Couldn't write the metadata — drop the lock so we don't
            # block ourselves on a malformed file.
            try:
                os.remove(lock_path)
            except FileNotFoundError:
                pass
            raise
        logger.info(
            f"Acquired table lock for '{self.table_name}' at {lock_path}"
        )
        return lock_path

    def _release_table_lock(self, lock_path: Optional[str]) -> None:
        """Remove the lock file. No-op when ``_acquire_table_lock``
        returned None (no STORAGE_PATH configured). Idempotent — a
        double-release (e.g. exception path + finally path both call
        it) silently swallows ``FileNotFoundError``.
        """
        if not lock_path:
            return
        try:
            os.remove(lock_path)
            logger.info(f"Released table lock for '{self.table_name}'")
        except FileNotFoundError:
            pass
        except Exception as exc:
            logger.warning(
                f"Failed to remove table lock {lock_path}: {exc}. "
                f"It will auto-clear after "
                f"{self._TABLE_LOCK_STALE_SECONDS}s."
            )

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
        if self.category in _SRC_PATH_REQUIRED_CATEGORIES:
            self._check_src_path()

        # Pre-flight: a non-UTF-8 CSV otherwise surfaces as a misleading
        # "No data found" (validators read UTF-8 and swallow decode errors).
        # Catch it once here with a clear, actionable message.
        self._check_csv_encoding(source)

        # Pass the configured label_column through (without permanently
        # mutating file_options / metadata) so label-aware validators like
        # BIOLabelValidator check the right column when a custom name is used.
        validators = map_validators(
            self.category, {**self.file_options, "label_column": self.label_column}
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
        _lock_path = self._acquire_table_lock()
        try:
            return self._ingest_with_lock(source, batch_size)
        finally:
            self._release_table_lock(_lock_path)

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

        with Session(self.engine) as session:
            try:
                pbar = tqdm(total=total, desc="Ingesting records", unit="records")

                for record in self.read_data(source):
                    stats["total_records"] += 0 if total else 1

                    try:
                        processed_record = self.process_record(record)
                        if processed_record:
                            stats["processed_records"] += 1

                            if self.category in [
                                TaskCategory.IMAGE_CLASSIFICATION,
                                TaskCategory.OBJECT_DETECTION,
                                TaskCategory.TEXT_CLASSIFICATION,
                                TaskCategory.TOKEN_CLASSIFICATION,
                                TaskCategory.SEMANTIC_SEGMENTATION,
                                TaskCategory.KEYPOINT_DETECTION,
                                TaskCategory.MASKED_LANGUAGE_MODELING,
                            ]:
                                processed_record = map_file_transfer(
                                    self.category, processed_record, self.file_options
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
                                    self._flush_batch(
                                        batch, session, stats, failed_records
                                    )
                                finally:
                                    pbar.update(len(batch))
                                    batch = []
                        else:
                            stats["skipped_records"] += 1
                            pbar.update(1)  # Update progress bar for skipped records
                    except Exception as e:
                        # Count processing errors (including missing columns) as failed records
                        stats["failed_records"] += 1
                        failed_records.append({"record": record, "error": str(e)})
                        pbar.update(1)

                # Process remaining records
                if batch:
                    try:
                        self._flush_batch(batch, session, stats, failed_records)
                    finally:
                        pbar.update(len(batch))

                session.commit()
                pbar.close()

                # Register the dataset with the backend. Every step here is
                # REQUIRED: the rows are already committed to MySQL above, so if
                # any step fails the dataset is half-created — rows present but
                # not registered. The previous code nested these as
                # `if A: if B: if C: create()`, so a False return at ANY step
                # silently skipped the rest (including create_dataset AND the
                # summary) and the run STILL exited 0 — leaving committed rows
                # with no registered dataset and no error the user could see.
                # Fail loudly instead: raise so the process exits non-zero and
                # the failure surfaces (the CLI streams these logs live and marks
                # the Job failed). The api_client has already logged the
                # underlying HTTP detail before returning False.
                # Skip the edge-label backend call for self-supervised
                # categories (#213). They have no `label` column on the rows;
                # the backend's edge-label endpoint then returns a misleading
                # HTTP 400 ("No data found for table X" — wrong, the table HAS
                # rows, it just has no edge labels). Combined with PR #187's
                # fail-loud behaviour, the user saw a registration crash that
                # had nothing to do with the actual misconfiguration. The
                # schema now rejects `label:` on these categories at
                # submission, but this gate is the defensive in-ingestor half
                # so script-driven / older-schema runs don't trip the same
                # trap.
                if self.category not in _SELF_SUPERVISED_CATEGORIES:
                    if not self.api_client.send_generate_edge_label_meta(
                        self.table_name, self.ingestor_id, self.intent
                    ):
                        raise RuntimeError(
                            "Backend rejected edge-label metadata; the dataset was "
                            "NOT registered (its rows are already in the database). "
                            "See the logged API error above."
                        )

                schema_dict = self.database.get_table_schema(self.table_name)
                if not self.api_client.send_global_meta_meta(
                    self.table_name, schema_dict, self.file_options
                ):
                    raise RuntimeError(
                        "Backend rejected the dataset schema/metadata; the "
                        "dataset was NOT registered (its rows are already in the "
                        "database). See the logged API error above."
                    )

                if not self.api_client.prepare_dataset(
                    self.category,
                    self.ingestor_id,
                    self.data_format,
                    self.intent,
                ):
                    raise RuntimeError(
                        "Backend failed to prepare the dataset; it was NOT "
                        "registered (its rows are already in the database). See "
                        "the logged API error above."
                    )

                self.api_client.create_dataset(
                    category=self.category, ingestor_id=self.ingestor_id
                )

                # Create and log summary — only after successful registration.
                summary = IngestionSummary(**stats)
                self._log_summary(summary)

            except Exception as e:
                session.rollback()
                logger.error(f"Error during ingestion: {str(e)}")
                raise e

        return failed_records

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup when used as context manager"""
        pass

    def _flush_batch(
        self,
        batch: List[Dict[str, Any]],
        session: Session,
        stats: Dict[str, int],
        failed_records: List[Dict[str, Any]],
    ) -> None:
        """Process one batch and fold its outcome into ``stats`` /
        ``failed_records``. Shared by the in-loop and final-batch flush
        sites in ``_ingest_with_lock``.

        Failure accounting is the point of this helper. A run where every
        batch POST was rejected with HTTP 400 used to finish with
        "All records processed successfully" and exit 0 — the rows were
        in MySQL but the backend had zero records, and the next platform
        call failed with "No data found for table name". Two swallow
        points caused that:

        - ``api_success=False`` only skipped the ``api_sent_records``
          increment; the records never reached ``failed_records``, so
          ``ingest()`` returned ``[]`` and the caller exited 0. Now each
          inserted-but-unsent record is returned as a failed record with
          ``error="api_send_failed"`` (the rows stay committed — they're
          in MySQL but invisible to the platform until re-sent).
        - an exception from ``_process_batch`` was logged and dropped,
          leaving the whole batch out of every counter. Now the batch is
          counted and returned as failed.

        The summary needs no extra field: "Failed to Send to API" is
        derived from ``inserted_records - api_sent_records``, and
        ``IngestionSummary.has_failures`` already trips on that gap.
        """
        try:
            inserted_ids, api_success, db_failures = self._process_batch(
                batch, session
            )
            # Only count records that were successfully inserted
            if inserted_ids:
                stats["inserted_records"] += len(inserted_ids)
                if api_success:
                    stats["api_sent_records"] += len(inserted_ids)
                else:
                    # The inserted-but-unsent records are the batch minus
                    # the DB failures. Don't assume they're the first
                    # len(ids) entries: insert_batch's per-record fallback
                    # appends successes in scan order, so a mid-batch DB
                    # failure shifts which records were inserted. Failure
                    # entries carry a *copy* of the record (processed_record
                    # adds updated_at), so match by data_id — set on every
                    # processed record by _map_unique_id — not by identity.
                    db_failed_data_ids = {
                        f.get("record", {}).get("data_id") for f in db_failures
                    }
                    failed_records.extend(
                        {"record": record, "error": "api_send_failed"}
                        for record in batch
                        if record.get("data_id") not in db_failed_data_ids
                    )
            if db_failures:
                stats["failed_records"] += len(db_failures)
                failed_records.extend(db_failures)
        except Exception as e:
            logger.error(f"Batch processing failed: {str(e)}")
            stats["failed_records"] += len(batch)
            failed_records.extend(
                {"record": record, "error": str(e)} for record in batch
            )

    def _process_batch(
        self, batch: List[Dict[str, Any]], session: Session
    ) -> List[int]:
        """
        Process and insert a batch of records

        Args:
            batch: List of records to process
            session: Database session

        Returns:
            List of record IDs

        Raises:
            Exception: If batch processing fails
        """
        try:
            # Strip framework-internal runtime indirections that don't
            # correspond to a DB column before binding. ``mask_id`` is
            # carried on semantic_segmentation records purely so
            # ``file_transfer.map_file_transfer`` can locate the per-row
            # mask file; the standard tracebloc table has no ``mask_id``
            # column (see database.py:standard_columns), so leaving it on
            # the record would cause SQLAlchemy to treat it as an
            # unconsumed column on insert (#212 bugbot). By the time we
            # reach this point, file_transfer has already used the value
            # — it's safe to drop.
            for r in batch:
                r.pop("mask_id", None)
            # Insert batch and get IDs
            ids, db_failures = self.database.insert_batch(self.table_name, batch)
            api_success = False
            # Send to API with ingestor_id
            if ids:  # Only send to API if we have valid IDs
                api_success = self.api_client.send_batch(
                    [(id, record) for id, record in zip(ids, batch)],
                    self.table_name,
                    ingestor_id=self.ingestor_id,  # Include ingestor_id in API requests
                )
            return (
                ids if ids else [],
                api_success,
                db_failures,
            )  # Ensure we always return a list

        except Exception as e:
            logger.error(f"{RED}Error processing batch: {str(e)}{RESET}")
            # Guard the attribute chain: a non-HTTP exception (e.g. a DB
            # error) has no .response at all, and the old
            # hasattr(e.response, "text") raised AttributeError INSIDE the
            # handler — replacing the real error with "'RuntimeError'
            # object has no attribute 'response'".
            response = getattr(e, "response", None)
            if response is not None and hasattr(response, "text"):
                logger.error(f"{RED}Error response: {response.text}{RESET}")
            raise

    def _log_summary(self, summary: IngestionSummary):
        """Log ingestion summary in a clear, formatted way with enhanced visual appeal.

        A "success" here means the record was inserted to the DB, sent to
        the API, AND its sidecar file (image / annotation / mask / text)
        was copied to the destination. Records whose source file was
        missing are subtracted from the success count even if the DB
        row landed — see issue #99 for the silent-data-loss pattern that
        motivated this.
        """

        # A successful record requires DB insert AND API send AND, where
        # applicable, a successful file transfer. File-transfer failures
        # short-circuit before the DB write (they're skipped from the
        # batch), so subtracting them from total_records gives the
        # denominator's effective ceiling. Use inserted_records (the
        # actual durable outcome) as the numerator.
        success_rate = 0
        if summary.total_records > 0:
            success_rate = (summary.inserted_records / summary.total_records) * 100

        # Determine overall status color
        status_color = (
            GREEN if success_rate >= 90 and not summary.has_failures
            else YELLOW if success_rate >= 70
            else RED
        )

        print(f"\n{CYAN}{'═'*60}{RESET}")
        print(f"{BOLD}{CYAN}📊 INGESTION SUMMARY 📊{RESET}")
        print(f"{CYAN}{'═'*60}{RESET}")
        print(
            f"{BOLD}Ingestor ID:{RESET}                {BLUE}{summary.ingestor_id}{RESET}"
        )
        # Main statistics with icons and colors
        print(
            f"{BOLD}📈 Total Records Found:{RESET}     {BLUE}{summary.total_records:,}{RESET}"
        )
        print(
            f"{BOLD}✅ Successfully Processed:{RESET}  {GREEN}{summary.processed_records:,}{RESET}"
        )
        print(
            f"{BOLD}💾 Inserted to Database:{RESET}    {GREEN}{summary.inserted_records:,}{RESET}"
        )
        print(
            f"{BOLD}🚀 Sent to API:{RESET}             {GREEN}{summary.api_sent_records:,}{RESET}"
        )
        print(
            f"{BOLD}⏭️  Skipped Records:{RESET}        {YELLOW}{summary.skipped_records:,}{RESET}"
        )
        file_transfer_color = (
            RED if summary.file_transfer_failures > 0 else GREEN
        )
        print(
            f"{BOLD}📁 File Transfer Failures:{RESET}  {file_transfer_color}{summary.file_transfer_failures:,}{RESET}"
        )
        print(
            f"{BOLD}❌ Failed DB Insertion:{RESET}     {RED}{summary.failed_records:,}{RESET}"
        )
        # Only count records that made it to a DB insert but didn't ship
        # to the API. Using `total_records - api_sent_records` would also
        # include file-transfer failures and DB failures (which never had
        # a chance to ship), giving an inflated, double-counted total.
        api_only_failures = max(
            0, summary.inserted_records - summary.api_sent_records
        )
        print(
            f"{BOLD}❌ Failed to Send to API:{RESET}   {RED}{api_only_failures:,}{RESET}"
        )
        print(f"{CYAN}{'─'*60}{RESET}")

        # Success rate with visual indicator
        if summary.total_records > 0:
            # Progress bar
            bar_length = 30
            filled_length = int(bar_length * success_rate / 100)
            bar = "█" * filled_length + "░" * (bar_length - filled_length)
            print(
                f"{BOLD}📊 Success Rate:{RESET} [{status_color}{bar}{RESET}] {status_color}{success_rate:.1f}%{RESET}"
            )

        # Status banner. Any non-trivial failure (DB, API, or file-transfer)
        # disqualifies the "completed successfully" message — a customer
        # seeing 🎉 should be able to trust that no record was silently
        # dropped. The three failure channels are mutually exclusive per
        # record (file-transfer failures never reach DB; DB failures never
        # reach API; api_only_failures are records that hit DB but didn't
        # ship), so summing them gives a clean unique count instead of
        # the double-count `total_records - api_sent_records` would produce.
        total_failures = (
            summary.failed_records
            + summary.file_transfer_failures
            + api_only_failures
        )
        if not summary.has_failures:
            status_msg = "🎉 Ingestion completed successfully!"
        elif success_rate >= 80:
            status_msg = (
                f"⚠️  Ingestion completed with {total_failures:,} failure(s), "
                "see logs."
            )
        elif success_rate >= 60:
            status_msg = (
                f"⚠️  Ingestion completed with {total_failures:,} failure(s); "
                "many records failed to process — see logs."
            )
        else:
            status_msg = (
                f"❌ Critical! Ingestion completed with {total_failures:,} "
                "failure(s); most records failed — see logs."
            )

        print(f"{CYAN}{'─'*60}{RESET}")
        print(f"{BOLD}{status_color}{status_msg}{RESET}")
        print(f"{CYAN}{'═'*60}{RESET}\n")
