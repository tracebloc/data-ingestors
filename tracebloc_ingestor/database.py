from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    BigInteger,
    CHAR,
    DateTime,
    Date,
    Time,
    text,
    Text,
    Integer,
    String,
    Float,
    Boolean,
    Double,
    Numeric,
    Index,
    bindparam,
    inspect,

)
from sqlalchemy.engine import Engine
from sqlalchemy import LargeBinary
from sqlalchemy.dialects.mysql import insert, LONGBLOB, BLOB
from sqlalchemy.exc import OperationalError, InterfaceError, DBAPIError
import logging
import secrets

from .utils import redaction
from urllib.parse import quote
from typing import List, Dict, Any, Optional
from datetime import datetime
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from .config import Config
from .identifiers import MAX_COLUMN_IDENTIFIER_LENGTH
from .utils.typo_suggest import suggest_type as _suggest_type

# Configure unified logging with config
config = Config()
logger = logging.getLogger(__name__)


# Transient MySQL/SQLAlchemy errors that warrant a retry. Roughly: anything
# from the network / connection layer or a server-side temporary state. We
# deliberately DO NOT retry IntegrityError / DataError / ProgrammingError —
# those reflect bad data or schema and won't fix themselves on a retry.
#   - OperationalError: "MySQL server has gone away", "Lost connection during
#     query", "Deadlock found", connection-pool eviction, network blip.
#   - InterfaceError: stale connection / lower-level driver fault.
# Issue: backend #772 P2 — `insert_batch` had no DB-retry; a 5-second
# MySQL restart in the middle of an 8-hour proteomics ingest failed every
# in-flight batch permanently. file_transfer already uses tenacity for
# file-copy retries; the DB path was just inconsistent.
_DB_RETRY_EXCEPTIONS = (OperationalError, InterfaceError)


_retry_on_transient_db_error = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(_DB_RETRY_EXCEPTIONS),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


@_retry_on_transient_db_error
def _execute_with_retry(connection, stmt):
    """Run ``connection.execute(stmt)`` with bounded retries on transient
    DB errors (network blip, MySQL restart, stale pool connection). A
    permanent error (IntegrityError, DataError, …) is NOT retried and
    propagates immediately to the existing per-row fallback path.

    Rollback between attempts (#219 bugbot): SQLAlchemy leaves the
    connection in a pending-rollback state after a failed statement, so
    the next ``connection.execute`` on the SAME connection would raise
    ``PendingRollbackError`` — a NON-transient class tenacity doesn't
    retry, which would cut the retries short and skip the intended
    backoff. Rolling back here resets the connection's transactional
    state so the next attempt sees a clean slate. The rollback runs on
    EVERY transient failure (including the final one that propagates),
    which is also fine — the per-row fallback path runs another
    rollback at the top, so the double-rollback is a harmless no-op.
    """
    try:
        return connection.execute(stmt)
    except _DB_RETRY_EXCEPTIONS:
        try:
            connection.rollback()
        except Exception as rb_exc:
            # The rollback itself might fail on a truly dead connection.
            # Swallow it so the original transient error propagates to
            # tenacity for the retry (or, on the last attempt, to the
            # caller's existing error path).
            logger.debug(
                f"connection.rollback() failed between retries: {rb_exc}"
            )
        raise


class Database:
    def __init__(self, config: Config):
        self.config = config
        self.engine = self._create_engine()
        self.metadata = MetaData()
        self.tables: Dict[str, Table] = {}
        self.unique_id_column: Optional[str] = (
            None  # Store table-specific unique ID column mappings
        )

    def _create_engine(self) -> Engine:
        # First create database if it doesn't exist
        base_connection_string = (
            f"mysql+mysqlconnector://{self.config.DB_USER}:{quote(self.config.DB_PASSWORD)}"
            f"@{self.config.DB_HOST}:{self.config.DB_PORT}"
        )
        # hide_parameters: SQLAlchemy otherwise appends every statement
        # parameter — i.e. whole rows of customer data — to error strings
        # that get logged (#226).
        engine = create_engine(
            base_connection_string, pool_pre_ping=True, hide_parameters=True
        )

        with engine.connect() as connection:
            connection.execute(
                text(f"CREATE DATABASE IF NOT EXISTS {self.config.DB_NAME}")
            )
            connection.commit()

        # Now connect to the specific database
        connection_string = f"{base_connection_string}/{self.config.DB_NAME}"
        return create_engine(
            connection_string, pool_pre_ping=True, hide_parameters=True
        )

    def _get_sqlalchemy_type(self, mysql_type: str):
        """Convert MySQL type to SQLAlchemy type.
        
        Extracts the base type (before parentheses) and matches exactly to avoid
        substring issues (e.g., "DATE" matching "DATETIME").
        """
        type_mapping = {
            "VARCHAR": String,
            "CHAR": CHAR,
            "TEXT": Text,
            "INT": Integer,
            "INTEGER": Integer,
            "TINYINT": Integer,
            "SMALLINT": Integer,
            "MEDIUMINT": Integer,
            "BIGINT": BigInteger,
            "FLOAT": Float,
            "DOUBLE": Double,
            "DECIMAL": Numeric,
            "NUMERIC": Numeric,
            "BOOLEAN": Boolean,
            "BOOL": Boolean,
            "DATE": Date,
            "DATETIME": DateTime,
            "TIMESTAMP": DateTime,
            "TIME": Time,
            "BLOB": BLOB,
            "LONGBLOB": LONGBLOB,
        }
        
        mysql_type_upper = mysql_type.upper().strip()
        base_type = mysql_type_upper.split("(")[0].split()[0]
        
        if base_type in type_mapping:
            alchemy_type = type_mapping[base_type]
            # Extract parenthesised arguments. Two shapes:
            #   - VARCHAR(255) / CHAR(10)      -> single int length
            #   - DECIMAL(10, 2) / NUMERIC(p,s) -> precision, scale (both honoured;
            #     previously int("10,2") raised ValueError and we silently fell
            #     back to a bare Numeric, dropping the declared scale and writing
            #     the column at MySQL's default — losing precision on the values
            #     it then bound).
            if "(" in mysql_type_upper:
                try:
                    inside = mysql_type_upper.split("(", 1)[1].rsplit(")", 1)[0]
                    parts = [int(p.strip()) for p in inside.split(",") if p.strip()]
                except (ValueError, IndexError):
                    parts = []
                if len(parts) == 2 and base_type in ("DECIMAL", "NUMERIC"):
                    return alchemy_type(parts[0], parts[1])
                if len(parts) == 1:
                    return alchemy_type(parts[0])
            return alchemy_type

        # Surface a "did you mean" hint when the typo is one edit away from
        # a supported type — BIGINTEGER → BIGINT, BOOLEAN → BOOL, NUMRIC →
        # NUMERIC, etc. A user-facing schema error that just says
        # "Unsupported" leaves the customer guessing; a single short hint
        # turns a 5-minute "what's the right spelling?" round trip into a
        # zero-thought fix. Levenshtein distance ≤ 3 catches every realistic
        # typo we've seen without false-flagging unrelated types.
        suggestion = _suggest_type(base_type, type_mapping.keys())
        if suggestion:
            raise ValueError(
                f"Unsupported MySQL type: {mysql_type}. Did you mean "
                f"'{suggestion}'? Supported types: "
                f"{sorted(type_mapping.keys())}"
            )
        raise ValueError(
            f"Unsupported MySQL type: {mysql_type}. Supported types: "
            f"{sorted(type_mapping.keys())}"
        )

    def create_table(
        self,
        table_name: str,
        schema: Dict[str, str],
        index_columns: Optional[List[str]] = None,
        must_not_exist: bool = False,
    ):
        """
        Creates a table if it doesn't exist, or returns existing table

        Args:
            table_name: Name of the table
            schema: Dictionary defining the table schema
            must_not_exist: RFC-0003 D16/D19 (tracebloc/backend#1205) —
                per-ingestion tables are immutable, so reusing or reflecting
                an existing table is a contract violation, never a
                convenience. When True, an existing table (cached or in the
                database) raises instead of being returned. Callers pass the
                per-ingestion flag here; legacy shared-table ingests keep the
                reflect/append behavior below.
            index_columns: Optional list of schema columns to compose into a
                secondary (non-unique) index. Used by the sequence-grouped
                categories to create the composite ``(sequence_id,
                timestamp)`` index (backend#1054 WS1) so the engine's grouped
                "fetch all rows of the sampled sequences, ordered" reads
                don't full-scan. Ignored when the table already exists
                (reflected as-is). Columns must be part of ``schema``.

        Returns:
            SQLAlchemy Table object

        Raises:
            ValueError: if a schema column collides with a reserved/internal
                column (e.g. a user CSV with its own ``id``), which would
                otherwise surface as a cryptic SQLAlchemy DuplicateColumnError;
                or if ``index_columns`` references a column absent from
                ``schema``.
        """
        if table_name in (self.SALT_TABLE, self.RUNS_TABLE):
            raise ValueError(
                f"{table_name!r} is reserved for tracebloc ingest bookkeeping "
                "(the content-hash salt store #225 / the run journal "
                "backend#1028) and cannot be used as a dataset table."
            )
        # Fail fast on reserved-column collisions before any DB I/O. `label`
        # is intentionally excluded — it's the user-facing label column the
        # framework maps onto the standard `label` column.
        _RESERVED = {
            "id", "created_at", "updated_at", "status", "data_intent",
            "data_id", "filename", "extension", "annotation", "ingestor_id",
        }
        _collisions = sorted(_RESERVED & set(schema))
        if _collisions:
            raise ValueError(
                f"Schema column(s) {_collisions} collide with reserved tracebloc "
                f"columns. The framework manages its own row id, timestamps, status, "
                f"data_id and sidecar metadata — rename these column(s) in your "
                f"CSV/schema. (To use your own `id` as the record identifier, set "
                f"data_id.strategy=column instead.)"
            )

        # Fail fast on column names longer than MySQL's 64-char identifier limit,
        # before CREATE TABLE turns it into a raw MySQL 1059 error. CSV headers are
        # used verbatim as column names, so long proteomics/genomics headers (e.g. a
        # semicolon-joined isoform list) blow the limit; name the offenders clearly.
        # The 64-char cap is the only column-name restriction enforced at ingest:
        # every other shape (digit-leading, '|'/'.'/'-', spaces, unicode, even
        # backticks) is deliberately accepted and backtick-quoted downstream
        # (see identifiers.py — the canonical grammar shared with the trainer,
        # ISSUE #382). MySQL's identifier limit is the one thing quoting can't
        # rescue, so it stays a hard, actionable failure here.
        _MAX_IDENTIFIER = MAX_COLUMN_IDENTIFIER_LENGTH
        _too_long = sorted(c for c in schema if len(str(c)) > _MAX_IDENTIFIER)
        if _too_long:
            preview = "; ".join(f"'{c[:40]}…' ({len(c)} chars)" for c in _too_long[:5])
            more = "" if len(_too_long) <= 5 else f" (and {len(_too_long) - 5} more)"
            raise ValueError(
                f"{len(_too_long)} column name(s) exceed the {_MAX_IDENTIFIER}-character "
                f"database column-name limit and must be shortened: {preview}{more}"
            )

        # Fail fast on too many columns before CREATE TABLE turns it into a raw
        # MySQL 1117 ("Too many columns"). MySQL's hard limit is 4096 columns per
        # table; the framework adds ~11 standard columns on top of the schema, so
        # bound the user schema below that. A very wide panel (genomics /
        # proteomics matrices with thousands of feature columns) is the realistic
        # trigger. (MySQL also caps the row at ~65535 bytes — that limit binds
        # first for very wide VARCHAR panels and still surfaces at CREATE TABLE as
        # 1118; this count guard catches the common numeric-panel case with an
        # actionable message.)
        _MAX_FEATURE_COLUMNS = 4000
        if len(schema) > _MAX_FEATURE_COLUMNS:
            raise ValueError(
                f"Schema has {len(schema)} columns, exceeding the supported "
                f"maximum of {_MAX_FEATURE_COLUMNS} (MySQL's hard limit is 4096 "
                f"columns per table, and the framework reserves ~11). Reduce the "
                f"column count — e.g. narrow the feature panel, or pivot a very "
                f"wide matrix to long form."
            )

        # Return existing table if already created
        if table_name in self.tables:
            if must_not_exist:
                raise ValueError(
                    f"Table '{table_name}' already exists but was created as "
                    f"a per-ingestion table (RFC-0003 D16: one immutable "
                    f"table per ingest run, append disabled). This should be "
                    f"impossible for a fresh ds_<uuid4> name; a collision "
                    f"means ingestor_id reuse within one process."
                )
            return self.tables[table_name]

        # Check if table exists in database
        inspector = inspect(self.engine)
        if table_name in inspector.get_table_names():
            if must_not_exist:
                raise ValueError(
                    f"Table '{table_name}' already exists in the database, "
                    f"but per-ingestion tables are immutable (RFC-0003 D16/"
                    f"D19 — one table per ingest run, append disabled). A "
                    f"leftover ds_ table under a fresh uuid4 ingestor_id "
                    f"should be impossible; drop it and re-run."
                )
            # Reflect existing table using MetaData
            self.metadata.reflect(self.engine, only=[table_name])
            table = self.metadata.tables[table_name]

            # Fail fast if the existing table's feature columns don't match the
            # incoming schema. Reflecting and silently reusing a mismatched table
            # makes every record insert die downstream with SQLAlchemy's cryptic
            # "Unconsumed column names: ..." — the record keys (built from the
            # current schema) reference columns the reflected table doesn't have.
            # This happens when a table is left over from an earlier ingestion:
            # e.g. a prior run created the table and then failed before inserting,
            # or the dataset's column names changed between pushes (a customer
            # renaming proteomics headers like `P01033|TIMP1` -> `P01033_TIMP1`
            # to work around an unrelated error is exactly this case). Surface an
            # actionable error naming the drift instead.
            _STANDARD_COLUMNS = {
                "id", "created_at", "updated_at", "status", "label",
                "data_intent", "data_id", "filename", "extension",
                "annotation", "ingestor_id",
            }
            existing_features = {c.name for c in table.columns} - _STANDARD_COLUMNS
            expected_features = set(schema) - _STANDARD_COLUMNS
            if expected_features and existing_features != expected_features:
                missing = sorted(expected_features - existing_features)
                extra = sorted(existing_features - expected_features)

                def _preview(cols):
                    head = ", ".join(cols[:8])
                    return f"{head}{'' if len(cols) <= 8 else f', … (+{len(cols) - 8} more)'}"

                raise ValueError(
                    f"Table '{table_name}' already exists with feature columns "
                    f"that do not match the dataset schema. This usually means a "
                    f"stale table from an earlier ingestion (one that failed "
                    f"before inserting, or a dataset whose column names changed "
                    f"between pushes). "
                    f"In the schema but not the table: [{_preview(missing)}]. "
                    f"In the table but not the schema: [{_preview(extra)}]. "
                    f"Drop the existing '{table_name}' table, or ingest under a "
                    f"new dataset name, and re-run."
                )

            self.tables[table_name] = table
            return table

        # Define standard columns that should be present in all tables
        standard_columns = [
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("created_at", DateTime, server_default=text("CURRENT_TIMESTAMP")),
            Column(
                "updated_at",
                DateTime,
                server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
            ),
            Column(
                "status", Integer, server_default=text("0")
            ),  # 1 for active, 0 for inactive
            Column("label", String(255), nullable=True),
            Column("data_intent", String(100), nullable=True),
            Column("data_id", String(255), unique=True, nullable=False),
            Column("filename", String(255), nullable=True),
            Column("extension", String(10), nullable=True),
            Column("annotation", Text, nullable=True),
            Column("ingestor_id", String(255), nullable=True),
        ]

        # Add custom columns from the schema
        custom_columns = [
            Column(column_name, self._get_sqlalchemy_type(mysql_type))
            for column_name, mysql_type in schema.items()
        ]

        # Optional composite secondary index over schema columns (#1054 WS1:
        # (sequence_id, timestamp) for the grouped time-series reads). Fail
        # fast on a column that isn't in the schema — MySQL would raise a raw
        # 1072 at CREATE TABLE otherwise.
        table_args = []
        if index_columns:
            missing = sorted(set(index_columns) - set(schema))
            if missing:
                raise ValueError(
                    f"index_columns {missing} are not in the table schema; a "
                    f"secondary index can only cover schema columns."
                )
            # MySQL caps identifier length at 64; index names are per-table,
            # so truncation cannot collide across tables.
            index_name = f"ix_{table_name}_{'_'.join(index_columns)}"[:64]
            table_args.append(Index(index_name, *index_columns))

        # Combine standard and custom columns
        table = Table(
            table_name, self.metadata, *(standard_columns + custom_columns + table_args)
        )
        self.tables[table_name] = table

        # Create table if it doesn't exist
        self.metadata.create_all(self.engine, tables=[table])
        return table

    def insert_batch(
        self, table_name: str, records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Insert or update batch of records based on data_id

        Args:
            table_name: Name of the target table
            records: List of records to insert/update

        Returns:
            Dictionary containing:
            - success_ids: List of successfully processed record IDs
            - failures: List of dictionaries containing failed records and their error messages
        """
        if not records:
            # Return the same (success_ids, failures) tuple shape as the
            # non-empty path so callers can always unpack two values —
            # BaseIngestor._process_batch does ``ids, failures = insert_batch(...)``.
            return [], []

        table = self.tables[table_name]
        result = {"success_ids": [], "failures": []}

        try:
            with self.engine.connect() as connection:
                current_time = datetime.now()
                processed_records = []

                # BLOB/LONGBLOB columns need bytes at bind time — CSV/JSON
                # cells arrive as str, and SQLAlchemy raises StatementError
                # (TypeError) on every such row (Bugbot on #330: the blob
                # example could never actually ingest). Encode once here so
                # both ingestion paths are covered.
                blob_columns = {
                    c.name
                    for c in table.columns
                    if isinstance(c.type, (BLOB, LONGBLOB, LargeBinary))
                }
                for record in records:
                    processed_record = {
                        **record,
                        "updated_at": current_time,
                    }

                    if "created_at" not in record:
                        processed_record["created_at"] = current_time

                    for col in blob_columns:
                        val = processed_record.get(col)
                        if isinstance(val, str):
                            processed_record[col] = val.encode("utf-8")

                    processed_records.append(processed_record)

                # Create an "INSERT ... ON DUPLICATE KEY UPDATE" statement.
                # Build the VALUES(...) RHS as a backtick-quoted raw fragment.
                #
                # The previous f-string ``VALUES({column.name})`` left the name
                # unquoted, so any header with a character MySQL treats as an
                # operator — proteomics ``UniProt|gene`` columns like
                # ``P01033|TIMP1`` or isoform names like ``P02751-1|FN1`` —
                # produced 1064 (syntax error) and failed the whole batch.
                #
                # The natural SQLAlchemy alternative ``insert_stmt.inserted[
                # column.name]`` looks right but, against MySQL 8, the dialect
                # compiles it as the row-alias form ``AS new ... new.col`` —
                # which *requires* every referenced column to appear in the
                # INSERT column list and breaks any batch whose records don't
                # supply every column on the table (e.g. created_at-only
                # rows). Sticking with the legacy ``VALUES(`col`)`` syntax
                # preserves the prior behaviour (works regardless of which
                # columns the row actually has) while fixing the quoting bug.
                # Embedded backticks in the name are doubled (MySQL identifier
                # escape rule, mirrors the CREATE TABLE DDL path). Without
                # that, a header containing a literal backtick would close
                # the quoted identifier early and either break SQL parsing or
                # silently alter the statement. Pipe / dash / dot headers
                # worked because they carry no backtick; this guards the
                # residual case bugbot flagged on #190 (the fix was authored
                # in #191 but dropped by the squash-merge — re-applying).
                insert_stmt = insert(table)
                update_dict = {
                    column.name: text(
                        f"VALUES(`{column.name.replace('`', '``')}`)"
                    )
                    for column in table.columns
                    if column.name not in ["id", "created_at", "data_id"]
                }

                try:
                    # Execute upsert. Wrapped in _execute_with_retry so a
                    # transient MySQL hiccup (server-gone-away, lost
                    # connection mid-query, brief deadlock, network blip)
                    # is retried (3 attempts, exponential backoff) before
                    # the per-record fallback below takes over. Permanent
                    # errors (IntegrityError, DataError) bypass the retry
                    # and fall straight through to the per-record path
                    # which can identify the offending row.
                    _execute_with_retry(
                        connection,
                        insert_stmt.values(processed_records).on_duplicate_key_update(
                            **update_dict
                        ),
                    )
                    connection.commit()

                    # Get IDs for successfully processed records
                    data_ids = [record["data_id"] for record in records]
                    select_stmt = table.select().where(table.c.data_id.in_(data_ids))
                    rows = _execute_with_retry(connection, select_stmt).fetchall()
                    result["success_ids"] = [row.id for row in rows]

                except Exception as e:
                    # If batch insert fails, try one by one to identify problematic records
                    connection.rollback()
                    logger.warning(
                        f"Batch insert failed, attempting individual inserts: "
                        f"{redaction.safe_db_error(e)}"
                    )

                    for record in processed_records:
                        try:
                            stmt = insert_stmt.values([record]).on_duplicate_key_update(
                                **update_dict
                            )
                            _execute_with_retry(connection, stmt)
                            connection.commit()

                            # Get ID for the successful record
                            select_stmt = table.select().where(
                                table.c.data_id == record["data_id"]
                            )
                            row = _execute_with_retry(
                                connection, select_stmt
                            ).fetchone()
                            if row:
                                result["success_ids"].append(row.id)

                        except Exception as individual_error:
                            result["failures"].append(
                                {
                                    "record": record,
                                    "error": redaction.safe_db_error(
                                        individual_error
                                    ),
                                }
                            )
                            connection.rollback()
                            logger.error(
                                f"Failed to process record {record['data_id']}: "
                                f"{redaction.safe_db_error(individual_error)}"
                            )

        except Exception as e:
            logger.error(
                f"Database connection error in insert_batch: "
                f"{redaction.safe_db_error(e)}"
            )
            result["failures"].extend(
                [
                    {
                        "record": record,
                        "error": f"Database connection error: "
                        f"{redaction.safe_db_error(e)}",
                    }
                    for record in records
                ]
            )

        return result["success_ids"], result["failures"]

    SALT_TABLE = "tracebloc_ingest_meta"

    def get_or_create_table_salt(self, table_name: str) -> str:
        """
        Return the per-table salt for content-hash ``data_id`` derivation
        (#225), creating it atomically on first use.

        The salt is 32 random bytes (hex) stored ONLY in the cluster's MySQL
        — it never appears in any payload that leaves the cluster. Salting
        per table means identical content in two tables (or two clusters)
        yields unrelated ids, so the opaque sample ids in the ingest summary
        can't be correlated across datasets, while a retried run on the SAME
        table reproduces its ids exactly (the point of #225).

        Concurrency-safe without the table lock: ``INSERT IGNORE`` makes the
        create atomic — the loser of a race simply reads the winner's salt.
        """
        with self.engine.connect() as connection:
            _execute_with_retry(
                connection,
                text(
                    f"CREATE TABLE IF NOT EXISTS `{self.SALT_TABLE}` ("
                    "  table_name VARCHAR(64) NOT NULL PRIMARY KEY,"
                    "  salt CHAR(64) NOT NULL,"
                    "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                    ")"
                ),
            )
            _execute_with_retry(
                connection,
                text(
                    f"INSERT IGNORE INTO `{self.SALT_TABLE}` (table_name, salt) "
                    "VALUES (:table_name, :salt)"
                ).bindparams(table_name=table_name, salt=secrets.token_hex(32)),
            )
            connection.commit()
            row = _execute_with_retry(
                connection,
                text(
                    f"SELECT salt FROM `{self.SALT_TABLE}` "
                    "WHERE table_name = :table_name"
                ).bindparams(table_name=table_name),
            ).fetchone()
        if row is None:  # pragma: no cover — insert+select on one connection
            raise RuntimeError(
                f"Could not create or read the content-hash salt for "
                f"table {table_name!r}."
            )
        return row[0]

    # ── Run journal: orphan-row reconciliation (backend#1028 item 2) ────────
    #
    # The #227 compensating delete removes a failed run's rows only when the
    # failure is CAUGHT. A hard kill (OOMKilled / SIGKILL mid-ingest) never
    # reaches that except-branch, so the dead run's rows stay in the table
    # while its dataset was never registered — and the k8s Job retry then
    # ingests the same source next to them under fresh data_ids, duplicating
    # every row. "Registered" exists only in the central backend
    # (send_ingest_summary), which exposes no lookup, so this cluster-local
    # journal records each run's lifecycle in the same MySQL the rows land in:
    #
    #   record_ingest_started   — journaled before the run's first row insert
    #   mark_ingest_registered  — right after send_ingest_summary returns
    #   reclaim_dead_run_rows   — at the start of every ingest: delete rows
    #                             whose ingestor_id was journaled as started
    #                             but never registered (a dead prior attempt)
    #
    # Rows are reclaimed ONLY when the journal witnessed their run start and
    # never saw it register. Rows that predate the journal (legacy ingests —
    # registered or not) have no started-entry and are never touched, so
    # shipping this cannot delete rows of any pre-existing dataset.

    RUNS_TABLE = "tracebloc_ingest_runs"

    def _ensure_runs_table(self, connection) -> None:
        """Create the run-journal table if missing, then add the ``task``
        column to a table created before it existed. Idempotent DDL, mirroring
        the salt store's lazy creation (#225) — no migration step needed."""
        _execute_with_retry(
            connection,
            text(
                f"CREATE TABLE IF NOT EXISTS `{self.RUNS_TABLE}` ("
                "  ingestor_id VARCHAR(64) NOT NULL PRIMARY KEY,"
                "  table_name VARCHAR(64) NOT NULL,"
                "  registered TINYINT(1) NOT NULL DEFAULT 0,"
                "  task VARCHAR(64) NULL,"
                "  started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                "  registered_at TIMESTAMP NULL DEFAULT NULL,"
                "  KEY ix_tracebloc_ingest_runs_table (table_name)"
                ")"
            ),
        )
        self._ensure_runs_task_column(connection)

    def _runs_has_task_column(self, connection) -> bool:
        """Whether the runs table already carries the ``task`` column."""
        return bool(
            _execute_with_retry(
                connection,
                text(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() "
                    "AND table_name = :t AND column_name = 'task'"
                ).bindparams(t=self.RUNS_TABLE),
            ).scalar()
        )

    def _ensure_runs_task_column(self, connection) -> None:
        """Add ``task`` to a runs table created before the column existed
        (clusters that journalled runs under the pre-task schema). MySQL has no
        portable ``ADD COLUMN IF NOT EXISTS``, so check information_schema first
        and ALTER only when it's missing; a concurrent ingestor winning the race
        surfaces as a duplicate-column error, which is re-checked and swallowed.
        New tables already carry the column, so this is a no-op there."""
        if self._runs_has_task_column(connection):
            return
        try:
            # Not via _execute_with_retry: a duplicate-column failure is
            # deterministic, so retrying it would only burn the backoff before
            # the re-check below handles the race.
            connection.execute(
                text(
                    f"ALTER TABLE `{self.RUNS_TABLE}` "
                    "ADD COLUMN task VARCHAR(64) NULL"
                )
            )
        except DBAPIError:
            # SQLAlchemy leaves the connection in a pending-rollback state
            # after the failed ALTER, so the re-check below would raise
            # PendingRollbackError instead of running — the same #219 failure
            # mode _execute_with_retry guards against. Reset the transactional
            # state first; then re-check. A lost race (a concurrent ingestor
            # added the column) is swallowed; any other ALTER failure re-raises.
            try:
                connection.rollback()
            except Exception as rb_exc:
                # A rollback on a truly dead connection may itself fail; let
                # the re-check (or the re-raise) surface the real problem.
                logger.debug(f"connection.rollback() failed after ALTER: {rb_exc}")
            if not self._runs_has_task_column(connection):
                raise

    def record_ingest_started(
        self, table_name: str, ingestor_id: str, task: Optional[str] = None
    ) -> None:
        """Journal that *ingestor_id* is about to insert rows into
        *table_name* (backend#1028 item 2), tagging the run with its *task*
        (the ingest category, e.g. ``image_classification``).

        The task is otherwise known only to the backend; recording it here lets
        a cluster-local reader (``tb data list``) report each dataset's real
        task instead of inferring it. ``task`` is optional so an older caller
        keeps working (the run is journalled with a NULL task).

        Must be called before the run's first ``insert_batch`` so that a hard
        kill at ANY later point leaves a started-but-unregistered journal
        entry behind — the marker :meth:`reclaim_dead_run_rows` uses to
        recognise the dead run's rows on the next attempt. ``INSERT IGNORE``
        keeps it idempotent (re-entry with the same ingestor_id is a no-op).
        """
        with self.engine.connect() as connection:
            self._ensure_runs_table(connection)
            _execute_with_retry(
                connection,
                text(
                    f"INSERT IGNORE INTO `{self.RUNS_TABLE}` "
                    "(ingestor_id, table_name, task) "
                    "VALUES (:ingestor_id, :table_name, :task)"
                ).bindparams(ingestor_id=ingestor_id, table_name=table_name, task=task),
            )
            connection.commit()

    def mark_ingest_registered(self, table_name: str, ingestor_id: str) -> None:
        """Flip the run's journal entry to REGISTERED (backend#1028 item 2).

        Called immediately BEFORE ``send_ingest_summary`` (not after): the
        local flip and the remote registration can't be atomic, and writing
        the journal first makes a crash in that window a recoverable duplicate
        instead of a deletion of registered rows — see ``BaseIngestor.ingest``
        and :meth:`mark_ingest_unregistered` (the failure-path undo). Once set,
        no future reconcile pass can mistake this run's rows for orphans.
        Idempotent (repeating the UPDATE is a no-op).
        """
        with self.engine.connect() as connection:
            self._ensure_runs_table(connection)
            _execute_with_retry(
                connection,
                text(
                    f"UPDATE `{self.RUNS_TABLE}` "
                    "SET registered = 1, registered_at = CURRENT_TIMESTAMP "
                    "WHERE ingestor_id = :ingestor_id "
                    "AND table_name = :table_name"
                ).bindparams(ingestor_id=ingestor_id, table_name=table_name),
            )
            connection.commit()

    def mark_ingest_unregistered(self, table_name: str, ingestor_id: str) -> None:
        """Flip the run's journal entry back to NOT-registered (backend#1028
        item 2).

        The registration flip is written BEFORE ``send_ingest_summary`` so a
        crash in that window degrades to a recoverable duplicate rather than a
        deletion of registered rows (see ``BaseIngestor.ingest``). When the
        summary call then FAILS, that optimistic flip must be undone so the
        run's rows read as reclaimable again — otherwise a compensating-delete
        failure would strand them as a ``registered = 1`` entry that no future
        reconcile pass would ever clean. Idempotent (repeating is a no-op); the
        caller invokes it best-effort on the failure path.
        """
        with self.engine.connect() as connection:
            self._ensure_runs_table(connection)
            _execute_with_retry(
                connection,
                text(
                    f"UPDATE `{self.RUNS_TABLE}` "
                    "SET registered = 0, registered_at = NULL "
                    "WHERE ingestor_id = :ingestor_id "
                    "AND table_name = :table_name"
                ).bindparams(ingestor_id=ingestor_id, table_name=table_name),
            )
            connection.commit()

    def reclaim_dead_run_rows(
        self, table_name: str, current_ingestor_id: str
    ) -> Dict[str, int]:
        """Reconcile-on-start (backend#1028 item 2): delete rows left in
        *table_name* by prior attempts that died without registering.

        A prior run's rows are orphans exactly when its ``ingestor_id``
        (i) still owns rows in the table, (ii) was journaled as STARTED, and
        (iii) was never journaled as REGISTERED — i.e. the run began
        inserting and then died hard (OOMKilled / SIGKILL) before it could
        register, bypassing the #227 compensating delete. Requiring the
        started-entry means rows that predate the journal are NEVER touched,
        and (iii) excludes every registered run, so no registered dataset's
        rows can be deleted. ``current_ingestor_id`` is excluded so the pass
        is idempotent and safe to re-run at any point of the current ingest.

        Runs under the caller's table lock (``BaseIngestor.ingest``), so a
        journaled-started run for this table cannot still be live. Reuses
        :meth:`delete_by_ingestor_id` per dead run (transient-retry wrapped,
        logs each count).

        Returns:
            ``{dead_ingestor_id: rows_deleted}`` — empty when there is
            nothing to reclaim.
        """
        safe_table = table_name.replace("`", "``")
        with self.engine.connect() as connection:
            self._ensure_runs_table(connection)
            rows = _execute_with_retry(
                connection,
                text(
                    f"SELECT DISTINCT d.ingestor_id "
                    f"FROM `{safe_table}` d "
                    f"JOIN `{self.RUNS_TABLE}` r "
                    f"ON r.ingestor_id = d.ingestor_id "
                    f"WHERE r.table_name = :table_name "
                    f"AND r.registered = 0 "
                    f"AND r.ingestor_id != :current_ingestor_id"
                ).bindparams(
                    table_name=table_name,
                    current_ingestor_id=current_ingestor_id,
                ),
            ).fetchall()
        reclaimed: Dict[str, int] = {}
        for (dead_id,) in rows:
            reclaimed[dead_id] = self.delete_by_ingestor_id(table_name, dead_id)
        if reclaimed:
            logger.warning(
                f"Reclaimed {sum(reclaimed.values())} orphan row(s) in "
                f"`{table_name}` from {len(reclaimed)} dead unregistered "
                f"run(s) {sorted(reclaimed)} — left by a previous attempt "
                f"killed before it could register or clean up "
                f"(backend#1028); this run now converges instead of "
                f"duplicating them."
            )
        return reclaimed

    def list_registered_runs(self) -> List[Dict[str, str]]:
        """Return the REGISTERED ingest runs journalled on this client, one per
        dataset: ``[{"ingestor_id", "table_name", "task"}, ...]``.

        The run journal (``record_ingest_started`` / ``mark_ingest_registered``)
        is the authoritative local list of datasets this edge has ingested and
        successfully registered with the backend — exactly the set the
        pre-cutover metadata backfill sweeps. Only ``registered = 1`` rows are
        returned, so a started-but-never-registered (dead) run is never
        backfilled. ``task`` is the recorded category (may be NULL on runs
        journalled before the column existed); the backfill runner re-reads the
        authoritative category from the backend anyway.
        """
        with self.engine.connect() as connection:
            self._ensure_runs_table(connection)
            rows = _execute_with_retry(
                connection,
                text(
                    f"SELECT ingestor_id, table_name, task "
                    f"FROM `{self.RUNS_TABLE}` WHERE registered = 1 "
                    f"ORDER BY started_at"
                ),
            ).fetchall()
        return [
            {"ingestor_id": iid, "table_name": tname, "task": task}
            for (iid, tname, task) in rows
        ]

    def list_dataset_ingestor_ids(self) -> List[Dict[str, str]]:
        """Discover every dataset table directly and return its distinct
        ``ingestor_id``\\ s: ``[{"ingestor_id", "table_name"}, ...]``.

        This is the enumeration the metadata backfill sweeps by default, in
        preference to :meth:`list_registered_runs`. The pre-cutover backlog the
        backfill exists for was ingested BEFORE the run journal existed, so those
        datasets have no journal row — but their tables still carry the framework
        ``ingestor_id`` column. Scanning for that column finds them; the journal
        would miss them entirely.

        A dataset table is any table in this schema with an ``ingestor_id``
        column, minus the framework bookkeeping tables (the run journal — which
        also has the column — and the salt store). Only non-null, non-empty
        ``ingestor_id`` values are returned; a table appended by several runs
        yields one pair per distinct id (the caller backfills the table once).
        """
        framework_tables = {self.SALT_TABLE, self.RUNS_TABLE}
        with self.engine.connect() as connection:
            table_rows = _execute_with_retry(
                connection,
                text(
                    "SELECT DISTINCT table_name FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND column_name = 'ingestor_id'"
                ),
            ).fetchall()

        pairs: List[Dict[str, str]] = []
        for (table_name,) in table_rows:
            if table_name in framework_tables:
                continue
            # Identifier interpolated (bound params can't name a table); the name
            # comes from information_schema on our own DB, and backticks are
            # escaped, so it is not attacker-controlled free text.
            safe_table = table_name.replace("`", "``")
            # Per-table guard: a single unreadable table (timeout on a large
            # unindexed scan, a table dropped mid-sweep, a permissions gap) must
            # NOT abort discovery — otherwise every other table is skipped and the
            # sweep's "one bad table can't stop the rollout" guarantee is lost.
            # Log the table name + exception TYPE only (never str(exc): a driver
            # message can embed cell values, and this feeds install logs).
            try:
                with self.engine.connect() as connection:
                    id_rows = _execute_with_retry(
                        connection,
                        text(
                            f"SELECT DISTINCT ingestor_id FROM `{safe_table}` "
                            f"WHERE ingestor_id IS NOT NULL AND ingestor_id <> ''"
                        ),
                    ).fetchall()
            except Exception as exc:  # noqa: BLE001 — skip the table, continue the sweep
                logger.warning(
                    "list_dataset_ingestor_ids: skipping table %s — could not read "
                    "ingestor_ids (%s)",
                    table_name,
                    type(exc).__name__,
                )
                continue
            for (ingestor_id,) in id_rows:
                pairs.append({"ingestor_id": ingestor_id, "table_name": table_name})
        return pairs

    def get_label_counts(self, table_name: str, ingestor_id: str) -> Dict[str, int]:
        """
        Return ``{label: row_count}`` for every label inserted by *ingestor_id*.

        Used to build the summary payload sent to the backend after all records
        have been committed, giving an accurate count that excludes any rows
        that failed DB insertion.

        Args:
            table_name: Name of the table to query
            ingestor_id: UUID of the current ingest run

        Returns:
            Dict mapping label string to integer row count
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"SELECT label, COUNT(*) AS cnt "
                    f"FROM `{table_name}` "
                    f"WHERE ingestor_id = :ingestor_id "
                    f"GROUP BY label"
                ),
                {"ingestor_id": ingestor_id},
            ).fetchall()
        counts: Dict[str, int] = {}
        for label, cnt in rows:
            key = label if label is not None else ""
            counts[key] = counts.get(key, 0) + cnt
        return counts

    def get_label_sequence_counts(
        self, table_name: str, ingestor_id: str, group_column: str = "sequence_id"
    ) -> Dict[str, int]:
        """
        Return ``{label: sequence_count}`` for every label inserted by
        *ingestor_id*, counting DISTINCT sequences rather than rows
        (backend#1054 WS1, Decision-3/T2).

        The sequence-grouped categories' sample unit is one SEQUENCE (many
        timestep rows sharing a ``sequence_id``), so the summary payload —
        which feeds ``data_per_class``, the sub-dataset arithmetic and the
        leaderboard normalizers downstream — must count sequences. Using the
        row-unit :meth:`get_label_counts` here would inflate every count by
        ~mean(T)×. The label is constant within a sequence (validated
        pre-ingest), so grouping by label cannot split a sequence across two
        labels.

        Args:
            table_name: Name of the table to query
            ingestor_id: UUID of the current ingest run
            group_column: The sequence group column (default ``sequence_id``,
                from the category's ``ModalitySpec.grouping`` trait)

        Returns:
            Dict mapping label string to number of distinct sequences
        """
        safe_group = group_column.replace("`", "``")
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"SELECT label, COUNT(DISTINCT `{safe_group}`) AS cnt "
                    f"FROM `{table_name.replace('`', '``')}` "
                    f"WHERE ingestor_id = :ingestor_id "
                    f"GROUP BY label"
                ),
                {"ingestor_id": ingestor_id},
            ).fetchall()
        counts: Dict[str, int] = {}
        for label, cnt in rows:
            key = label if label is not None else ""
            counts[key] = counts.get(key, 0) + cnt
        return counts

    def delete_sequences(
        self,
        table_name: str,
        ingestor_id: str,
        sequence_ids: List[str],
        group_column: str = "sequence_id",
    ) -> int:
        """
        Group-integrity compensating delete (backend#1054 WS1, T5): remove
        every row of the given sequences inserted by this ingest run.

        The ingest loop drops individual failed rows and continues — correct
        for per-row categories, but for sequence-grouped data it silently
        persists sequences with MISSING TIMESTEPS: the training side would
        read a truncated vital-signs series as if it were complete. The
        post-insert group-integrity pass in ``ingestors/base.py`` collects
        the sequence ids touched by any dropped/failed row and removes those
        sequences' surviving rows here, so a sequence is stored either whole
        or not at all. Scoped to ``ingestor_id`` like the #227 compensating
        delete, so other runs' rows are never touched.

        Args:
            table_name: Name of the table to clean
            ingestor_id: UUID of the current ingest run
            sequence_ids: The sequence ids whose rows must be removed
            group_column: The sequence group column (default ``sequence_id``)

        Returns:
            Number of rows removed
        """
        if not sequence_ids:
            return 0
        safe_group = group_column.replace("`", "``")
        stmt = text(
            f"DELETE FROM `{table_name.replace('`', '``')}` "
            f"WHERE ingestor_id = :ingestor_id "
            f"AND `{safe_group}` IN :sequence_ids"
        ).bindparams(
            # expanding=True turns the IN into per-value binds — required for
            # a list parameter with SQLAlchemy text().
            bindparam("sequence_ids", value=list(sequence_ids), expanding=True),
            ingestor_id=ingestor_id,
        )
        with self.engine.connect() as connection:
            result = _execute_with_retry(connection, stmt)
            connection.commit()
        deleted = result.rowcount if result is not None else 0
        logger.warning(
            f"Group-integrity pass removed {deleted} row(s) across "
            f"{len(sequence_ids)} partial sequence(s) for "
            f"ingestor_id={ingestor_id!r} from `{table_name}` (T5: a "
            f"sequence is stored whole or not at all)."
        )
        return deleted

    def delete_by_ingestor_id(self, table_name: str, ingestor_id: str) -> int:
        """
        Compensating delete (#227): remove every row a single ingest run
        inserted, identified by its per-process ``ingestor_id``.

        Called from the ingestion failure path when the run will NOT register
        its dataset with the backend. Rows commit per batch during the ingest
        loop, so without this a failed run leaves rows that no consumer can
        reach (training queries are scoped to REGISTERED ingestor_ids) but
        that inflate the heartbeat's availability report and occupy disk
        with no remote delete path (the live wound in #336).

        Staged files are deliberately left in place: they are keyed by source
        filename and idempotently overwritten on re-run.

        Uses ``_execute_with_retry`` so a transient MySQL hiccup does not
        leave a partial cleanup; a permanent failure propagates to the caller,
        which logs it loudly while preserving the ORIGINAL ingestion error.

        Args:
            table_name: Name of the table to clean
            ingestor_id: UUID of the failed ingest run

        Returns:
            Number of rows removed
        """
        with self.engine.connect() as connection:
            result = _execute_with_retry(
                connection,
                text(
                    f"DELETE FROM `{table_name.replace('`', '``')}` "
                    f"WHERE ingestor_id = :ingestor_id"
                ).bindparams(ingestor_id=ingestor_id),
            )
            connection.commit()
        deleted = result.rowcount if result is not None else 0
        logger.info(
            f"Removed {deleted} unregistered row(s) for "
            f"ingestor_id={ingestor_id!r} from `{table_name}`."
        )
        return deleted

    def get_samples(
        self, table_name: str, ingestor_id: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Return a small sample of records for the given ingest run.

        These are stored on the backend as ``UserDataSet.data_samples`` and
        displayed in the dataset preview UI.

        Args:
            table_name: Name of the table to query
            ingestor_id: UUID of the current ingest run
            limit: Maximum number of sample rows to return (default 10)

        Returns:
            List of ``{"data_id": ..., "label": ...}`` dicts
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"SELECT data_id, label "
                    f"FROM `{table_name}` "
                    f"WHERE ingestor_id = :ingestor_id "
                    f"LIMIT :limit"
                ),
                {"ingestor_id": ingestor_id, "limit": limit},
            ).fetchall()
        return [{"data_id": row[0], "label": row[1] if row[1] is not None else ""} for row in rows]

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        Returns the schema of a table as a dictionary mapping column names to their MySQL types.
        Args:
            table_name: Name of the table to inspect

        Returns:
            Dictionary of column names and their MySQL types
        """

        inspector = inspect(self.engine)

        # Get all columns from the table
        columns = inspector.get_columns(table_name)

        # Reflection against a live MySQL returns DIALECT type classes whose
        # names already ARE the MySQL keywords (VARCHAR, INTEGER, DECIMAL,
        # TINYINT, ...), while in-process metadata uses the GENERIC classes
        # (String, Integer, Numeric, ...). Upper-casing the class name unifies
        # the two; this map only covers names that differ from the platform's
        # MySQL vocabulary (the keywords _get_sqlalchemy_type accepts).
        type_mapping = {
            "STRING": "VARCHAR",
            "INTEGER": "INT",
            "BIGINTEGER": "BIGINT",
            "SMALLINTEGER": "SMALLINT",
            "NUMERIC": "DECIMAL",
            "DOUBLE_PRECISION": "DOUBLE",
            "LARGEBINARY": "BLOB",
        }

        schema = {}
        for column in columns:
            col_type = column["type"]
            type_name = col_type.__class__.__name__.upper()

            # MySQL has no native BOOLEAN type: BOOL columns are created — and
            # therefore reflected — as TINYINT(1).
            if (
                type_name == "TINYINT"
                and getattr(col_type, "display_width", None) == 1
            ):
                schema[column["name"]] = "BOOLEAN"
                continue

            mysql_type = type_mapping.get(type_name, type_name)

            # Re-attach the parametrisation MySQL carries in the DDL string.
            if mysql_type in ("VARCHAR", "CHAR", "BINARY", "VARBINARY"):
                length = getattr(col_type, "length", None)
                if length:
                    mysql_type = f"{mysql_type}({length})"
            elif mysql_type == "DECIMAL":
                precision = getattr(col_type, "precision", None)
                if precision is not None:
                    scale = getattr(col_type, "scale", None) or 0
                    mysql_type = f"DECIMAL({precision},{scale})"

            schema[column["name"]] = mysql_type

        return schema
