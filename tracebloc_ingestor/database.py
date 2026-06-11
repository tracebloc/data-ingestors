from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    BigInteger,
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
    inspect,

)
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.mysql import insert, LONGBLOB, BLOB
from sqlalchemy.exc import OperationalError, InterfaceError, DBAPIError
import logging
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
from .utils.logging import setup_logging

# Configure unified logging with config
config = Config()
setup_logging(config)
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
        engine = create_engine(base_connection_string, pool_pre_ping=True)

        with engine.connect() as connection:
            connection.execute(
                text(f"CREATE DATABASE IF NOT EXISTS {self.config.DB_NAME}")
            )
            connection.commit()

        # Now connect to the specific database
        connection_string = f"{base_connection_string}/{self.config.DB_NAME}"
        return create_engine(connection_string, pool_pre_ping=True)

    def _get_sqlalchemy_type(self, mysql_type: str):
        """Convert MySQL type to SQLAlchemy type.
        
        Extracts the base type (before parentheses) and matches exactly to avoid
        substring issues (e.g., "DATE" matching "DATETIME").
        """
        type_mapping = {
            "VARCHAR": String,
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

        raise ValueError(f"Unsupported MySQL type: {mysql_type}")

    def create_table(self, table_name: str, schema: Dict[str, str]):
        """
        Creates a table if it doesn't exist, or returns existing table

        Args:
            table_name: Name of the table
            schema: Dictionary defining the table schema

        Returns:
            SQLAlchemy Table object

        Raises:
            ValueError: if a schema column collides with a reserved/internal
                column (e.g. a user CSV with its own ``id``), which would
                otherwise surface as a cryptic SQLAlchemy DuplicateColumnError.
        """
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
        _MAX_IDENTIFIER = 64
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
            return self.tables[table_name]

        # Check if table exists in database
        inspector = inspect(self.engine)
        if table_name in inspector.get_table_names():
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

        # Combine standard and custom columns
        table = Table(table_name, self.metadata, *(standard_columns + custom_columns))
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

                for record in records:
                    processed_record = {
                        **record,
                        "updated_at": current_time,
                    }

                    if "created_at" not in record:
                        processed_record["created_at"] = current_time

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
                        f"Batch insert failed, attempting individual inserts: {str(e)}"
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
                                {"record": record, "error": str(individual_error)}
                            )
                            connection.rollback()
                            logger.error(
                                f"Failed to process record {record['data_id']}: {str(individual_error)}"
                            )

        except Exception as e:
            logger.error(f"Database connection error in insert_batch: {str(e)}")
            result["failures"].extend(
                [
                    {"record": record, "error": f"Database connection error: {str(e)}"}
                    for record in records
                ]
            )

        return result["success_ids"], result["failures"]

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

        # Convert SQLAlchemy types back to MySQL types
        type_mapping = {
            "String": "VARCHAR",
            "Text": "TEXT",
            "Integer": "INT",
            "BigInteger": "BIGINT",
            "Float": "FLOAT",
            "Double": "DOUBLE",
            "Boolean": "BOOLEAN",
            "Date": "DATE",
            "DateTime": "DATETIME",
            "Timestamp": "TIMESTAMP",
            "Time": "TIME",
            "BLOB": "BLOB",
            "LONGBLOB": "LONGBLOB",
        }

        schema = {}
        for column in columns:
            # Get the type name
            type_name = column["type"].__class__.__name__

            # Convert SQLAlchemy type to MySQL type
            mysql_type = type_mapping.get(type_name, "VARCHAR")

            # Add length for VARCHAR types
            if mysql_type == "VARCHAR" and hasattr(column["type"], "length"):
                mysql_type = f"{mysql_type}({column['type'].length})"

            schema[column["name"]] = mysql_type

        return schema
