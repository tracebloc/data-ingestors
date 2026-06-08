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
    inspect,

)
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.mysql import insert, LONGBLOB, BLOB
import logging
from urllib.parse import quote
from typing import List, Dict, Any, Optional
from datetime import datetime
from .config import Config
from .utils.logging import setup_logging

# Configure unified logging with config
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)


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
            # Extract length for VARCHAR types
            length = None
            if "(" in mysql_type_upper:
                try:
                    length = int(mysql_type_upper.split("(")[1].split(")")[0])
                except (ValueError, IndexError):
                    pass
            return alchemy_type(length) if length else alchemy_type

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

        # Return existing table if already created
        if table_name in self.tables:
            return self.tables[table_name]

        # Check if table exists in database
        inspector = inspect(self.engine)
        if table_name in inspector.get_table_names():
            # Reflect existing table using MetaData
            self.metadata.reflect(self.engine, only=[table_name])
            table = self.metadata.tables[table_name]
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
                # Use insert_stmt.inserted[...] rather than a raw
                # text(f"VALUES({column.name})"): the f-string left the column
                # name unquoted in the VALUES() clause, so any header with a
                # special character (e.g. proteomics "UniProt|gene" columns
                # like `P01033|TIMP1`, or isoform names like `P02751-1|FN1`)
                # produced invalid SQL — MySQL parsed the `|`/`-` as operators
                # and raised 1064 (syntax error), failing the whole batch.
                # insert_stmt.inserted renders the column name backtick-quoted.
                insert_stmt = insert(table)
                update_dict = {
                    column.name: insert_stmt.inserted[column.name]
                    for column in table.columns
                    if column.name not in ["id", "created_at", "data_id"]
                }

                try:
                    # Execute upsert
                    connection.execute(
                        insert_stmt.values(processed_records).on_duplicate_key_update(
                            **update_dict
                        )
                    )
                    connection.commit()

                    # Get IDs for successfully processed records
                    data_ids = [record["data_id"] for record in records]
                    select_stmt = table.select().where(table.c.data_id.in_(data_ids))
                    rows = connection.execute(select_stmt).fetchall()
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
                            connection.execute(stmt)
                            connection.commit()

                            # Get ID for the successful record
                            select_stmt = table.select().where(
                                table.c.data_id == record["data_id"]
                            )
                            row = connection.execute(select_stmt).fetchone()
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
