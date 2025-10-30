from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    BigInteger,
    DateTime,
    text,
    Text,
    Integer,
    String,
    Float,
    Boolean,
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
        type_mapping = {
            "VARCHAR": String,
            "TEXT": Text,
            "INT": Integer,
            "BIGINT": BigInteger,
            "FLOAT": Float,
            "BOOLEAN": Boolean,
            "DATETIME": DateTime,
            "TIMESTAMP": DateTime,
            "BLOB": BLOB,
            "LONGBLOB": LONGBLOB,
        }

        for sql_type, alchemy_type in type_mapping.items():
            if sql_type in mysql_type.upper():
                length = None
                if "(" in mysql_type:
                    length = int(mysql_type.split("(")[1].split(")")[0])
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
        """
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
            return {"success_ids": [], "failures": []}

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

                # Create an "INSERT ... ON DUPLICATE KEY UPDATE" statement
                insert_stmt = insert(table)
                update_dict = {
                    column.name: text(f"VALUES({column.name})")
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
            "Boolean": "BOOLEAN",
            "DateTime": "DATETIME",
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
