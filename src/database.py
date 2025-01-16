from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, DateTime, text, Text, Integer, String, Float, Boolean, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.mysql import insert, LONGBLOB, BLOB
from urllib.parse import quote
from typing import List, Dict, Any, Optional
from datetime import datetime
from .config import Config
import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)


class Database:
    def __init__(self, config: Config):
        self.config = config
        self.engine = self._create_engine()
        self.metadata = MetaData()
        self.tables: Dict[str, Table] = {}
        self.unique_id_column: Optional[str] = None  # Store table-specific unique ID column mappings

    def _create_engine(self) -> Engine:
        connection_string = (
            f"mysql+mysqldb://{self.config.DB_USER}:{quote(self.config.DB_PASSWORD)}"
            f"@{self.config.DB_HOST}:{self.config.DB_PORT}/{self.config.DB_NAME}"
        )
        print(connection_string)
        return create_engine(connection_string, pool_pre_ping=True)

    def _get_sqlalchemy_type(self, mysql_type: str):
        type_mapping = {
            'VARCHAR': String,
            'TEXT': Text,
            'INT': Integer,
            'BIGINT': BigInteger,
            'FLOAT': Float,
            'BOOLEAN': Boolean,
            'DATETIME': DateTime,
            'TIMESTAMP': DateTime,
            'BLOB': BLOB,
            'LONGBLOB': LONGBLOB,
        }
        
        for sql_type, alchemy_type in type_mapping.items():
            if sql_type in mysql_type.upper():
                length = None
                if '(' in mysql_type:
                    length = int(mysql_type.split('(')[1].split(')')[0])
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
            Column('id', BigInteger, primary_key=True, autoincrement=True),
            Column('created_at', DateTime, server_default=text('CURRENT_TIMESTAMP')),
            Column('updated_at', DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')),
            Column('status', Integer, server_default=text('0')),  # 1 for active, 0 for inactive
            Column('label', String(255), nullable=True),
            Column('data_intent', String(100), nullable=True),
            Column('data_id', String(255), unique=True, nullable=False),
            Column('annotation', Text, nullable=True)
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

    def insert_batch(self, table_name: str, records: List[Dict[str, Any]]) -> List[int]:
        """
        Insert or update batch of records based on data_id
        
        Args:
            table_name: Name of the target table
            records: List of records to insert/update
            
        Returns:
            List of record IDs
        """
        if not records:
            return []

        table = self.tables[table_name]
        with self.engine.connect() as connection:
            current_time = datetime.now()
            processed_records = []
            
            for record in records:
                processed_record = {
                    **record,
                    'updated_at': current_time,
                }
                
                # Only set created_at for new records
                if 'created_at' not in record:
                    processed_record['created_at'] = current_time
                    
                processed_records.append(processed_record)

            # Create an "INSERT ... ON DUPLICATE KEY UPDATE" statement
            insert_stmt = insert(table)
            
            # Define which columns should be updated on duplicate
            update_dict = {
                column.name: text(f"VALUES({column.name})")
                for column in table.columns
                if column.name not in ['id', 'created_at', 'data_id']  # Don't update these fields
            }
            
            # Execute upsert
            result = connection.execute(
                insert_stmt.values(processed_records).on_duplicate_key_update(**update_dict)
            )
            connection.commit()

            # Get IDs for both inserted and updated records
            data_ids = [record['data_id'] for record in records]
            select_stmt = table.select().where(table.c.data_id.in_(data_ids))
            rows = connection.execute(select_stmt).fetchall()
            return [row.id for row in rows] 

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
            'String': 'VARCHAR',
            'Text': 'TEXT',
            'Integer': 'INT',
            'BigInteger': 'BIGINT',
            'Float': 'FLOAT',
            'Boolean': 'BOOLEAN',
            'DateTime': 'DATETIME',
            'BLOB': 'BLOB',
            'LONGBLOB': 'LONGBLOB'
        }
        
        schema = {}
        for column in columns:
            # Get the type name
            type_name = column['type'].__class__.__name__
            
            # Convert SQLAlchemy type to MySQL type
            mysql_type = type_mapping.get(type_name, 'VARCHAR')
            
            # Add length for VARCHAR types
            if mysql_type == 'VARCHAR' and hasattr(column['type'], 'length'):
                mysql_type = f"{mysql_type}({column['type'].length})"
            
            schema[column['name']] = mysql_type
        
        return schema 