"""Duplicate Validator Module.

This module provides validation to check if a table name already exists in the MySQL database
and if the destination parent directory exists, raising errors if they do.
"""

import os
from pathlib import Path
from typing import Any, List, Optional
import logging

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.logging import setup_logging

try:
    from sqlalchemy import create_engine, text, inspect
    from urllib.parse import quote
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    create_engine = None
    text = None
    inspect = None
    quote = None

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class DuplicateValidator(BaseValidator):
    """Validator for checking table and directory existence.
    
    This validator checks:
    1. If the table name already exists in the MySQL database
    2. If the destination parent directory already exists
    
    It raises errors if either the table or directory already exists,
    preventing accidental overwrites.
    
    Attributes:
        table_name: Name of the table to check
        dest_path: Destination path to check
    """
    
    def __init__(self, 
                 table_name: Optional[str] = None,
                 dest_path: Optional[str] = None,
                 skip_database_check: Optional[bool] = None,
                 name: str = "Duplicate Validator"):
        """Initialize the duplicate validator.
        
        Args:
            table_name: Name of the table to check (defaults to config.TABLE_NAME)
            dest_path: Destination path to check (defaults to config.DEST_PATH)
            skip_database_check: Skip database check (defaults to SKIP_DB_CHECK env var or False)
            name: Human-readable name of the validator
        """
        super().__init__(name)
        self.table_name = table_name or config.TABLE_NAME
        self.dest_path = dest_path or config.DEST_PATH
        self.skip_database_check = skip_database_check if skip_database_check is not None else os.getenv("SKIP_DB_CHECK", "false").lower() == "true"
    
    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate table and directory existence.
        
        Args:
            data: Not used, but required by base class
            **kwargs: Additional validation parameters
                
        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            if not SQLALCHEMY_AVAILABLE:
                return self._create_result(
                    is_valid=False,
                    errors=["SQLAlchemy not available. Cannot check table existence."],
                    metadata={'sqlalchemy_available': False}
                )
            
            errors = []
            warnings = []
            metadata = {
                'table_name': self.table_name,
                'dest_path': self.dest_path,
                'table_exists': False,
                'directory_exists': False
            }
            
            # Check table existence in MySQL database (if not skipped)
            if self.skip_database_check:
                table_exists = False
                metadata['table_exists'] = False
                metadata['table_check_skipped'] = True
                warnings.append("Database check skipped for local testing")
            else:
                table_check_result = self._check_table_exists()
                table_exists = table_check_result['exists']
                metadata['table_exists'] = table_exists
                metadata['table_check_error'] = table_check_result.get('error')
                
                if table_exists:
                    errors.append(f"Table '{self.table_name}' already exists in the database")
                elif table_check_result.get('error'):
                    # If we can't check the database, add a warning but don't fail validation
                    warnings.append(f"Could not check table existence: {table_check_result['error']}")
            
            # Check destination directory existence
            directory_exists = self._check_directory_exists()
            metadata['directory_exists'] = directory_exists
            
            if directory_exists:
                errors.append(f"Destination directory '{self.dest_path}' already exists")
            
            # Check if parent directory exists (for creating the destination)
            parent_dir = Path(self.dest_path).parent
            parent_exists = parent_dir.exists()
            metadata['parent_directory_exists'] = parent_exists
            
            if not parent_exists:
                warnings.append(f"Parent directory '{parent_dir}' does not exist and will be created")
            
            is_valid = len(errors) == 0
            
            return self._create_result(
                is_valid=is_valid,
                errors=errors,
                warnings=warnings,
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Error during duplicate validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Duplicate validation error: {str(e)}"],
                metadata={'error_type': 'validation_exception'}
            )
    
    def _check_table_exists(self) -> dict:
        """Check if the table exists in the MySQL database.
        
        Returns:
            Dictionary with 'exists' (bool) and optional 'error' (str)
        """
        try:
            # Create database connection
            connection_string = (
                f"mysql+mysqlconnector://{config.DB_USER}:{quote(config.DB_PASSWORD)}"
                f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
            )
            engine = create_engine(connection_string, pool_pre_ping=True)
            
            # Check if table exists
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            return {
                'exists': self.table_name in tables,
                'error': None
            }
            
        except Exception as e:
            error_msg = f"Database connection failed: {str(e)}"
            logger.warning(error_msg)
            # If we can't check, assume it doesn't exist to avoid false positives
            return {
                'exists': False,
                'error': error_msg
            }
    
    def _check_directory_exists(self) -> bool:
        """Check if the destination directory exists.
        
        Returns:
            True if directory exists, False otherwise
        """
        try:
            dest_path = Path(self.dest_path)
            return dest_path.exists() and dest_path.is_dir()
        except Exception as e:
            logger.error(f"Error checking directory existence: {str(e)}")
            return False
    
    def _create_directory_if_needed(self) -> bool:
        """Create the destination directory if it doesn't exist.
        
        Returns:
            True if directory was created or already exists, False otherwise
        """
        try:
            dest_path = Path(self.dest_path)
            dest_path.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"Error creating directory: {str(e)}")
            return False
