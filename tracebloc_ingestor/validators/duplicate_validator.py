"""Duplicate Validator Module.

This module provides validation to check if the destination directory exists,
raising errors if it does to prevent accidental overwrites.
"""

import os
from pathlib import Path
from typing import Any, List, Optional
import logging

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class DuplicateValidator(BaseValidator):
    """
    This validator checks if the destination directory already exists.

    It raises errors if the directory already exists,
    preventing accidental overwrites.

    Attributes:
        dest_path: Destination path to check
    """

    def __init__(
        self, dest_path: Optional[str] = None, name: str = "Duplicate Validator"
    ):
        """Initialize the duplicate validator.

        Args:
            dest_path: Destination path to check (defaults to config.DEST_PATH)
            name: Human-readable name of the validator
        """
        super().__init__(name)
        self.dest_path = dest_path or config.DEST_PATH

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate directory existence.

        Args:
            data: Not used, but required by base class
            **kwargs: Additional validation parameters

        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            errors = []
            warnings = []
            metadata = {"dest_path": self.dest_path, "directory_exists": False}

            # Check destination directory existence
            directory_exists = self._check_directory_exists()
            metadata["directory_exists"] = directory_exists

            if directory_exists:
                errors.append(
                    f"Destination directory '{self.dest_path}' already exists"
                )

            # Check if parent directory exists (for creating the destination)
            parent_dir = Path(self.dest_path).parent
            parent_exists = parent_dir.exists()
            metadata["parent_directory_exists"] = parent_exists

            if not parent_exists:
                warnings.append(
                    f"Parent directory '{parent_dir}' does not exist and will be created"
                )

            is_valid = len(errors) == 0

            return self._create_result(
                is_valid=is_valid, errors=errors, warnings=warnings, metadata=metadata
            )

        except Exception as e:
            logger.error(f"Error during duplicate validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Duplicate validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )

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
        """
        Create the destination directory if it doesn't exist.

        Returns:
            True if directory was created or already exists, False otherwise
        """
        try:
            dest_path = Path(self.dest_path)
            if not dest_path.exists():
                dest_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created destination directory: {self.dest_path}")
            return True
        except Exception as e:
            logger.error(f"Error creating directory: {str(e)}")
            return False
