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

config = Config()
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
        # Store the explicit override only; the config-backed default is
        # resolved lazily in the ``dest_path`` property so the run's injected
        # Config (bound by ``map_validators`` AFTER construction, P4b) wins.
        # Resolving here would snapshot the module-global DEST_PATH before the
        # bind and ignore the injected value.
        self._dest_path = dest_path

    @property
    def dest_path(self) -> str:
        """Destination directory: explicit override, else the bound Config's
        ``DEST_PATH`` (falling back to the module-global ``config`` when this
        validator was constructed outside the registry flow)."""
        return self._dest_path or (self._config or config).DEST_PATH

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
                # An empty directory typically means a previous ingestion
                # attempt aborted before any data landed. Reusing it is
                # safe and lets customers rerun without manual PVC cleanup.
                # A populated directory is treated as a real collision.
                is_empty = self._is_directory_empty()
                metadata["directory_empty"] = is_empty
                if is_empty:
                    warnings.append(
                        f"Destination directory '{self.dest_path}' exists but "
                        "is empty (likely from a previous failed run); reusing."
                    )
                else:
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

    def _is_directory_empty(self) -> bool:
        """Return True iff dest_path is an empty directory.

        Returns False on any error so the validator falls back to the
        existing "fail loudly" behavior rather than masking a real issue.
        """
        try:
            return not any(Path(self.dest_path).iterdir())
        except Exception as e:
            logger.error(f"Error checking if directory is empty: {str(e)}")
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
