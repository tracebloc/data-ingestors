"""Base Validator Module.

This module provides the base validator class and validation result data structures
for implementing data validation before ingestion.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from pathlib import Path
import json
import logging

from tqdm import tqdm

from tracebloc_ingestor.config import Config

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


@dataclass
class ValidationResult:
    """Data class to hold validation results.

    Attributes:
        is_valid: Whether the validation passed
        errors: List of validation error messages
        warnings: List of validation warning messages
        metadata: Additional validation metadata
    """

    is_valid: bool
    errors: List[str]
    warnings: List[str]
    metadata: Dict[str, Any]


class BaseValidator(ABC):
    """Base class for all data validators.

    This abstract base class provides the core functionality for validating data
    before ingestion. It defines the interface that all validators must implement.

    Attributes:
        validator_id: Unique identifier for this validator instance
        name: Human-readable name of the validator
    """

    # The run's resolved Config, injected by ``map_validators`` (which calls
    # ``bind_config`` on every validator it builds) — structural refactor
    # backend#796, P4b. Class-level default ``None`` so a validator constructed
    # outside the registry flow (e.g. directly in a test) keeps working: its
    # path-reading sites fall back to the module-global ``config`` while
    # ``_config`` is unset. This is the seam that lets the ingestor's
    # explicitly-resolved Config replace the env-driven global — the ``cli/run``
    # env-var bridge the globals relied on is removed in the P4c slice.
    _config: Optional[Config] = None

    def __init__(self, name: str):
        """Initialize the base validator.

        Args:
            name: Human-readable name of the validator
        """
        self.name = name
        self.validator_id = f"{name.lower().replace(' ', '_')}_validator"

    def bind_config(self, config: Config) -> None:
        """Bind the run's resolved :class:`Config` to this validator (P4b).

        Called by ``map_validators`` for every validator in a category's set so
        the path-reading validators (SRC_PATH / DEST_PATH / TABLE_NAME) read the
        ingestor's resolved Config instead of a module-global ``Config()`` that
        snapshots ``os.environ`` at import time.
        """
        self._config = config

    @abstractmethod
    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate the provided data.

        Args:
            data: The data to validate
            **kwargs: Additional validation parameters

        Returns:
            ValidationResult containing validation status and messages
        """
        pass

    def _create_result(
        self,
        is_valid: bool,
        errors: Optional[List[str]] = None,
        warnings: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        """Create a validation result object.

        Args:
            is_valid: Whether validation passed
            errors: List of error messages
            warnings: List of warning messages
            metadata: Additional metadata

        Returns:
            ValidationResult object
        """
        return ValidationResult(
            is_valid=is_valid,
            errors=errors or [],
            warnings=warnings or [],
            metadata=metadata or {},
        )

    def _create_progress_bar(self, total: int, desc: str = None) -> tqdm:
        """Create a progress bar for validation operations.

        Args:
            total: Total number of items to process
            desc: Description for the progress bar

        Returns:
            tqdm progress bar instance
        """

        progress_desc = desc or f"{self.name} - Validating"
        return tqdm(
            total=total,
            desc=progress_desc,
            unit="files",
            leave=False,
            ncols=100,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
        )

    def _load_data(self, data: Any) -> Optional["pd.DataFrame"]:
        """Load data as a DataFrame from a file path or pass through an existing DataFrame."""
        import pandas as pd

        try:
            if isinstance(data, pd.DataFrame):
                return data
            elif isinstance(data, (str, Path)):
                return pd.read_csv(data, encoding="utf-8", on_bad_lines="warn")
            return None
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            return None

    @staticmethod
    def _parse_json(row: Any, column: str) -> Optional[Any]:
        """Parse a JSON string from a DataFrame row column. Returns None on failure."""
        import pandas as pd

        try:
            value = row[column]
            if pd.isna(value):
                return None
            return json.loads(str(value))
        except (json.JSONDecodeError, TypeError, KeyError):
            return None

    def __str__(self) -> str:
        """String representation of the validator."""
        return f"{self.__class__.__name__}(name='{self.name}')"

    def __repr__(self) -> str:
        """Detailed string representation of the validator."""
        return (
            f"{self.__class__.__name__}(name='{self.name}', id='{self.validator_id}')"
        )
