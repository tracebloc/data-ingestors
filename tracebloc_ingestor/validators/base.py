"""Base Validator Module.

This module provides the base validator class and validation result data structures
for implementing data validation before ingestion.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import logging

from tqdm import tqdm

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.utils.logging import setup_logging

config = Config()
setup_logging(config)
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

    def __init__(self, name: str):
        """Initialize the base validator.

        Args:
            name: Human-readable name of the validator
        """
        self.name = name
        self.validator_id = f"{name.lower().replace(' ', '_')}_validator"

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

    def __str__(self) -> str:
        """String representation of the validator."""
        return f"{self.__class__.__name__}(name='{self.name}')"

    def __repr__(self) -> str:
        """Detailed string representation of the validator."""
        return (
            f"{self.__class__.__name__}(name='{self.name}', id='{self.validator_id}')"
        )
