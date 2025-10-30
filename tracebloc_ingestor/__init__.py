"""Tracebloc Data Ingestor Package.

A flexible and extensible framework for ingesting data from various sources into a database
and optionally sending it to an API. The package provides base classes for creating custom
ingestors, along with built-in support for common data formats.
"""

from .config import Config
from .database import Database
from .api.client import APIClient
from .ingestors import BaseIngestor, CSVIngestor, JSONIngestor
from .validators import (
    BaseValidator,
    ValidationResult,
    FileTypeValidator,
    ImageResolutionValidator,
    TableNameValidator,
)

__version__ = "0.1.0"

__all__ = [
    "Config",
    "Database",
    "APIClient",
    "BaseIngestor",
    "CSVIngestor",
    "JSONIngestor",
    "BaseValidator",
    "ValidationResult",
    "FileTypeValidator",
    "ImageResolutionValidator",
    "TableNameValidator",
]
