"""Tracebloc Data Ingestor Package.

A flexible and extensible framework for ingesting data from various sources into a database
and optionally sending it to an API. The package provides base classes for creating custom
ingestors, along with built-in support for common data formats.
"""

from .config import Config
from .database import Database
from .api.client import APIClient
from .ingestors import BaseIngestor, CSVIngestor, JSONIngestor
from .utils.template_runner import run_ingestion
from .validators import (
    BaseValidator,
    ValidationResult,
    FileTypeValidator,
    ImageResolutionValidator,
    TableNameValidator,
)

# Single source of truth for the package version. setup.py parses this literal
# (see _read_version in setup.py) so the two can't drift again (#175). Bump here
# only — setup.py picks it up automatically.
__version__ = "0.3.9"

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
    "run_ingestion",
]
