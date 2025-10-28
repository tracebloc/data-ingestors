"""Tracebloc data ingestion package.

This module contains all data ingestion related components:
- BaseIngestor: Abstract base class for all ingestors
- CSVIngestor: Specialized ingestor for CSV files
- JSONIngestor: Specialized ingestor for JSON files
"""

from .base import BaseIngestor, IngestionSummary
from .csv_ingestor import CSVIngestor
from .json_ingestor import JSONIngestor

__all__ = ["BaseIngestor", "IngestionSummary", "CSVIngestor", "JSONIngestor"]
