"""Tracebloc data ingestion package.

This module contains all data ingestion related components:
- BaseIngestor: Abstract base class for all ingestors
- CSVIngestor: Specialized ingestor for CSV files
- JSONIngestor: Specialized ingestor for JSON files
- VOCIngestor: Pascal-VOC XML record enumerator for object detection
"""

from .base import BaseIngestor, IngestionSummary
from .csv_ingestor import CSVIngestor
from .json_ingestor import JSONIngestor
from .voc_ingestor import VOCIngestor

__all__ = [
    "BaseIngestor",
    "IngestionSummary",
    "CSVIngestor",
    "JSONIngestor",
    "VOCIngestor",
]
