"""Per-modality specification registry — the single source of truth for each
task category's ingestion behavior (structural refactor — backend#796, P3)."""

from .layout import Grouping
from .registry import (
    FILE_BEARING_CATEGORIES,
    NLP_CATEGORIES,
    REGISTRY,
    SELF_SUPERVISED_CATEGORIES,
    TABULAR_FAMILY_CATEGORIES,
    spec_for,
)
from .spec import ModalitySpec

__all__ = [
    "ModalitySpec",
    "Grouping",
    "REGISTRY",
    "spec_for",
    "FILE_BEARING_CATEGORIES",
    "TABULAR_FAMILY_CATEGORIES",
    "SELF_SUPERVISED_CATEGORIES",
    "NLP_CATEGORIES",
]
