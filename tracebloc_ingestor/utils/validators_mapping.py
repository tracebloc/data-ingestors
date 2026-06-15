"""Resolve the validator set for a task category.

The per-category validator factories now live on each ``ModalitySpec``
(bodies in ``modalities/validators.py``, attached in ``modalities/registry.py``
— structural refactor backend#796, P3b). This stays the entry point
``BaseIngestor.validate_data`` calls; it is now a thin lookup over the registry
rather than an 11-arm if/elif.

An unknown / ``None`` category returns ``[]`` (no validators) — preserving the
prior fall-through. That is kept loud at the gate by
``tests/test_category_congruence.py``, which requires every schema-enum
category to resolve a non-empty set.
"""

from typing import Any, Dict, List

from ..modalities.registry import REGISTRY
from ..validators.base import BaseValidator


def map_validators(task_category: str, options: Dict[str, Any]) -> List[BaseValidator]:
    spec = REGISTRY.get(task_category)
    if spec is None:
        return []
    return spec.build_validators(options)
