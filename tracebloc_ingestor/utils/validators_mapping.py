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

This is also the single seam where the run's resolved :class:`Config` is
injected into the validators (structural refactor backend#796, P4b): the
per-category factories stay ``(options) -> [validators]`` and this function
calls ``bind_config`` on each result. So the path-reading validators read the
ingestor's explicitly-resolved Config rather than a module-global ``Config()``
that snapshots ``os.environ`` — instead of threading ``config=`` through ~40
factory construction sites.
"""

from typing import Any, Dict, List, Optional

from ..config import Config
from ..modalities.registry import REGISTRY
from ..validators.base import BaseValidator


def map_validators(
    task_category: str,
    options: Dict[str, Any],
    config: Optional[Config] = None,
) -> List[BaseValidator]:
    spec = REGISTRY.get(task_category)
    if spec is None:
        return []
    validators = spec.build_validators(options)
    # Inject the run's Config into every validator (P4b). Optional so direct
    # callers / tests that omit it keep the prior behavior: the path-reading
    # validators fall back to their module-global ``config`` at the read site.
    if config is not None:
        for validator in validators:
            validator.bind_config(config)
    return validators
