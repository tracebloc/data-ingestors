"""ModalityRegistry — one ModalitySpec per task category.

The single place a category's behavior is declared (built up over P3a–P3d;
see modalities/spec.py). ``tests/test_modality_registry.py`` and
``tests/test_category_congruence.py`` pin the registry to the schema enum so a
category the customer can submit always has a complete spec — making the
``instance_segmentation`` half-wired-zombie class of bug (#240 / #99)
unrepresentable.
"""

from __future__ import annotations

from typing import Dict

from ..utils.constants import TaskCategory
from .spec import ModalitySpec

# One entry per supported category. P3a populates the three behavior flags
# that were hand-maintained frozensets in ingestors/base.py.
_SPECS = (
    # File-bearing categories (per-row sidecar files under SRC_PATH).
    ModalitySpec(
        TaskCategory.IMAGE_CLASSIFICATION,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
    ),
    ModalitySpec(
        TaskCategory.OBJECT_DETECTION,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
    ),
    ModalitySpec(
        TaskCategory.KEYPOINT_DETECTION,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
    ),
    ModalitySpec(
        TaskCategory.SEMANTIC_SEGMENTATION,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
    ),
    ModalitySpec(
        TaskCategory.TEXT_CLASSIFICATION,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
    ),
    ModalitySpec(
        TaskCategory.TOKEN_CLASSIFICATION,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
    ),
    # masked_language_modeling is file-bearing AND self-supervised (no label).
    ModalitySpec(
        TaskCategory.MASKED_LANGUAGE_MODELING,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=True,
    ),
    # Tabular family (structured feature tables; no sidecar files).
    ModalitySpec(
        TaskCategory.TABULAR_CLASSIFICATION,
        is_file_bearing=False,
        is_tabular_family=True,
        is_self_supervised=False,
    ),
    ModalitySpec(
        TaskCategory.TABULAR_REGRESSION,
        is_file_bearing=False,
        is_tabular_family=True,
        is_self_supervised=False,
    ),
    ModalitySpec(
        TaskCategory.TIME_SERIES_FORECASTING,
        is_file_bearing=False,
        is_tabular_family=True,
        is_self_supervised=False,
    ),
    ModalitySpec(
        TaskCategory.TIME_TO_EVENT_PREDICTION,
        is_file_bearing=False,
        is_tabular_family=True,
        is_self_supervised=False,
    ),
)

REGISTRY: Dict[str, ModalitySpec] = {spec.category: spec for spec in _SPECS}


def spec_for(category: str) -> ModalitySpec:
    """Return the spec for ``category``, raising on an unknown one.

    Strict by design: a category that reaches the engine without a spec is a
    wiring bug, not something to silently no-op (that was the
    ``instance_segmentation`` failure mode). Call sites that must tolerate
    ``None`` / non-categories use the derived ``*_CATEGORIES`` frozensets
    below, whose ``in`` test returns False — preserving the prior
    ``category in frozenset`` semantics exactly.
    """
    try:
        return REGISTRY[category]
    except KeyError:
        raise ValueError(
            f"No ModalitySpec registered for category {category!r}. Add one in "
            f"modalities/registry.py or remove the category from the schema enum."
        )


# Registry-derived category sets. These REPLACE the three hand-maintained
# frozensets that lived in ingestors/base.py: derived from the specs so they
# can no longer drift from the single source. ``base.py`` imports these under
# its previous names, so its ``category in <set>`` checks (and their
# None/unknown -> False semantics) are unchanged.
FILE_BEARING_CATEGORIES = frozenset(c for c, s in REGISTRY.items() if s.is_file_bearing)
TABULAR_FAMILY_CATEGORIES = frozenset(
    c for c, s in REGISTRY.items() if s.is_tabular_family
)
SELF_SUPERVISED_CATEGORIES = frozenset(
    c for c, s in REGISTRY.items() if s.is_self_supervised
)
