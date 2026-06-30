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

from ..utils.constants import DataFormat, TaskCategory
from . import transfer as t
from . import validators as v
from .spec import ModalitySpec

# One entry per supported category — the single source of truth. P3a: the three
# behavior flags (was three frozensets in ingestors/base.py). P3b: the
# validator factory (was the map_validators if/elif arm). P3c: the sidecar
# transfer factory (was the map_file_transfer if/elif arm). P3d: data_format
# (was the 6-frozenset ladder in conventions._data_format_for).
_SPECS = (
    # File-bearing categories (per-row sidecar files under SRC_PATH).
    ModalitySpec(
        TaskCategory.IMAGE_CLASSIFICATION,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
        data_format=DataFormat.IMAGE,
        build_validators=v.image_classification,
        transfer=t.image_classification,
        file_subdir="images",
        is_classification=True,
    ),
    ModalitySpec(
        TaskCategory.OBJECT_DETECTION,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
        data_format=DataFormat.IMAGE,
        build_validators=v.object_detection,
        transfer=t.object_detection,
        file_subdir="images",
        is_classification=True,
    ),
    ModalitySpec(
        TaskCategory.KEYPOINT_DETECTION,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
        data_format=DataFormat.IMAGE,
        build_validators=v.keypoint_detection,
        transfer=t.keypoint_detection,
        file_subdir="images",
        is_classification=True,
    ),
    ModalitySpec(
        TaskCategory.SEMANTIC_SEGMENTATION,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
        data_format=DataFormat.IMAGE,
        build_validators=v.semantic_segmentation,
        transfer=t.semantic_segmentation,
        file_subdir="images",
        is_classification=True,
    ),
    ModalitySpec(
        TaskCategory.TEXT_CLASSIFICATION,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
        data_format=DataFormat.TEXT,
        build_validators=v.text_classification,
        transfer=t.text_classification,
        is_nlp=True,
        file_subdir="texts",
        is_classification=True,
    ),
    ModalitySpec(
        TaskCategory.TOKEN_CLASSIFICATION,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
        data_format=DataFormat.TEXT,
        build_validators=v.token_classification,
        transfer=t.token_classification,
        is_nlp=True,
        file_subdir="texts",
    ),
    # masked_language_modeling is file-bearing AND self-supervised (no label).
    ModalitySpec(
        TaskCategory.MASKED_LANGUAGE_MODELING,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=True,
        data_format=DataFormat.TEXT,
        build_validators=v.masked_language_modeling,
        transfer=t.masked_language_modeling,
        is_nlp=True,
        file_subdir="sequences",
    ),
    # causal_language_modeling is file-bearing AND self-supervised (no label),
    # like MLM. Unlike MLM it consumes RAW text — each sample is a ``.txt`` of
    # either plain text (pretraining) or a tab-separated ``prompt\tcompletion``
    # pair (SFT) — so it stages from ``texts/`` (the raw-text subdir shared with
    # text/token classification), not ``sequences/`` (which the framework
    # reserves for pre-tokenized data). It is NLP, so it ships the #805
    # data-derived text profile for the contributor-tokenizer-fit check.
    ModalitySpec(
        TaskCategory.CAUSAL_LANGUAGE_MODELING,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=True,
        data_format=DataFormat.TEXT,
        build_validators=v.causal_language_modeling,
        transfer=t.causal_language_modeling,
        is_nlp=True,
        file_subdir="texts",
    ),
    # seq2seq is file-bearing AND self-supervised (no label), exactly like
    # causal_language_modeling. Each sample is one ``.txt`` of RAW text — a
    # tab-separated ``source\ttarget`` pair (the same on-disk shape as causal
    # LM's ``prompt\tcompletion``) — so it stages from ``texts/`` (the raw-text
    # subdir shared with text/token classification), not ``sequences/`` (which
    # the framework reserves for pre-tokenized data). It is NLP, so it ships the
    # #805 data-derived text profile for the contributor-tokenizer-fit check.
    ModalitySpec(
        TaskCategory.SEQ2SEQ,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=True,
        data_format=DataFormat.TEXT,
        build_validators=v.seq2seq,
        transfer=t.seq2seq,
        is_nlp=True,
        file_subdir="texts",
    ),
    # embeddings is file-bearing AND self-supervised (no label) — the
    # contrastive NLP modality. Each sample is one ``.txt`` of RAW text: a
    # tab-separated ``anchor\tpositive`` pair OR an ``anchor\tpositive\tnegative``
    # triplet (no label column). Same raw-text staging as seq2seq / causal LM —
    # ``texts/``, not the pre-tokenized ``sequences/`` MLM uses. Unlike those two
    # (which also accept free-form text), the on-disk shape here is STRUCTURED —
    # exactly 2 or 3 tab-separated fields — so its validator factory adds a
    # structural ContrastivePairsValidator that rejects malformed rows. It is
    # NLP, so it ships the #805 data-derived text profile for the
    # contributor-tokenizer-fit check.
    ModalitySpec(
        TaskCategory.EMBEDDINGS,
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=True,
        data_format=DataFormat.TEXT,
        build_validators=v.embeddings,
        transfer=t.embeddings,
        is_nlp=True,
        file_subdir="texts",
    ),
    # Tabular family (structured feature tables; no sidecar files -> no transfer).
    ModalitySpec(
        TaskCategory.TABULAR_CLASSIFICATION,
        is_file_bearing=False,
        is_tabular_family=True,
        is_self_supervised=False,
        data_format=DataFormat.TABULAR,
        build_validators=v.tabular_classification,
        is_classification=True,
    ),
    ModalitySpec(
        TaskCategory.TABULAR_REGRESSION,
        is_file_bearing=False,
        is_tabular_family=True,
        is_self_supervised=False,
        data_format=DataFormat.TABULAR,
        build_validators=v.tabular_regression,
    ),
    ModalitySpec(
        TaskCategory.TIME_SERIES_FORECASTING,
        is_file_bearing=False,
        is_tabular_family=True,
        is_self_supervised=False,
        data_format=DataFormat.TABULAR,
        build_validators=v.time_series_forecasting,
    ),
    ModalitySpec(
        TaskCategory.TIME_TO_EVENT_PREDICTION,
        is_file_bearing=False,
        is_tabular_family=True,
        is_self_supervised=False,
        data_format=DataFormat.TABULAR,
        build_validators=v.time_to_event_prediction,
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
# NLP text categories (text / token classification, MLM). Gates NLP-specific
# ingest handling such as the data-derived text profile (#805).
NLP_CATEGORIES = frozenset(c for c, s in REGISTRY.items() if s.is_nlp)
