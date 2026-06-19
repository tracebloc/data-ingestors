"""Tests for the ModalityRegistry (structural refactor — backend#796, P3a).

The registry is the single source of truth for per-category behavior. These
pin the invariant that makes a half-wired modality (the instance_segmentation
zombie, #240/#99) unrepresentable: every category the engine/schema knows has
a spec, and the registry-derived category sets match the spec flags.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tracebloc_ingestor.modalities import (
    FILE_BEARING_CATEGORIES,
    NLP_CATEGORIES,
    REGISTRY,
    SELF_SUPERVISED_CATEGORIES,
    TABULAR_FAMILY_CATEGORIES,
    registry,
    spec_for,
)
from tracebloc_ingestor.utils.constants import TaskCategory

SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent
    / "tracebloc_ingestor"
    / "schema"
    / "ingest.v1.json"
)
SCHEMA_CATEGORIES = set(
    json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))["properties"]["category"][
        "enum"
    ]
)


def test_registry_covers_taskcategory_and_schema_enum_exactly():
    """The registry must cover exactly the engine's categories and the
    customer-facing schema enum — no extras, no gaps. A category in the schema
    without a spec is the half-wired-zombie failure mode."""
    engine = set(TaskCategory.get_all_categories())
    assert set(REGISTRY) == engine == SCHEMA_CATEGORIES


def test_derived_sets_match_spec_flags():
    assert FILE_BEARING_CATEGORIES == {
        c for c, s in REGISTRY.items() if s.is_file_bearing
    }
    assert TABULAR_FAMILY_CATEGORIES == {
        c for c, s in REGISTRY.items() if s.is_tabular_family
    }
    assert SELF_SUPERVISED_CATEGORIES == {
        c for c, s in REGISTRY.items() if s.is_self_supervised
    }
    assert NLP_CATEGORIES == {c for c, s in REGISTRY.items() if s.is_nlp}


def test_nlp_categories_are_exactly_the_text_categories():
    """#805: the NLP set is exactly the text categories (text/token
    classification + masked & causal language modeling) — never image/tabular."""
    assert NLP_CATEGORIES == {
        TaskCategory.TEXT_CLASSIFICATION,
        TaskCategory.TOKEN_CLASSIFICATION,
        TaskCategory.MASKED_LANGUAGE_MODELING,
        TaskCategory.CAUSAL_LANGUAGE_MODELING,
    }


def test_spec_for_raises_on_unknown_category():
    with pytest.raises(ValueError, match="No ModalitySpec"):
        spec_for("totally_not_a_category")


def test_known_flag_values():
    # Lock the data that used to live in the three base.py frozensets.
    mlm = spec_for(TaskCategory.MASKED_LANGUAGE_MODELING)
    assert mlm.is_file_bearing and mlm.is_self_supervised and not mlm.is_tabular_family

    # causal LM mirrors MLM's flags (file-bearing + self-supervised + NLP) but
    # stages raw text from texts/, not pre-tokenized sequences/.
    clm = spec_for(TaskCategory.CAUSAL_LANGUAGE_MODELING)
    assert clm.is_file_bearing and clm.is_self_supervised and clm.is_nlp
    assert not clm.is_tabular_family and not clm.is_classification
    assert clm.file_subdir == "texts"

    tab = spec_for(TaskCategory.TABULAR_CLASSIFICATION)
    assert (
        tab.is_tabular_family and not tab.is_file_bearing and not tab.is_self_supervised
    )

    img = spec_for(TaskCategory.IMAGE_CLASSIFICATION)
    assert (
        img.is_file_bearing and not img.is_tabular_family and not img.is_self_supervised
    )


def test_base_py_imports_the_registry_derived_sets():
    """base.py must consume the registry's sets (not redefine its own), so the
    flags can't drift from the single source."""
    from tracebloc_ingestor.ingestors import base

    assert base._FILE_BEARING_CATEGORIES is registry.FILE_BEARING_CATEGORIES
    assert base._TABULAR_FAMILY_CATEGORIES is registry.TABULAR_FAMILY_CATEGORIES
    # base.py no longer imports SELF_SUPERVISED_CATEGORIES: edge-label metadata
    # is now generated for every category (the #213 self-supervised skip was
    # removed once backend PR #683 allowed blank labels), so the set is unused
    # here. The registry remains its single source for other consumers.


def test_data_format_valid_and_matches_conventions():
    """Every spec carries a valid DataFormat (P3d), and conventions'
    _data_format_for reads it from the registry (single source)."""
    from tracebloc_ingestor.cli.conventions import _data_format_for
    from tracebloc_ingestor.utils.constants import DataFormat

    valid = set(DataFormat.get_all_formats())
    for category, spec in REGISTRY.items():
        assert (
            spec.data_format in valid
        ), f"{category}: bad data_format {spec.data_format!r}"
        assert _data_format_for(category) == spec.data_format


def test_transfer_present_iff_file_bearing():
    """The sidecar transfer factory is set exactly for file-bearing categories
    (P3c invariant)."""
    for category, spec in REGISTRY.items():
        assert spec.is_file_bearing == (spec.transfer is not None)
