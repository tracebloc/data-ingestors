"""Congruence tests across the per-category dispatch sites.

A category accepted by the schema enum flows through four dispatch sites,
each of which silently no-ops on a category it doesn't know:

    1. ``conventions._data_format_for``        (raises — the only loud one)
    2. ``utils.validators_mapping.map_validators``  (falls through to ``[]``)
    3. ``ingestors.base._FILE_BEARING_CATEGORIES``  (gate: skips file transfer)
    4. ``file_transfer.map_file_transfer``          (falls through to ``None``)

instance_segmentation shipped wired into the enum and conventions but
missing from sites 2-4: configs validated, validation "passed" with zero
checks, rows reached MySQL and the backend API, and not a single image was
staged to DEST_PATH — training then failed on missing files (the
silent-half-ingest pattern from #99). It has been removed from the enum;
these tests make the invariant structural so the next category cannot ship
half-wired: every category the schema accepts must be fully dispatched
everywhere, no carve-outs.

The enum ↔ ``TaskCategory`` equality itself is pinned separately by
``test_schema_validation.test_schema_category_enum_matches_engine_categories``.
"""

from __future__ import annotations

import inspect
import json
import re
from pathlib import Path

import pytest

from tracebloc_ingestor import file_transfer
from tracebloc_ingestor.cli.conventions import (
    DEFAULT_IMAGE_FILE_OPTIONS_BY_CATEGORY,
    IMAGE_CATEGORIES,
    _data_format_for,
)
from tracebloc_ingestor.ingestors.base import _FILE_BEARING_CATEGORIES
from tracebloc_ingestor.utils.constants import DataFormat, TaskCategory
from tracebloc_ingestor.utils.validators_mapping import map_validators


REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "tracebloc_ingestor" / "schema" / "ingest.v1.json"

SCHEMA_CATEGORIES = sorted(
    json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))["properties"]["category"]["enum"]
)

# Deliberately over-provisioned: carries every key any map_validators branch
# constructs from (image branches index `extension`/`target_size` directly;
# the rest use .get). Values only need to satisfy validator constructors —
# no validation runs here, so semantic fit per category doesn't matter.
GENEROUS_OPTIONS = {
    "extension": ".jpg",
    "target_size": [256, 256],
    "schema": {"feature_a": "INT", "timestamp": "TIMESTAMP"},
    "number_of_keypoints": 9,
    "time_column": "time",
    "label_column": "label",
}


def _file_bearing(category: str) -> bool:
    """A category is file-bearing iff its data format implies per-row
    sidecar files (image/text). Derived from the same production helper
    the resolver uses, so this test can't drift from the engine — and
    ``_data_format_for`` raises on a category missing from every
    conventions grouping, which makes that gap loud here too."""
    return _data_format_for(category) in (DataFormat.IMAGE, DataFormat.TEXT)


@pytest.mark.parametrize("category", SCHEMA_CATEGORIES)
def test_every_schema_category_resolves_validators(category: str):
    """Site 2: ``map_validators`` falls through to ``[]`` for categories it
    doesn't know, which makes validation trivially "pass" with zero checks.
    Every wired branch returns at least TableName + Duplicate validators,
    so non-empty is exactly the wired/unwired distinction."""
    validators = map_validators(category, dict(GENEROUS_OPTIONS))
    assert validators, (
        f"map_validators({category!r}) returned no validators — the schema "
        f"accepts this category but validation would trivially pass with "
        f"zero checks. Add a branch in utils/validators_mapping.py or "
        f"remove the category from the schema enum."
    )


def test_file_bearing_categories_match_file_transfer_gate():
    """Site 3: ``_FILE_BEARING_CATEGORIES`` gates both the SRC_PATH
    preflight and the per-record ``map_file_transfer`` call. Equality in
    both directions: a file-bearing category missing from the set ingests
    rows without ever staging files; a tabular category in the set would
    fail every record on a file transfer it doesn't need."""
    expected = {c for c in SCHEMA_CATEGORIES if _file_bearing(c)}
    assert _FILE_BEARING_CATEGORIES == expected, (
        "ingestors/base.py _FILE_BEARING_CATEGORIES has drifted from the "
        "schema enum's file-bearing categories:\n"
        f"  file-bearing but NOT gated (records ingest with zero files "
        f"staged): {sorted(expected - _FILE_BEARING_CATEGORIES)}\n"
        f"  gated but not file-bearing (every record would fail transfer): "
        f"{sorted(_FILE_BEARING_CATEGORIES - expected)}"
    )


def test_every_file_bearing_category_has_file_transfer_branch():
    """Site 4: ``map_file_transfer`` falls through to ``None`` for
    categories it doesn't know; the ingest loop counts that as a
    file-transfer failure for EVERY record. Inspect the dispatch source
    for an explicit ``TaskCategory.X`` branch per file-bearing category.

    (Source inspection because calling the real branches touches the
    filesystem, and the unwired fall-through returns the same ``None`` as
    a wired branch handling a missing file — behaviorally ambiguous. If
    map_file_transfer is ever refactored away from explicit TaskCategory
    comparisons, update this test alongside it.)
    """
    source = inspect.getsource(file_transfer.map_file_transfer)
    mentioned_names = set(re.findall(r"TaskCategory\.([A-Z_][A-Z0-9_]*)", source))
    mentioned = {getattr(TaskCategory, name) for name in mentioned_names}

    missing = {c for c in SCHEMA_CATEGORIES if _file_bearing(c)} - mentioned
    assert not missing, (
        f"map_file_transfer has no branch for file-bearing categories "
        f"{sorted(missing)} — every record would be dropped as a "
        f"file-transfer failure. Add a branch in file_transfer.py or "
        f"remove the category from the schema enum."
    )


def test_every_image_category_has_file_options_defaults():
    """Companion to site 1: ``_default_file_options_for`` indexes
    ``DEFAULT_IMAGE_FILE_OPTIONS_BY_CATEGORY[category]`` for image
    categories — a missing entry is a KeyError at resolve time for every
    config of that category."""
    missing = set(IMAGE_CATEGORIES) - set(DEFAULT_IMAGE_FILE_OPTIONS_BY_CATEGORY)
    assert not missing, (
        f"IMAGE_CATEGORIES {sorted(missing)} have no entry in "
        f"DEFAULT_IMAGE_FILE_OPTIONS_BY_CATEGORY — resolve() raises "
        f"KeyError for every config using them."
    )
