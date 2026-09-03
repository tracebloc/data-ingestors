"""Pins the machine-readable per-task layout contract (data-ingestors#347).

Three guarantees:
  1. DRIFT — the committed schema/layout.v1.json equals what the registry emits
     (regenerate with `python -m tracebloc_ingestor.modalities.layout`).
  2. CONGRUENCE — the contract covers exactly the task taxonomy, and its
     derived fields agree with each spec's flags (single source, no drift).
  3. FAITHFULNESS — spot-checks that the two hand-declared pieces (sidecars,
     record_format) match the ingestor's actual layout truth, so the Go mirror
     the CLI builds against this JSON can't encode a wrong layout.
"""

from __future__ import annotations

import json

from tracebloc_ingestor.modalities.layout import (
    build_layout_contract,
    contract_path,
    render_contract,
)
from tracebloc_ingestor.modalities.registry import (
    FIXED_TIME_COLUMN_BY_CATEGORY,
    REGISTRY,
)
from tracebloc_ingestor.utils.constants import TaskCategory


def _contract():
    return build_layout_contract()["tasks"]


# 1. DRIFT --------------------------------------------------------------------


def test_committed_json_matches_registry():
    with open(contract_path(), encoding="utf-8") as fh:
        committed = fh.read()
    assert committed == render_contract(), (
        "schema/layout.v1.json is stale — regenerate with "
        "`python -m tracebloc_ingestor.modalities.layout` and commit the result."
    )


def test_committed_json_is_valid_and_versioned():
    with open(contract_path(), encoding="utf-8") as fh:
        doc = json.load(fh)
    assert doc["version"], "layout contract must carry a version"
    assert doc["tasks"], "layout contract has no tasks"
    _assert_shape_matches_declared_version(doc)


# The FIELD SET each task entry exposes, per contract version — the thing a
# consumer in another repo writes code against. Three repos read this schema
# (data-ingestors emits it, the CLI vendors a copy, the backend pins one as a
# fixture), so a field appearing or disappearing without a version bump leaves
# them unable to tell which shape they hold.
#
# Deliberately keyed on field NAMES, not values: a category gaining a stricter
# `data_shape` is not a shape change and must not force a bump, whereas
# `manifest` becoming nullable in v4 (backend#3110) is exactly one.
#
# This replaces three copies of `assert doc["version"] == "3"` that had
# accumulated across unrelated test files. Scattered literals are a poor
# tripwire twice over: bumping the version means hunting all of them, and
# nothing made any single copy authoritative, so deleting the awkward one was
# always the path of least resistance.
SHAPE_BY_VERSION = {
    "4": {
        "family",
        "grouping",
        "manifest",
        "ordering",
        "primary_subdir",
        "record_format",
        "sidecars",
    },
}


def _assert_shape_matches_declared_version(doc: dict) -> None:
    version = doc["version"]
    assert version in SHAPE_BY_VERSION, (
        f"layout contract declares version {version!r}, which SHAPE_BY_VERSION "
        f"does not describe. If you changed the per-task field set, add an "
        f"entry for {version!r}; if you bumped the version without changing "
        f"the shape, the bump was unnecessary."
    )
    expected = SHAPE_BY_VERSION[version]
    for category, entry in sorted(doc["tasks"].items()):
        actual = set(entry)
        assert actual == expected, (
            f"{category}: task entry exposes {sorted(actual)} but version "
            f"{version} is declared to expose {sorted(expected)} "
            f"(added: {sorted(actual - expected)}, "
            f"removed: {sorted(expected - actual)}). A changed field set is a "
            f"new contract shape: bump LAYOUT_CONTRACT_VERSION and add the new "
            f"field set to SHAPE_BY_VERSION."
        )


# 2. CONGRUENCE ---------------------------------------------------------------


def test_contract_covers_exactly_the_taxonomy():
    # Same set as the registry / TaskCategory / schema enum — a category the
    # customer can submit always has a layout, and none is invented.
    assert set(_contract()) == set(REGISTRY)
    assert set(_contract()) == set(TaskCategory.get_all_categories())


def test_derived_fields_agree_with_spec_flags():
    contract = _contract()
    for cat, spec in REGISTRY.items():
        layout = contract[cat]
        assert layout["family"] == spec.data_format, cat
        assert layout["primary_subdir"] == spec.file_subdir, cat
        # `manifest` is null exactly for a category the user stages no manifest
        # for. Asserted BOTH ways: a manifest-less category must not describe
        # one, and a manifest-bearing category must not be null. Before
        # backend#1006 these could not come apart, because manifest-ness was
        # read off `is_file_bearing`; object_detection is the first category
        # that is file-bearing AND manifest-less (records enumerated from
        # `annotations/*.xml`), which is what left the generated schema
        # declaring a labels.csv for OD after #552 removed it (backend#3110).
        assert (layout["manifest"] is None) == (not spec.has_manifest), (
            f"{cat}: manifest is "
            f"{'null' if layout['manifest'] is None else 'declared'} but "
            f"has_manifest={spec.has_manifest}"
        )
        if not spec.has_manifest:
            continue
        # file-bearing ⇒ a labels CSV listing per-row files; else the data CSV.
        assert (
            layout["manifest"]["requires_filename_column"] == spec.is_file_bearing
        ), cat
        assert layout["manifest"]["kind"] == (
            "labels_csv" if spec.is_file_bearing else "data_csv"
        ), cat
        # a user label/target column exists iff the task isn't self-supervised.
        assert layout["manifest"]["has_label_column"] == (
            not spec.is_self_supervised
        ), cat


def test_sidecars_only_on_file_bearing_tasks():
    for cat, layout in _contract().items():
        if layout["sidecars"]:
            assert REGISTRY[
                cat
            ].is_file_bearing, f"{cat} declares sidecars but isn't file-bearing"


def test_record_format_only_on_text_tasks():
    for cat, layout in _contract().items():
        if layout["record_format"] is not None:
            assert (
                REGISTRY[cat].data_format == "text"
            ), f"{cat} has a record_format but isn't text"


def test_ordering_agrees_with_the_registry():
    # `ordering` is DERIVED, not hand-declared: its column is the category's
    # entry in FIXED_TIME_COLUMN_BY_CATEGORY and its scope follows the spec's
    # grouping trait, so mutating `_ordering` reddens this. This pins the
    # derivation to the REGISTRY only — that scope actually matches the validator
    # the ingestor runs is the separate, stronger guarantee in
    # test_ordering_scope_is_anchored_to_the_enforcing_validator (backend#1870).
    for cat, layout in _contract().items():
        fixed = FIXED_TIME_COLUMN_BY_CATEGORY.get(cat)
        if fixed is None:
            assert layout["ordering"] is None, cat
        else:
            assert layout["ordering"] == {
                "column": fixed,
                "scope": "per_group" if REGISTRY[cat].grouping else "dataset",
            }, cat


# 3. FAITHFULNESS (spot-checks vs the ingestor's real layout) -----------------


def test_object_detection_requires_pascal_voc_xml_sidecar():
    sc = _contract()["object_detection"]["sidecars"]
    assert sc == [
        {
            "subdir": "annotations",
            "glob": "*.xml",
            "required": True,
            "link_column": None,
        }
    ]


def test_semantic_segmentation_masks_linked_by_mask_id():
    sc = _contract()["semantic_segmentation"]["sidecars"]
    assert sc == [
        {"subdir": "masks", "glob": "*.png", "required": True, "link_column": "mask_id"}
    ]


def test_sentence_pair_is_enforced_tab_pair():
    rf = _contract()["sentence_pair_classification"]["record_format"]
    assert rf == {
        "separator": "\t",
        "fields": ["text_a", "text_b"],
        "min_fields": 2,
        "enforced": True,
    }


def test_embeddings_is_enforced_two_or_three_fields():
    rf = _contract()["embeddings"]["record_format"]
    assert rf["fields"] == ["anchor", "positive", "negative"]
    assert rf["min_fields"] == 2 and rf["enforced"] is True  # 2 (pair) or 3 (triplet)


def test_seq2seq_and_causal_lm_are_unenforced_conventions():
    # Both accept raw free text — a mirror must NOT reject a non-tab file.
    for cat in ("seq2seq", "causal_language_modeling"):
        rf = _contract()[cat]["record_format"]
        assert rf is not None and rf["enforced"] is False, cat


def test_mlm_stages_from_sequences_with_no_record_format():
    mlm = _contract()["masked_language_modeling"]
    assert mlm["primary_subdir"] == "sequences"
    assert mlm["record_format"] is None  # pre-tokenized is a convention, not enforced
    assert mlm["manifest"]["has_label_column"] is False


def test_keypoint_has_no_sidecar():
    # Keypoints ride in the CSV (Annotation/Visibility columns), NOT a sidecar dir.
    assert _contract()["keypoint_detection"]["sidecars"] == []


def test_tabular_family_is_data_csv_without_file_layout():
    for cat in (
        "tabular_classification",
        "tabular_regression",
        "time_series_forecasting",
        "time_to_event_prediction",
    ):
        layout = _contract()[cat]
        assert layout["manifest"]["kind"] == "data_csv", cat
        assert layout["manifest"]["requires_filename_column"] is False, cat
        assert layout["primary_subdir"] is None, cat
        assert layout["sidecars"] == [] and layout["record_format"] is None, cat


def test_time_ordering_constraint_is_discoverable():
    # The seam backend#1870 closed: the ingestor enforces timestamp ordering in
    # its validators, and now the contract says so. Forecasting is one merged
    # series (global TimeOrderedValidator → dataset scope); classification is
    # ordered within each sequence (PerGroupTimeOrderedValidator → per_group).
    contract = _contract()
    assert contract["time_series_forecasting"]["ordering"] == {
        "column": "timestamp",
        "scope": "dataset",
    }
    assert contract["time_series_classification"]["ordering"] == {
        "column": "timestamp",
        "scope": "per_group",
    }
    # time_to_event_prediction has a time column too, but it is a per-row
    # duration validated by TimeToEventValidator — NOT a monotonic axis. The
    # contract must state that it is not subject to the ordering rule, so a
    # consumer no longer has to infer it from the validator's silence.
    assert contract["time_to_event_prediction"]["ordering"] is None


def test_ordering_scope_is_anchored_to_the_enforcing_validator():
    # The derivation ties `scope` to a CORRELATED trait (grouping), not to the
    # thing that enforces ordering — the per-category validator factory, which
    # hardcodes its choice (validators.py: TSF appends TimeOrderedValidator, TSC
    # appends PerGroupTimeOrderedValidator; neither consults `grouping`). Today
    # they agree only because TSC is the sole grouped category AND the sole
    # per-group user — coincidence, not construction. A future grouped category
    # wired to the global validator would make the contract declare `per_group`
    # while the ingestor enforces dataset-wide ordering: the exact seam
    # backend#1870 is about, reopened. Pin the two together so it's true by test.
    #
    # Instantiating a factory is side-effect-free (no DB/file), so we can read
    # the validator types directly. The superset options satisfy every factory's
    # required keys (extension/target_size for the image tasks); values are only
    # touched at validate() time, which we never call.
    from tracebloc_ingestor.utils.constants import FileExtension
    from tracebloc_ingestor.validators.per_group_time_ordered_validator import (
        PerGroupTimeOrderedValidator,
    )
    from tracebloc_ingestor.validators.time_ordered_validator import (
        TimeOrderedValidator,
    )

    opts = {"schema": {}, "extension": FileExtension.JPG, "target_size": (1, 1)}
    contract = _contract()
    for cat, spec in REGISTRY.items():
        validators = spec.build_validators(dict(opts))
        # PerGroup does not subclass the global validator, but guard anyway so a
        # later class change can't silently count a per-group run as dataset.
        dataset_scoped = any(
            isinstance(v, TimeOrderedValidator)
            and not isinstance(v, PerGroupTimeOrderedValidator)
            for v in validators
        )
        per_group_scoped = any(
            isinstance(v, PerGroupTimeOrderedValidator) for v in validators
        )
        ordering = contract[cat]["ordering"]
        if ordering is None:
            assert not dataset_scoped and not per_group_scoped, (
                f"{cat}: contract declares no ordering, but its factory runs a "
                f"time-ordering validator"
            )
        elif ordering["scope"] == "per_group":
            assert per_group_scoped and not dataset_scoped, cat
        else:
            assert ordering["scope"] == "dataset" and dataset_scoped, cat
            assert not per_group_scoped, cat
