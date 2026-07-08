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
from tracebloc_ingestor.modalities.registry import REGISTRY
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
