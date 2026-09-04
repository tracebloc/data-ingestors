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


#: ADAPTED FOR THIS REPO'S SHAPE, and the adaptation is the point.
#:
#: A field-set-only fingerprint does NOT detect v4 here: `manifest` is still an
#: object, so every per-task field is unchanged and the tripwire would report
#: the bump as unnecessary. What v4 actually adds is a new `kind` VALUE
#: (`"none"`), which is exactly why backend#3076 argues the bump is warranted --
#: a consumer has to grow a BRANCH, not read a different string.
#:
#: So the fingerprint is the field set AND the set of manifest kinds in use. A
#: new field demands a bump, a new kind demands a bump, and a value-level change
#: to something like `ordering.column` correctly does not.
def _shape_of(doc: dict) -> dict:
    """The contract's shape: every task's NESTED key paths, plus the kinds.

    NESTED, not `set(entry)`. The first version fingerprinted top-level keys
    only, so `manifest.encoding` or `sidecars[].mime` could be added and the
    tripwire passed while declaring the same version (LukasWodka,
    data-ingestors#558). Those are not hypothetical: each maps one-to-one onto
    a struct the CLI would have to grow a field on (`ManifestLayout`,
    `SidecarSpec` in `internal/push/layout_contract.go`), which is the same
    "the consumer must grow a branch" argument the bump rests on.

    A list contributes the union of its elements' paths, so a field added to
    ONE sidecar entry is still visible.
    """

    def paths(node, prefix=""):
        out = set()
        if isinstance(node, dict):
            for k, v in node.items():
                here = f"{prefix}.{k}" if prefix else k
                out.add(here)
                out |= paths(v, here)
        elif isinstance(node, list):
            for item in node:
                out |= paths(item, f"{prefix}[]")
        return out

    field_paths = set()
    for entry in doc["tasks"].values():
        field_paths |= paths(entry)
    kinds = {
        (entry.get("manifest") or {}).get("kind") for entry in doc["tasks"].values()
    } - {None}
    return {"field_paths": field_paths, "manifest_kinds": kinds}


#: The shape each contract version declares. Both members are pinned by
#: EQUALITY, not subset.
#:
#: `manifest_kinds` was `<=` and that made the check one-directional: a NEW
#: kind reddened, a REMOVED one did not. Since v3's kind set
#: (`{labels_csv, data_csv}`) is a strict subset of v4's, a contract silently
#: reverted to the v3 shape passed while still declaring `version: "4"` --
#: which is the single distinction this tripwire was written to make
#: (LukasWodka, data-ingestors#558).
SHAPE_BY_VERSION = {
    "4": {
        "field_paths": {
            "family",
            "grouping",
            "grouping.count_unit",
            "grouping.group_column",
            "grouping.time_column",
            "manifest",
            "manifest.has_label_column",
            "manifest.kind",
            "manifest.requires_filename_column",
            "ordering",
            "ordering.column",
            "ordering.scope",
            "primary_subdir",
            "record_format",
            "record_format.enforced",
            "record_format.fields",
            "record_format.min_fields",
            "record_format.separator",
            "sidecars",
            "sidecars[].glob",
            "sidecars[].link_column",
            "sidecars[].required",
            "sidecars[].subdir",
        },
        "manifest_kinds": {"labels_csv", "data_csv", "none"},
    },
}


def _assert_shape_matches_declared_version(doc: dict) -> None:
    version = doc["version"]

    assert version in SHAPE_BY_VERSION, (
        f"layout contract declares version {version!r}, which "
        f"SHAPE_BY_VERSION does not describe. Add an entry for {version!r} "
        f"recording the shape it publishes -- and note the next assertion "
        f"requires it to DIFFER from the previous version's, so copying the "
        f"previous body verbatim will not satisfy it."
    )

    actual = _shape_of(doc)
    expected = SHAPE_BY_VERSION[version]

    for member, label in (
        ("field_paths", "field paths"),
        ("manifest_kinds", "manifest kinds"),
    ):
        assert actual[member] == expected[member], (
            f"{label} in the contract are {sorted(actual[member])} but version "
            f"{version} is declared to publish {sorted(expected[member])} "
            f"(added: {sorted(actual[member] - expected[member])}, "
            f"removed: {sorted(expected[member] - actual[member])}). Both "
            f"directions are a shape change: adding one means consumers grow a "
            f"branch, and REMOVING one means a consumer's existing branch is "
            f"now dead while the version still claims otherwise. Bump "
            f"LAYOUT_CONTRACT_VERSION and record the new shape."
        )

    # THE PROMISE THE OLD MESSAGE MADE AND THE CHECK DID NOT KEEP. It said "if
    # you bumped the version without doing either, the bump was unnecessary" --
    # but nothing compared a new entry against its predecessor, so the remedy
    # it offered first ("add an entry for '5'") was satisfied identically by a
    # necessary bump and by copying the previous body verbatim (LukasWodka,
    # data-ingestors#558).
    previous = [v for v in SHAPE_BY_VERSION if v < version]
    if previous:
        prior = SHAPE_BY_VERSION[max(previous)]
        assert (expected["field_paths"], expected["manifest_kinds"]) != (
            prior["field_paths"],
            prior["manifest_kinds"],
        ), (
            f"version {version} declares exactly the same shape as "
            f"{max(previous)}, so the bump changed nothing a consumer can "
            f"observe and was unnecessary. Either revert it, or record what "
            f"actually changed -- copying the previous entry is how a version "
            f"number stops meaning anything."
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
        manifest = layout["manifest"]
        if spec.records_from_sidecar:
            # backend#1006: object_detection has NO manifest CSV — its records
            # are enumerated from the annotations sidecar and each label is
            # derived, so there is no labels CSV, no filename column, and no
            # user-declared label column.
            assert manifest["kind"] == "none", cat
            assert manifest["requires_filename_column"] is False, cat
            assert manifest["has_label_column"] is False, cat
        else:
            # file-bearing ⇒ a labels CSV listing per-row files; else the data CSV.
            assert manifest["requires_filename_column"] == spec.is_file_bearing, cat
            assert manifest["kind"] == (
                "labels_csv" if spec.is_file_bearing else "data_csv"
            ), cat
            # a user label/target column exists iff the task isn't self-supervised.
            assert manifest["has_label_column"] == (not spec.is_self_supervised), cat


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


def test_object_detection_has_no_manifest_csv():
    # backend#1006 / backend#3076: OD is file-bearing but has NO labels CSV — its
    # records are enumerated from the required annotations/*.xml sidecar and each
    # label is derived from <object><name>. The manifest therefore declares no
    # CSV, no filename column, and no user label column, so a consumer (the CLI)
    # skips the labels-CSV reads it runs for every other file-bearing category.
    # (This is the shape whose absence made OD ingest impossible from the CLI:
    # the CLI required + emitted a labels.csv the ingestor's schema rejects.)
    manifest = _contract()["object_detection"]["manifest"]
    assert manifest == {
        "kind": "none",
        "requires_filename_column": False,
        "has_label_column": False,
    }
    # It is still file-bearing (images + the xml sidecar are staged) — the
    # no-CSV shape is specifically the manifest, not the whole category.
    assert REGISTRY["object_detection"].is_file_bearing is True


def test_records_from_sidecar_is_unique_to_object_detection():
    # Single-category traits are pinned to their one owner (same guard grouping
    # and the tag-sequence trait carry) so a copy-paste can't silently hand
    # another task OD's manifest-less shape. records_from_sidecar derives
    # kind="none" with no CSV/label column; a mistaken second use would produce
    # an unrunnable CSV-less contract that only this assertion would catch.
    owners = {c for c, s in REGISTRY.items() if s.records_from_sidecar}
    assert owners == {TaskCategory.OBJECT_DETECTION}


def test_records_from_sidecar_requires_a_sidecar_to_enumerate_from():
    # kind="none" means the records come FROM a sidecar, so a category that sets
    # the trait MUST declare a required one — otherwise the contract is
    # manifest-less with nothing to enumerate records from. Ties the trait to
    # its precondition rather than trusting the flag in isolation.
    for cat, spec in REGISTRY.items():
        if spec.records_from_sidecar:
            assert any(s.required for s in spec.sidecars), cat


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
