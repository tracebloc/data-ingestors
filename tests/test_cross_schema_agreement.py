"""The two user-facing schemas must not contradict each other.

``ingest.v1.json`` describes what a user may WRITE in ``ingest.yaml``;
``layout.v1.json`` describes the on-disk layout a consumer should EXPECT.
Both answer "does this category have a manifest?", and until backend#3110
they answered it differently: data-ingestors#552 taught ``ingest.v1.json``
that ``object_detection`` declares neither ``csv`` nor ``json`` (records are
enumerated from the Pascal-VOC XML in ``annotations/``) but left
``layout.v1.json`` still advertising a ``labels_csv`` manifest for it — so a
consumer reading the layout contract would ask for a file the config schema
forbids the user to declare.  The CLI vendors a copy of the layout contract
and the backend pins one as a test fixture, so the contradiction propagated
to three repos.

The manifest-less set is derived BEHAVIOURALLY here — by validating a config
against ``ingest.v1.json`` — rather than by reading the schema's ``if/then``
prose.  A guard that re-parsed the conditional would be asserting on a
reimplementation of the schema logic (backend#1074 trap 26), and would go
quiet the moment the conditional were rewritten in an equivalent form.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft7Validator

from tracebloc_ingestor.modalities.layout import build_layout_contract

REPO_ROOT = Path(__file__).resolve().parent.parent
INGEST_SCHEMA = REPO_ROOT / "tracebloc_ingestor" / "schema" / "ingest.v1.json"
EXAMPLES_DIR = REPO_ROOT / "examples" / "yaml"


@pytest.fixture(scope="module")
def validator() -> Draft7Validator:
    return Draft7Validator(json.loads(INGEST_SCHEMA.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def examples() -> dict:
    """The committed per-category example configs, keyed by category.

    `test_schema_validation.py` already pins every one of these as VALID, so
    they are the only bases from which a single-key mutation isolates the
    csv/json conditional: start from something the schema accepts, change one
    thing, and any new error is attributable to that change.
    """
    by_category = {}
    for path in sorted(EXAMPLES_DIR.glob("*.yaml")):
        config = yaml.safe_load(path.read_text(encoding="utf-8"))
        category = config.get("category")
        if category is not None:
            by_category.setdefault(category, config)
    return by_category


def _forbids_csv_source(validator: Draft7Validator, config: dict) -> bool:
    """Does `ingest.v1.json` REJECT this category naming a CSV data source?

    Derived by mutation, not by re-reading the schema's `if/then/else`: a
    guard that reparsed the conditional would assert on a reimplementation of
    the schema logic (backend#1074 trap 26) and go quiet the moment the
    conditional was rewritten in an equivalent form.  Here the base config is
    known-valid, so `csv` is the only thing that can have broken it.
    """
    probe = {k: v for k, v in config.items() if k != "json"}
    probe["csv"] = "data.csv"
    return not validator.is_valid(probe)


def test_manifest_less_categories_agree_across_schemas(validator, examples):
    """`layout.manifest is null` iff `ingest.yaml` forbids a csv/json source.

    Both directions matter.  A category the config schema forbids a manifest
    for must not advertise one in the layout contract (the backend#3110 bug);
    and a category that still declares a manifest must not have had its layout
    entry nulled out, which would send consumers looking for records in a
    directory the ingestor never enumerates.
    """
    layout = build_layout_contract()["tasks"]
    assert set(layout) <= set(examples), "no example config for: " + ", ".join(
        sorted(set(layout) - set(examples))
    )

    disagreements = []
    for category, entry in sorted(layout.items()):
        layout_has_manifest = entry["manifest"] is not None
        ingest_has_manifest = not _forbids_csv_source(validator, examples[category])
        if layout_has_manifest != ingest_has_manifest:
            disagreements.append(
                f"{category}: layout.v1.json "
                f"{'declares a' if layout_has_manifest else 'declares NO'} manifest "
                f"but ingest.v1.json "
                f"{'permits' if ingest_has_manifest else 'forbids'} a csv/json source"
            )

    assert not disagreements, "schemas contradict each other:\n  " + "\n  ".join(
        disagreements
    )


def test_probe_can_tell_the_two_answers_apart(validator, examples):
    """Guard the guard: the probe must not return the same verdict for every
    category, which is how a cross-schema check silently goes vacuous.

    If `_forbids_csv_source` ever collapsed to a constant — because the
    example configs stopped validating for an unrelated reason, or because the
    mutation stopped being the only difference — the test above would pass
    while comparing a constant to a constant.  Pinning one category on each
    side of the answer keeps that visible.
    """
    assert _forbids_csv_source(validator, examples["object_detection"]) is True
    assert _forbids_csv_source(validator, examples["image_classification"]) is False
