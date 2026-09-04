"""The manifest-less trait must agree with what actually forbids a manifest.

``records_from_sidecar`` (the ModalitySpec trait, backend#3076) drives the
layout contract's ``manifest.kind == "none"``. That is now the FIFTH place that
encodes "object_detection has no manifest" — alongside ``ingest.v1.json``'s
csv/json rejection, ``conventions.resolve``'s ``source_type == "annotations"``
dispatch, the ``VOCIngestor`` selection, and the trait itself. Four of the five
were previously untied to each other; the trait was pinned only to its own
uniqueness / required-sidecar guards (``test_layout_contract.py``), which are
self-consistency checks — they cannot catch the trait drifting away from the
behaviour it is supposed to mirror (Cursor Bugbot on backend#3076).

This module ties the trait to the two independent sources of the same truth:

  1. **ingest.v1.json** — the config schema the user writes against. If OD ever
     re-permitted a ``csv``/``json`` source (the exact regression that produced
     the HTTP 400) while the trait stayed set, the layout contract would still
     say ``kind="none"`` and the two user-facing schemas would contradict.
  2. **conventions.resolve** — the ingestor's own source dispatch. If a second
     category were enumerated from a sidecar (``source_type == "annotations"``)
     without flipping the trait, its layout entry would wrongly still advertise
     a labels CSV.

The manifest-less set is derived BEHAVIOURALLY — by validating a config against
the schema and by calling the real resolver — not by re-reading the schema's
``if/then`` prose. A guard that re-parsed the conditional would assert on a
reimplementation of the schema logic (backend#1074 trap 26) and would go quiet
the moment the conditional were rewritten in an equivalent form.

Lifted and adapted from the superseded data-ingestors#556 (backend#3110), whose
``manifest: null`` spelling this repo settled against in favour of a named
``kind: "none"`` sentinel — so the has-manifest predicate here is
``kind != "none"``, not ``manifest is not None``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft7Validator

from tracebloc_ingestor.cli.conventions import resolve
from tracebloc_ingestor.modalities.layout import build_layout_contract
from tracebloc_ingestor.modalities.registry import REGISTRY

REPO_ROOT = Path(__file__).resolve().parent.parent
INGEST_SCHEMA = REPO_ROOT / "tracebloc_ingestor" / "schema" / "ingest.v1.json"
EXAMPLES_DIR = REPO_ROOT / "examples" / "yaml"


@pytest.fixture(scope="module")
def validator() -> Draft7Validator:
    return Draft7Validator(json.loads(INGEST_SCHEMA.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def examples() -> dict:
    """The committed per-category example configs, keyed by category.

    ``test_schema_validation.py`` already pins every one of these as VALID, so
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


def _has_manifest(entry: dict) -> bool:
    """Does this layout entry advertise a manifest CSV?

    In this repo's spelling a manifest-less category is ``kind == "none"`` (a
    named sentinel), NOT ``manifest is None`` — so read the kind, not the
    presence of the block.
    """
    return entry["manifest"]["kind"] != "none"


def _forbids_csv_source(validator: Draft7Validator, config: dict) -> bool:
    """Does ``ingest.v1.json`` REJECT this category naming a CSV data source?

    Derived by mutation, not by re-reading the schema's ``if/then/else``: the
    base config is known-valid (pinned by ``test_schema_validation.py``), so a
    ``csv`` key is the only thing that can have broken it.
    """
    probe = {k: v for k, v in config.items() if k != "json"}
    probe["csv"] = "data.csv"
    return not validator.is_valid(probe)


def test_manifest_less_categories_agree_across_schemas(validator, examples):
    """``layout.manifest.kind == "none"`` iff ``ingest.yaml`` forbids a source.

    Both directions matter. A category the config schema forbids a manifest for
    must not advertise one in the layout contract (the backend#3110 / #3076
    bug); and a category that still declares a manifest must not have been
    nulled out, which would send consumers looking for records in a directory
    the ingestor never enumerates.
    """
    layout = build_layout_contract()["tasks"]
    assert set(layout) <= set(examples), "no example config for: " + ", ".join(
        sorted(set(layout) - set(examples))
    )

    disagreements = []
    for category, entry in sorted(layout.items()):
        layout_has_manifest = _has_manifest(entry)
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


def test_trait_agrees_with_the_resolver_source_dispatch(examples):
    """``records_from_sidecar`` iff ``resolve(...).source_type == "annotations"``.

    The layout contract's ``kind="none"`` is derived from the trait; the
    ingestor's OWN reader keys off ``conventions.resolve``. Tie the two so a
    category enumerated from a sidecar can't be given the trait without the
    resolver agreeing it reads from ``annotations/`` — and vice versa. Both
    sides are importable, so this is a direct behavioural comparison, not a
    re-read of either's prose.
    """
    mismatches = []
    for category, config in sorted(examples.items()):
        if category not in REGISTRY:
            continue
        reads_from_sidecar = resolve(config).source_type == "annotations"
        trait = REGISTRY[category].records_from_sidecar
        if reads_from_sidecar != trait:
            mismatches.append(
                f"{category}: resolver source_type "
                f"{'is' if reads_from_sidecar else 'is NOT'} 'annotations' but "
                f"records_from_sidecar={trait}"
            )
    assert not mismatches, "trait disagrees with the resolver:\n  " + "\n  ".join(
        mismatches
    )


def test_probe_can_tell_the_two_answers_apart(validator, examples):
    """Guard the guard: the probe must not return the same verdict for every
    category, which is how a cross-schema check silently goes vacuous.

    If ``_forbids_csv_source`` ever collapsed to a constant — because the
    example configs stopped validating for an unrelated reason, or because the
    mutation stopped being the only difference — the test above would pass while
    comparing a constant to a constant. Pinning one category on each side of the
    answer keeps that visible.
    """
    assert _forbids_csv_source(validator, examples["object_detection"]) is True
    assert _forbids_csv_source(validator, examples["image_classification"]) is False
