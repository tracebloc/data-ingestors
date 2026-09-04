"""`records_from_sidecar` must agree with what the CLI actually dispatches.

THE TRAIT WAS AN UNVERIFIED FIFTH COPY. "object_detection has no manifest" is
stated in four independent places, and before this file only one of them was
tested:

1. ``ingest.v1.json`` -- the per-category csv/json rejection
2. ``cli/conventions.py`` -- ``source_type = "annotations" if category ==
   TaskCategory.OBJECT_DETECTION``, a hard-coded category string, NOT the trait
3. ``cli/run.py`` -- ``if resolved.source_type == "annotations": VOCIngestor``
4. ``ModalitySpec.records_from_sidecar`` -- the trait itself

Measured on data-ingestors#555: dropping ``records_from_sidecar=True`` from the
registry and regenerating the JSON sends the published contract straight back to
``{"kind": "labels_csv", "has_label_column": true,
"requires_filename_column": true}`` -- the exact shape that produced the 400 --
while ``conventions.py`` keeps dispatching ``VOCIngestor`` and the entire
ingest-side suite stays green. The only thing that noticed was one hand-written
literal in ``test_layout_contract.py``.

``test_derived_fields_agree_with_spec_flags`` does not help there: it restates
``_manifest()`` against itself.

So this file pins the trait against the two things it is supposed to describe,
in both directions:

* the DISPATCH half -- ``resolve(...).source_type == "annotations"`` iff
  ``spec.records_from_sidecar``. A category whose trait says "from the sidecar"
  while the CLI reads a CSV, or the reverse, is the drift.
* the SCHEMA half -- ``manifest.kind == "none"`` iff ``ingest.v1.json`` rejects
  a csv/json source. That is the guard whose absence let #552 move one schema
  and not the other.

Two halves rather than one, because they can come apart independently: the
trait could agree with the dispatch while the published contract disagreed with
both, which is exactly what #552 shipped.
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

#: The sentinel kind meaning "this category stages no manifest".
MANIFEST_LESS_KIND = "none"


@pytest.fixture(scope="module")
def examples() -> dict:
    """The committed per-category example configs, keyed by category.

    `test_schema_validation.py` already pins every one as VALID, so they are
    the only inputs from which `resolve()` can be exercised without hand-built
    configs that could drift from the schema they are meant to satisfy.
    """
    out = {}
    for path in sorted(EXAMPLES_DIR.glob("*.yaml")):
        config = yaml.safe_load(path.read_text(encoding="utf-8"))
        if config.get("category"):
            out.setdefault(config["category"], config)
    return out


def test_the_trait_agrees_with_what_the_cli_dispatches(examples):
    """`source_type == "annotations"` iff `records_from_sidecar`."""
    disagreements = []
    for category, spec in sorted(REGISTRY.items()):
        config = examples.get(category)
        if config is None:
            disagreements.append(f"{category}: no example config to resolve")
            continue
        dispatched = resolve(config).source_type == "annotations"
        if dispatched != spec.records_from_sidecar:
            disagreements.append(
                f"{category}: records_from_sidecar={spec.records_from_sidecar} "
                f"but the CLI dispatches source_type="
                f"{resolve(config).source_type!r}"
            )
    assert not disagreements, (
        "the trait and the dispatch disagree:\n  "
        + "\n  ".join(disagreements)
        + "\n\nThese are two statements of one fact. `conventions.py` decides "
        "the source from a hard-coded category, so the trait can be flipped "
        "with nothing but a regenerated JSON noticing -- which is how a "
        "manifest-less category silently regains a labels_csv contract."
    )


def test_the_trait_agrees_with_the_published_contract(examples):
    """`manifest.kind == "none"` iff `ingest.v1.json` rejects csv/json.

    The schema half, and the one whose absence let data-ingestors#552 move
    `ingest.v1.json` without moving `layout.v1.json`.
    """
    validator = Draft7Validator(json.loads(INGEST_SCHEMA.read_text("utf-8")))
    layout = build_layout_contract()["tasks"]

    disagreements = []
    for category, entry in sorted(layout.items()):
        config = examples.get(category)
        if config is None:
            disagreements.append(f"{category}: no example config")
            continue
        # Single-key mutation of a KNOWN-VALID config, so `csv` is the only
        # thing that can have broken it. Re-parsing the schema's if/then/else
        # would assert on a reimplementation of the schema logic instead.
        probe = {k: v for k, v in config.items() if k != "json"}
        probe["csv"] = "data.csv"
        forbids_csv = not validator.is_valid(probe)

        manifest = entry.get("manifest") or {}
        declares_none = manifest.get("kind") == MANIFEST_LESS_KIND
        if declares_none != forbids_csv:
            disagreements.append(
                f"{category}: layout says kind="
                f"{manifest.get('kind')!r} but ingest.v1.json "
                f"{'forbids' if forbids_csv else 'permits'} a csv source"
            )
    assert not disagreements, (
        "the two schemas contradict each other:\n  " + "\n  ".join(disagreements)
    )


def test_both_probes_discriminate(examples):
    """Guard the guard: neither probe may return a constant.

    If `resolve()` started returning one `source_type` for everything, or the
    csv probe collapsed, both tests above would compare a constant to a
    constant and pass having measured nothing.
    """
    kinds = {resolve(c).source_type for c in examples.values()}
    assert len(kinds) > 1, f"resolve() returns only {kinds}"

    traits = {spec.records_from_sidecar for spec in REGISTRY.values()}
    assert traits == {True, False}, f"the trait is constant across the registry: {traits}"
