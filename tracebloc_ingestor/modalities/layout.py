"""The per-task local dataset-layout contract (data-ingestors#347).

The ingestor is the source of truth for *what a task's local dataset looks
like* on disk — the manifest CSV, whether it carries a label column, the
primary file subdir, extra sidecar dirs (``annotations/``, ``masks/``), and the
in-``.txt`` record format for the structured text tasks. The CLI used to
hardcode all of this in Go, which is why the CLI-pending tasks couldn't be
staged without re-implementing the ingestor's layout rules — a fork
(RFC-0002 Principle 6).

This module exposes that layout as **machine-readable data** so the CLI's
discovery/staging becomes a *verified mirror* of the ingestor's truth rather
than independent logic:

- ``Sidecar`` / ``RecordFormat`` are the two pieces that AREN'T already implied
  by a category's ``ModalitySpec`` flags; they're declared on the spec
  (``modalities/registry.py``).
- ``build_layout_contract()`` composes the full per-task layout by DERIVING the
  rest from the existing spec flags (``is_file_bearing`` → the manifest lists
  per-row files; ``not is_self_supervised`` → a label column; ``file_subdir`` →
  the primary dir; ``data_format`` → the family), so there is a single source
  and the derived facts can't drift from behaviour.
- ``tests/test_layout_contract.py`` pins the emitted contract against the
  committed ``schema/layout.v1.json`` (regenerate on change) and against the
  spec flags, and the CLI verifies its Go mirror against the same JSON.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

# Bump when the contract's SHAPE changes (fields added/removed/reinterpreted),
# not when a task's values change — those are caught by the drift test.
# "2": added the per-task ``grouping`` block (sequence-grouped categories,
#      backend#1054 Decision-4 — time_series_classification).
# "3": added the per-task ``ordering`` block — the column the ingestor requires
#      to stay monotonic and the scope it holds over (dataset-wide vs per
#      group). Surfaces the time-ordering constraint that used to live only in
#      the validators, so a consumer can read from the contract which tasks are
#      subject to it (backend#1870).
# "4": object_detection gained a MANIFEST-LESS shape — since backend#1006 its
#      records are enumerated from the required ``annotations/*.xml`` sidecar
#      (not a labels CSV) and each label is derived from ``<object><name>``, so
#      its manifest is ``kind="none"`` with no filename/label column. That is a
#      NEW ``kind`` value a consumer must handle (skip the labels-CSV reads), so
#      it is a shape reinterpretation, not just a task's value changing — bumped
#      so downstream version guards fire instead of a consumer silently
#      applying the old ``labels_csv`` shape, which is exactly the drift that
#      made OD ingest impossible from the CLI (backend#3076).
#
#      WHAT THOSE GUARDS ACTUALLY ARE, corrected twice by @LukasWodka after I
#      described them wrongly both times (data-ingestors#557, #558):
#
#        * ``e2e-test-agent``'s ``harness/layout.py`` — ``SUPPORTED_VERSIONS``,
#          refuses an unknown version at load
#        * ``tracebloc/cli``'s ``scripts/check-pin-version.sh``, run by
#          ``pin-version-drift.yml`` (backend#2704) — resolves the pin in
#          ``scripts/.data-ingestors-ref``, compares the CONTRACT VERSION at the
#          pin against this repo's default branch, fails closed on "cannot
#          evaluate", and opens a tracking issue. Its own header records the
#          previous instance: a pin on layout v2 while this repo was on v3.
#
#      I claimed the CLI had no version guard and that ``Manifest.Kind`` was
#      never read there. Both false: ``Kind`` is read at
#      ``layout_contract.go:87`` inside ``HasManifestCSV``, which has four live
#      call sites (``preflight.go:1526``, ``:1584``, ``spec.go:267``,
#      ``walk.go:139``) and is the predicate gating the labels-CSV reads and the
#      discovery walk. The error's DIRECTION is the problem: it understates how
#      much the CLI depends on this shape, and a reader taking it at face value
#      would conclude the ``kind="none"`` branch is dead code.
#
#      The one thing that remains true is narrower and is tracked separately:
#      ``pin-version-drift.yml`` is a WEEKLY cron with no ``pull_request``
#      trigger, so a stale pin does not surface on a PR in either repo.
LAYOUT_CONTRACT_VERSION = "4"


@dataclass(frozen=True)
class Sidecar:
    """An extra per-row directory a file-bearing task needs beyond its primary
    ``file_subdir`` — e.g. object detection's Pascal-VOC ``annotations/*.xml``
    or semantic segmentation's ``masks/*.png``.

    ``link_column`` names the manifest CSV column that ties a row to its
    sidecar file when the link isn't the plain filename stem (semseg's
    ``mask_id`` — backend#816); ``None`` means the sidecar is paired by the
    row's ``filename`` stem.
    """

    subdir: str
    glob: str
    required: bool
    link_column: Optional[str] = None


@dataclass(frozen=True)
class Grouping:
    """Sequence grouping for categories whose sample unit is a GROUP of rows
    rather than a single row (backend#1054 Decision-4 —
    time_series_classification: many timestep rows per ``sequence_id``, one
    label per sequence).

    Declared as a ModalitySpec trait so ``ingestors/base.py`` and the
    validators stay trait-driven (no category if/elses), and a later
    ``time_series_regression`` is a one-entry registry job.

    ``group_column``/``time_column`` are the FIXED physical column names
    (Decision-2); ``count_unit`` names the unit the ingest summary's label
    counts are expressed in (``"sequences"`` = ``COUNT(DISTINCT
    group_column)`` per label — Decision-3/T2; every non-grouped category
    implicitly counts ``"rows"``).
    """

    group_column: str
    time_column: str
    count_unit: str = "sequences"


@dataclass(frozen=True)
class RecordFormat:
    """The structure INSIDE each ``.txt`` for the structured text tasks.

    ``fields`` are the ordered field names separated by ``separator``;
    ``min_fields`` is the fewest that must be present (embeddings accepts an
    optional trailing ``negative``, so ``fields=(anchor,positive,negative)``
    with ``min_fields=2``). ``enforced`` is True only when a structural
    validator rejects a malformed file in-cluster (sentence_pair, embeddings);
    False marks a documented convention the ingestor does NOT reject (seq2seq,
    causal LM accept raw free text), so a mirror must not reject it either.
    """

    separator: str
    fields: Tuple[str, ...]
    min_fields: int
    enforced: bool


def _sidecar_dict(s: Sidecar) -> Dict[str, Any]:
    return {
        "subdir": s.subdir,
        "glob": s.glob,
        "required": s.required,
        "link_column": s.link_column,
    }


def _record_format_dict(rf: RecordFormat) -> Dict[str, Any]:
    return {
        "separator": rf.separator,
        "fields": list(rf.fields),
        "min_fields": rf.min_fields,
        "enforced": rf.enforced,
    }


def _grouping_dict(g: Grouping) -> Dict[str, Any]:
    return {
        "group_column": g.group_column,
        "time_column": g.time_column,
        "count_unit": g.count_unit,
    }


def _ordering(spec: Any, fixed_time_column: Optional[str]) -> Optional[Dict[str, Any]]:
    """The cross-row ordering the ingestor enforces on this task, or ``None``
    when it enforces none (backend#1870).

    ``column`` is the physical column that must be monotonically non-decreasing;
    ``scope`` is ``"per_group"`` when that must hold WITHIN each
    ``grouping.group_column`` (time_series_classification's
    ``PerGroupTimeOrderedValidator``) and ``"dataset"`` when it must hold across
    the whole manifest (time_series_forecasting's global ``TimeOrderedValidator``
    — forecasting is treated as one merged series). A consumer that reshapes or
    grows such a dataset now learns from the contract which column to keep
    monotonic instead of discovering it from a validator rejection.

    Derived, single-source: ``column`` is the category's entry in
    ``registry.FIXED_TIME_COLUMN_BY_CATEGORY`` (the same map preflight rejects a
    decorative ``time_column`` override against), and ``scope`` follows the
    presence of a ``grouping`` trait. Tasks with no fixed ordering column emit
    ``None`` — every non-time-series task, and ``time_to_event_prediction``
    (whose ``time_column`` is a per-row duration, not a monotonic axis, and is
    validated per-row by ``TimeToEventValidator``) — so the contract states,
    rather than hides, that they are not subject to the rule.
    """
    if fixed_time_column is None:
        return None
    return {
        "column": fixed_time_column,
        "scope": "per_group" if spec.grouping is not None else "dataset",
    }


def _manifest(spec: Any) -> Dict[str, Any]:
    """The task's manifest descriptor: where its record list + labels come from.

    ``object_detection`` is the one file-bearing category with NO manifest CSV
    (``spec.records_from_sidecar``): since backend#1006 its records are
    enumerated from the required ``annotations/*.xml`` sidecar (one per image)
    and each label is DERIVED from ``<object><name>``, so there is no labels CSV,
    no ``filename`` column to require, and no user-declared label column. It
    emits ``kind="none"`` with both column flags false, and a consumer keys off
    that to SKIP every labels-CSV read (the CLI's discovery/preflight/spec-build
    mirror) rather than staging a manifest the ingestor never reads (backend#3076).
    """
    if spec.records_from_sidecar:
        return {
            "kind": "none",
            "requires_filename_column": False,
            "has_label_column": False,
        }
    return {
        # File-bearing tasks list per-row files in a labels CSV (required
        # `filename` column); tabular/time-series have no sidecar files —
        # the data CSV itself is the manifest, with no `filename` column.
        "kind": "labels_csv" if spec.is_file_bearing else "data_csv",
        "requires_filename_column": spec.is_file_bearing,
        # A user-supplied label/target column. Self-supervised text tasks
        # (MLM/CLM/seq2seq/embeddings) have none; everything else does.
        "has_label_column": not spec.is_self_supervised,
    }


def _task_layout(spec: Any, fixed_time_column: Optional[str]) -> Dict[str, Any]:
    """Compose one task's layout: the two declared pieces (sidecars,
    record_format) plus the facts DERIVED from the spec's existing flags."""
    return {
        "family": spec.data_format,  # image | text | tabular
        "manifest": _manifest(spec),
        "primary_subdir": spec.file_subdir,  # images | texts | sequences | null
        "sidecars": [_sidecar_dict(s) for s in spec.sidecars],
        "record_format": (
            _record_format_dict(spec.record_format) if spec.record_format else None
        ),
        # Sequence grouping (backend#1054 Decision-4): non-null only for
        # grouped categories (time_series_classification), where one dataset
        # item spans MANY manifest rows keyed by ``group_column`` and the
        # ingest summary counts sequences, not rows.
        "grouping": _grouping_dict(spec.grouping) if spec.grouping else None,
        # Cross-row ordering the ingestor enforces (backend#1870): the column
        # that must stay monotonic and whether that holds dataset-wide or per
        # group. ``None`` for tasks with no such rule, so which tasks it applies
        # to is readable from the contract, not only from the validators.
        "ordering": _ordering(spec, fixed_time_column),
    }


def build_layout_contract() -> Dict[str, Any]:
    """Serialize the registry's per-task layout to a plain, JSON-ready dict —
    the machine-readable contract the CLI vendors + mirrors.

    Imported lazily to avoid a circular import (registry.py imports this
    module's dataclasses)."""
    from .registry import REGISTRY, FIXED_TIME_COLUMN_BY_CATEGORY

    tasks = {
        cat: _task_layout(spec, FIXED_TIME_COLUMN_BY_CATEGORY.get(cat))
        for cat, spec in REGISTRY.items()
    }
    return {"version": LAYOUT_CONTRACT_VERSION, "tasks": tasks}


# Path of the committed, machine-readable contract the CLI vendors + mirrors.
# Kept next to ingest.v1.json (both are the CLI's contract surface).
def contract_path() -> "os.PathLike[str]":
    return os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "schema", "layout.v1.json"
    )


def render_contract() -> str:
    """The canonical serialization (sorted, trailing newline) — the exact bytes
    committed to schema/layout.v1.json and pinned by the drift test."""
    import json

    return json.dumps(build_layout_contract(), indent=2, sort_keys=True) + "\n"


if (
    __name__ == "__main__"
):  # `python -m tracebloc_ingestor.modalities.layout` regenerates the JSON
    with open(contract_path(), "w", encoding="utf-8") as fh:
        fh.write(render_contract())
    print(f"wrote {contract_path()}")
