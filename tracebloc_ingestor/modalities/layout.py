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

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# Bump when the contract's SHAPE changes (fields added/removed/reinterpreted),
# not when a task's values change — those are caught by the drift test.
LAYOUT_CONTRACT_VERSION = "1"


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


def _task_layout(spec: Any) -> Dict[str, Any]:
    """Compose one task's layout: the two declared pieces (sidecars,
    record_format) plus the facts DERIVED from the spec's existing flags."""
    return {
        "family": spec.data_format,  # image | text | tabular
        "manifest": {
            # File-bearing tasks list per-row files in a labels CSV (required
            # `filename` column); tabular/time-series have no sidecar files —
            # the data CSV itself is the manifest, with no `filename` column.
            "kind": "labels_csv" if spec.is_file_bearing else "data_csv",
            "requires_filename_column": spec.is_file_bearing,
            # A user-supplied label/target column. Self-supervised text tasks
            # (MLM/CLM/seq2seq/embeddings) have none; everything else does.
            "has_label_column": not spec.is_self_supervised,
        },
        "primary_subdir": spec.file_subdir,  # images | texts | sequences | null
        "sidecars": [_sidecar_dict(s) for s in spec.sidecars],
        "record_format": (
            _record_format_dict(spec.record_format) if spec.record_format else None
        ),
    }


def build_layout_contract() -> Dict[str, Any]:
    """Serialize the registry's per-task layout to a plain, JSON-ready dict —
    the machine-readable contract the CLI vendors + mirrors.

    Imported lazily to avoid a circular import (registry.py imports this
    module's dataclasses)."""
    from .registry import REGISTRY

    tasks = {cat: _task_layout(spec) for cat, spec in REGISTRY.items()}
    return {"version": LAYOUT_CONTRACT_VERSION, "tasks": tasks}


# Path of the committed, machine-readable contract the CLI vendors + mirrors.
# Kept next to ingest.v1.json (both are the CLI's contract surface).
def contract_path() -> "os.PathLike[str]":
    import os

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
