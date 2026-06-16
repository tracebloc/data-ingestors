"""ModalitySpec — the single source of truth for one task category's
ingestion behavior.

Today "what is modality X" is spread across separate per-category dispatch
sites (validator mapping, file-transfer dispatch, three frozensets in
ingestors/base.py, the conventions groupings, the schema enum). They are kept
consistent only by ``tests/test_category_congruence.py`` — consistency by
test, not by construction. The registry (modalities/registry.py) makes one
``ModalitySpec`` per category the source the call sites are *derived from*, so
they can't drift.

This is delivered in slices (structural refactor — backend#796, phase P3):

- **P3a (this slice):** the three per-category FLAGS that were frozensets in
  ingestors/base.py.
- P3b: the validator factory (replaces utils/validators_mapping.py).
- P3c: the sidecar transfer plan (replaces file_transfer.map_file_transfer;
  folds in the semseg mask sidecar / #136).
- P3d: the conventions defaults (data_format, default file/csv options,
  regression-class flag).

So the spec is intentionally small here and grows over P3b–P3d.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


@dataclass(frozen=True)
class ModalitySpec:
    """Per-category behavior (built up over P3a–P3d). More fields land in P3c/P3d.

    Attributes:
        category: the ``TaskCategory`` value this spec describes.
        data_format: the ``DataFormat`` the framework expects for this category
            (P3d). Read by ``conventions._data_format_for`` (was a 6-frozenset
            ladder there).
        build_validators: ``(file_options) -> [validators]`` — the validator
            set this category runs (P3b). Replaces the corresponding
            ``map_validators`` if/elif arm; the factory bodies live in
            ``modalities/validators.py``.
        transfer: ``(record, file_options, cfg) -> record | None`` — stages
            this category's per-row sidecar file(s); ``None`` for
            non-file-bearing (tabular / time-series) categories (P3c). ``cfg``
            is the run's resolved Config, threaded from ``map_file_transfer``
            so the copy primitives read SRC_PATH / DEST_PATH from it rather
            than a module-global ``Config()`` (P4c). Replaces the
            ``map_file_transfer`` if/elif arm; bodies in
            ``modalities/transfer.py``. Invariant: ``transfer is not None``
            iff ``is_file_bearing`` (pinned by tests).
        is_file_bearing: every record references sidecar files (images,
            annotations, masks, texts, sequences) under ``SRC_PATH`` that must
            be copied to ``DEST_PATH``. Drives both the SRC_PATH preflight and
            the per-record file-transfer gate. Tabular / time-series are False.
        is_tabular_family: a structured-feature table — inject
            ``number_of_columns`` into the backend metadata. Image / text
            categories are False (a column count there would mislead).
        is_self_supervised: no ``label`` column; the model creates its own
            targets at train time (e.g. masked language modeling). The backend
            stores no edge-label metadata, so the edge-label call is skipped
            (#213).
    """

    category: str
    is_file_bearing: bool
    is_tabular_family: bool
    is_self_supervised: bool
    data_format: str
    build_validators: Callable[[Dict[str, Any]], List]
    transfer: Optional[
        Callable[[Dict[str, Any], Dict[str, Any], Any], Optional[Dict[str, Any]]]
    ] = None
