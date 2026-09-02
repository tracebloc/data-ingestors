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
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

if TYPE_CHECKING:  # annotations are strings (future import) — type-only import
    from .layout import Grouping, RecordFormat, Sidecar


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
        is_nlp: an NLP text category (text / token classification, MLM). Gates
            NLP-specific ingest handling — e.g. the data-derived text profile
            shipped on the global-metadata channel for tokenizer-fit checks
            (#805). Image / tabular / time-series are False.
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
    is_nlp: bool = False
    # Subdirectory under SRC_PATH holding this category's per-row files
    # (``images`` / ``texts`` / ``sequences``), or ``None`` for non-file-bearing
    # (tabular / time-series) categories. Used by the centralized 0-record guard
    # in ``map_validators``: with a subdir it also cross-checks the CSV's
    # referenced files exist; with ``None`` only the header-only / empty-CSV row
    # check runs.
    file_subdir: Optional[str] = None
    # Classification-family category whose dataset needs >= 2 distinct label
    # values (image / object / semantic / keypoint / tabular / text
    # classification). Gates the centralized LabelDiversityValidator. False for
    # regression / self-supervised / token-classification (per-token BIO) and
    # the time families.
    is_classification: bool = False
    # The ``label`` column holds an ENCODED PER-IMAGE CLASS HISTOGRAM
    # ("car:3 sign:1") rather than one class per row, because the record model
    # is one row per IMAGE and an image has no scalar class (backend#1006).
    # Selects ``get_class_histogram_counts`` in the ingest-summary count path,
    # which decodes each cell instead of GROUP BY-ing the raw string (that would
    # report whole compositions as classes, exactly the token_classification
    # failure of backend#1747, one category over).
    label_is_class_histogram: bool = False
    # The two per-task LAYOUT facts not already implied by the flags above
    # (data-ingestors#347). Everything else about the on-disk layout is derived
    # from the existing flags by ``modalities.layout.build_layout_contract``.
    #
    # sidecars: extra per-row directories beyond ``file_subdir`` —
    #   object_detection's ``annotations/*.xml``, semantic_segmentation's
    #   ``masks/*.png``. Empty for every other category.
    # record_format: the structure inside each ``.txt`` for the structured text
    #   tasks (sentence_pair / seq2seq / embeddings / causal LM); ``None`` when
    #   the file is free text with no field structure the CLI must preview.
    sidecars: Tuple["Sidecar", ...] = ()
    record_format: Optional["RecordFormat"] = None
    # Sequence grouping (backend#1054 Decision-4): set for categories whose
    # SAMPLE UNIT is a group of manifest rows rather than a single row
    # (time_series_classification: many timestep rows per ``sequence_id``,
    # one label per sequence). Consumed trait-style by ``ingestors/base.py``
    # — sequence-unit label counts (``COUNT(DISTINCT group_column)`` per
    # label), the composite ``(group_column, time_column)`` index, and the
    # post-insert group-integrity pass are all gated on ``grouping is not
    # None``, never on the category string. ``None`` for every per-row
    # category.
    grouping: Optional["Grouping"] = None
    # Token-tagging label (backend#1747): the ``label`` column holds a
    # whitespace-joined per-token tag SEQUENCE (token_classification's BIO/IOB2
    # tags, one tag per word — e.g. ``"O B-PER I-PER O"``), not a single class
    # value. The dataset's output_classes are therefore the DISTINCT TAGS, which
    # ``ingestors/base.py`` counts by EXPLODING each sequence
    # (``Database.get_tag_counts``) — a plain ``GROUP BY label``
    # (``get_label_counts``) would instead count distinct sequence STRINGS as
    # classes, so no model head links and the task is unrunnable e2e. Consumed
    # trait-style (never on the category string), so a future tag-sequence
    # category is a one-line registry entry. ``False`` for every other category.
    label_is_tag_sequence: bool = False
