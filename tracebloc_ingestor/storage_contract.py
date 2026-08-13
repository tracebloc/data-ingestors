"""Canonical ingest → trainer STORAGE contract — the single source of truth.

Where :mod:`tracebloc_ingestor.identifiers` pins what a column may be *called*,
this module pins what the framework columns *mean* and how a stored row maps
back to the sidecar file it describes. Same reason, same shape: it is
deliberately dependency-light (stdlib only, no package imports) so the trainer
(``tracebloc-engine``) can mirror it verbatim — see
``core/utils/ingest_contract.py`` there, and the mirrored case table in
``tests/test_ingest_storage_contract.py`` / ``core/tests/contracts/`` that keeps
the two copies from drifting.

The invariant
-------------
For a file-bearing category the ingestor copies the sidecar to::

    <DEST_PATH>/<manifest filename><extension>

and stores that stem in the **``filename``** column
(``file_transfer.image_transfer``). Therefore:

    **A trainer resolves a row to its file through ``filename``.
    ``data_id`` names nothing on disk.**

``data_id`` is an *independent* row identifier whose value depends on the run's
``data_id`` strategy — a salted content hash (the default since #350), a UUID,
or a copy of some source column. Only ``strategy=column`` over ``filename``
makes the two coincide, and that coincidence is not part of the contract.

Why this module exists (backend#1706, follow-up to tracebloc-engine#615)
------------------------------------------------------------------------
Two of the shipped Python templates — ``keypoint_detection`` and
``semantic_segmentation`` — set ``unique_id_column="filename"``, aliasing
``data_id`` to the on-disk stem. Those are precisely the two categories whose
trainer readers keyed on ``data_id``; the templates were papering over the
reader bug. When #350 flipped the default strategy ``uuid`` → ``content_hash``
nothing failed here, and every CLI/YAML-ingested keypoint or semseg dataset
crashed on the first training batch instead. Three facts had to agree and
nothing pinned any of them. They are pinned here now.
"""

# ── Framework columns every ingested row carries ────────────────────────────
# The stem of the sidecar file on disk. THE column a trainer resolves files
# through.
IMAGE_NAME_COLUMN = "filename"

# The row's opaque identity. Independent of the on-disk name — see the module
# docstring. Never use it to build a path.
ROW_ID_COLUMN = "data_id"

# The extension the sidecar was written with, e.g. ".jpg". Advisory: the
# ingestor may write ``.jpeg`` where a training default says ``.jpg``, so a
# reader probes rather than trusting this cell.
EXTENSION_COLUMN = "extension"

# Per-row semantic-segmentation mask file, resolved the same way as the image
# (backend#816). Present only when the manifest declares it.
MASK_NAME_COLUMN = "mask_id"

# Per-row object-detection annotation payload / pointer.
ANNOTATION_COLUMN = "annotation"

# The columns that can name a file on disk, in the order a reader must try
# them. ``filename`` first because it is what the file is actually named
# after; ``data_id`` remains a fallback ONLY for legacy datasets ingested
# before the ``filename`` column existed, and for template-built datasets
# where the two coincide.
IMAGE_NAME_COLUMNS = (IMAGE_NAME_COLUMN, ROW_ID_COLUMN)

# ── data_id strategies ──────────────────────────────────────────────────────
# Every way an ingest can mint ``data_id`` (``cli/conventions.py``,
# ``RecordProcessor._map_unique_id``).
DATA_ID_STRATEGIES = ("content_hash", "uuid", "column")

# The default since #350 (commit 913dd7a, ships in v0.7.5+): a deterministic
# salted SHA-256 so a retried k8s Job re-claims its rows instead of
# duplicating them. It was ``uuid`` before.
DEFAULT_DATA_ID_STRATEGY = "content_hash"

# Strategies under which ``data_id`` is guaranteed NOT to name a file on disk.
# ``column`` is absent not because it names one but because it *may* — when
# pointed at ``filename`` — which is exactly the coincidence that hid #615.
OPAQUE_DATA_ID_STRATEGIES = frozenset({"content_hash", "uuid"})

# ── On-disk naming ──────────────────────────────────────────────────────────
# Image suffixes the pipeline recognises, lower-cased. Matching is
# case-insensitive on both sides: the ingestor lower-cases before comparing
# (``file_transfer._has_extension``), the trainer probes the upper-cased
# variants too.
RECOGNISED_IMAGE_SUFFIXES = (".jpeg", ".jpg", ".png")


def has_recognised_extension(name: object) -> bool:
    """True when ``name`` already ends in a recognised image suffix (any case).

    Splits on the LAST dot only, so a manifest name that carries dots of its
    own (``image.001.jpg``) is judged on ``.jpg`` alone.
    """
    text = "" if name is None else str(name)
    _head, dot, suffix = text.rpartition(".")
    return bool(dot) and f".{suffix.lower()}" in RECOGNISED_IMAGE_SUFFIXES


def strip_image_extension(name: object) -> str:
    """Return ``name`` without a trailing **recognised** image extension.

    Deliberately neither ``name.split(".")[0]`` nor ``Path(name).stem``: both
    truncate at a dot belonging to the name itself, and manifests do carry
    such names (``image.001.jpg``). The ingestor keeps them whole, so a stem
    that dropped ``.001`` would name no file on disk.
    """
    text = str(name)
    if not has_recognised_extension(text):
        return text
    return text.rpartition(".")[0]


def stored_image_name(filename: object, extension: object) -> str:
    """The basename the ingestor writes for ``filename`` under ``extension``.

    Mirrors ``file_transfer._find_src``'s default (non-forced) mode: a manifest
    value that already carries a recognised extension names that very file and
    keeps it; a bare stem gets ``extension`` appended.
    """
    text = str(filename)
    if has_recognised_extension(text):
        return text
    return f"{text}{extension}"
