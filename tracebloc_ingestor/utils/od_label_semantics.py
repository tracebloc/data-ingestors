"""Object-detection per-image label semantics — THE single decision point.

Dropping ``labels.csv`` for ``object_detection`` (backend#1006) changes the
record model from **one row per bounding box** to **one row per image**. That
change is mechanical everywhere except here: a per-image record has no single
``label``, because one image can contain three cars and a sign.

Everything about "what does a class count MEAN for object detection" lives in
this module. The enumerator (:mod:`tracebloc_ingestor.ingestors.voc_ingestor`)
calls :func:`encode_image_label` and never reasons about class counting itself,
so the pending DS decision is a change to *this file*, not a sweep through the
ingestor.

Two units, deliberately
-----------------------
After #1006 the ingest summary carries two different units on purpose:

* ``record_count`` — **images**. Free: it is ``stats["inserted_records"]``
  (``ingestors/base.py``), and under per-image enumeration a row *is* an image.
  This is the half that dissolves backend#966 — ``record_count`` becomes the
  image count with no new field, and the selection path bounds images.
* ``labels`` — **boxes** (see :func:`decode_image_label`). Class *balance*
  stays comparable with every OD dataset ingested to date, so the federated
  split decisions read off it (backend ``dataset_validators``
  ``_m_label_distribution_skew`` / ``_m_min_class_support``) do not silently
  shift the day a dataset is re-ingested.

This is documented at the point of emission because two units in one payload
reads as a bug otherwise. OD is the *second* non-partitioning category, not the
first: ``token_classification`` already ships ``labels`` that do not partition
rows, which is exactly why backend#2770 added the explicit ``record_count`` and
backend#2787 stopped comparing ``dataset_meta['total']`` against
``sum(count.label)``.

PENDING — the box-vs-presence decision (@divyasinghds, backend#1006)
--------------------------------------------------------------------
If DS rules that OD class balance is defined on *images containing the class*
rather than on boxes, the change is confined to :func:`encode_image_label`:
emit each class once (``collections.Counter`` -> ``dict.fromkeys``) and every
count below becomes a presence count. Nothing else in the enumerator moves.

Why the encoding is ``cls:count`` and not a repeated-class multiset
------------------------------------------------------------------
The obvious encoding — mirroring ``token_classification``'s whitespace-joined
tag sequence, ``"car car car sign"``, and reusing its ``label_is_tag_sequence``
trait for free — **does not fit the column**. ``label`` is
``Column("label", String(255))`` (``database.py``). A VisDrone frame (the
sample this repo bundles for OD) carries well over a hundred objects, so the
repeated form overflows 255 chars and MySQL would either truncate the cell —
silently corrupting the histogram — or raise mid-ingest.

``"car:3 sign:1"`` is bounded by the number of DISTINCT classes in the image
rather than by the number of boxes, which fits comfortably. The cost is a
decoder, which is the ~10 lines below.
"""

from collections import Counter
from typing import Dict, Iterable, List

# MySQL ``label`` column width (``Column("label", String(255))``). Encoding is
# rejected rather than truncated at this bound: a truncated cell decodes into a
# wrong-but-plausible histogram, which is the failure mode that survives review.
MAX_LABEL_LENGTH = 255

# Separator between a class name and its per-image count, and between pairs.
# Class names containing either are rejected up front (see _validate_class_name)
# rather than round-tripping into a corrupted decode.
_PAIR_SEPARATOR = ":"


class ODLabelEncodingError(ValueError):
    """A per-image class multiset cannot be represented in the label column."""


def _validate_class_name(name: str) -> str:
    """Reject class names that would not survive the encode/decode round trip.

    Whitespace would split one class into two on decode; a ``:`` would make the
    count boundary ambiguous. Both are rejected loudly at ingest rather than
    silently producing a histogram whose classes do not match the annotations.
    """
    cleaned = str(name).strip()
    if not cleaned:
        raise ODLabelEncodingError(
            "Object-detection annotation contains an empty <object><name>. "
            "Every <object> must declare a non-empty class name."
        )
    if any(ch.isspace() for ch in cleaned) or _PAIR_SEPARATOR in cleaned:
        raise ODLabelEncodingError(
            f"Object-detection class name {cleaned!r} contains whitespace or "
            f"'{_PAIR_SEPARATOR}', which the per-image label encoding uses as "
            f"separators. Rename the class in the Pascal-VOC XML "
            f"(<object><name>) to remove them and re-ingest."
        )
    return cleaned


def encode_image_label(class_names: Iterable[str]) -> str:
    """Encode one image's object classes into its ``label`` cell.

    THE decision point. ``["car", "car", "car", "sign"]`` -> ``"car:3 sign:1"``.
    Classes are sorted so that two images with the same composition produce the
    same cell, which lets the summary's ``GROUP BY label`` collapse them and
    keeps the decode work proportional to distinct compositions rather than to
    rows.

    Raises:
        ODLabelEncodingError: a class name is unencodable, or the image's class
            set overflows the 255-char column.
    """
    counts = Counter(_validate_class_name(name) for name in class_names)
    encoded = " ".join(f"{cls}{_PAIR_SEPARATOR}{counts[cls]}" for cls in sorted(counts))
    if len(encoded) > MAX_LABEL_LENGTH:
        raise ODLabelEncodingError(
            f"Encoded per-image label is {len(encoded)} chars, over the "
            f"{MAX_LABEL_LENGTH}-char limit of the `label` column "
            f"({len(counts)} distinct classes in one image). Truncating would "
            f"silently corrupt the class histogram, so this is rejected."
        )
    return encoded


def decode_image_label(cell: str) -> Dict[str, int]:
    """Decode one ``label`` cell back to ``{class: box_count}``.

    Total against malformed input: an unparseable pair is skipped rather than
    raising, because this runs in the summary path after the rows are already
    committed — a crash there would leave an ingested dataset unregistered.
    Encoding is the loud side (:func:`encode_image_label`); decoding is the
    forgiving one.
    """
    counts: Dict[str, int] = {}
    if not cell:
        return counts
    for token in str(cell).split():
        cls, sep, raw = token.rpartition(_PAIR_SEPARATOR)
        if not sep or not cls:
            continue
        try:
            counts[cls] = counts.get(cls, 0) + int(raw)
        except (TypeError, ValueError):
            continue
    return counts


def distinct_classes(cells: Iterable[str]) -> List[str]:
    """Sorted distinct class names across encoded cells.

    Used by the diversity gate, which must count distinct CLASSES — an image
    of three cars and a sign is one distinct *cell* but two distinct classes,
    so counting cells would false-reject a genuinely multi-class dataset.
    """
    seen = set()
    for cell in cells:
        seen.update(decode_image_label(cell))
    return sorted(seen)
