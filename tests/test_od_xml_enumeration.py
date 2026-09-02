"""backend#1006 — object_detection enumerated from Pascal-VOC XML, one record
per IMAGE, with no labels.csv.

The guards here are keyed to the three things that actually change: the record
CARDINALITY (boxes -> images), the per-image LABEL semantics (a record now
carries a class multiset, not a scalar), and the schema's data-source rule
(object_detection declares neither csv nor json, while every other category
keeps the exactly-one guarantee).
"""

import json
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import MagicMock

import jsonschema
import pytest

from tracebloc_ingestor.ingestors.voc_ingestor import VOCIngestor
from tracebloc_ingestor.modalities.registry import REGISTRY
from tracebloc_ingestor.utils.constants import TaskCategory
from tracebloc_ingestor.utils.od_label_semantics import (
    MAX_LABEL_LENGTH,
    ODLabelEncodingError,
    decode_image_label,
    distinct_classes,
    encode_image_label,
)

SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "tracebloc_ingestor"
    / "schema"
    / "ingest.v1.json"
)


# ---------------------------------------------------------------------------
# Label semantics — the single decision point
# ---------------------------------------------------------------------------


def test_encode_counts_boxes_not_presence():
    """The recommended unit: three cars are three, not one.

    This is the assertion that pins the box-vs-presence decision. If DS rules
    for image-presence counts, THIS is the test that must change — which is the
    point of keeping the decision in one module.
    """
    assert encode_image_label(["car", "car", "car", "sign"]) == "car:3 sign:1"
    assert decode_image_label("car:3 sign:1") == {"car": 3, "sign": 1}


def test_encoding_is_order_independent_and_canonical():
    """Two images with the same composition must produce the SAME cell, or the
    summary's GROUP BY label cannot collapse them and the decode work stops
    being proportional to distinct compositions."""
    assert encode_image_label(["sign", "car", "car"]) == encode_image_label(
        ["car", "sign", "car"]
    )


def test_dense_image_fits_the_column_where_a_repeated_multiset_would_not():
    """The reason the encoding is ``cls:count`` rather than ``"car car car"``.

    The bundled VisDrone frame carries 128 objects; repeating class names would
    produce ~1000 chars against a VARCHAR(255) ``label`` column — MySQL would
    truncate (silently corrupting the histogram) or raise mid-ingest.
    """
    classes = ["car"] * 100 + ["pedestrian"] * 28
    encoded = encode_image_label(classes)
    assert len(encoded) <= MAX_LABEL_LENGTH
    assert len(" ".join(classes)) > MAX_LABEL_LENGTH
    assert decode_image_label(encoded) == {"car": 100, "pedestrian": 28}


def test_overflow_is_rejected_rather_than_truncated():
    """A truncated cell decodes into a wrong-but-plausible histogram, which is
    the failure mode that survives review. Fail loudly instead."""
    with pytest.raises(ODLabelEncodingError, match="over the"):
        encode_image_label([f"class_{i}" for i in range(60)])


@pytest.mark.parametrize("bad", ["traffic light", "a:b", "  ", "tab\tname"])
def test_unencodable_class_names_are_rejected(bad):
    """Whitespace splits one class into two on decode; ':' makes the count
    boundary ambiguous. Both must fail at ingest, not round-trip corrupted."""
    with pytest.raises(ODLabelEncodingError):
        encode_image_label([bad])


def test_decode_is_total_against_malformed_cells():
    """Decoding runs in the summary path AFTER rows are committed, so a raise
    there would leave an ingested dataset unregistered."""
    assert decode_image_label("garbage :5 x:notanint car:2") == {"car": 2}
    assert decode_image_label("") == {}


def test_distinct_classes_counts_classes_not_compositions():
    """The >= 2-class gate's real question.

    A dataset whose every image is 'cars and signs' has ONE distinct cell but
    TWO distinct classes. Counting cells would false-reject it.
    """
    cells = ["car:3 sign:1", "car:1 sign:2"]
    assert len(set(cells)) == 2
    assert distinct_classes(cells) == ["car", "sign"]
    assert distinct_classes(["car:9", "car:2"]) == ["car"]


# ---------------------------------------------------------------------------
# The enumerator
# ---------------------------------------------------------------------------


def _write_voc(directory: Path, stem: str, classes, filename=None):
    root = ET.Element("annotation")
    name_el = ET.SubElement(root, "filename")
    name_el.text = filename if filename is not None else f"{stem}.jpg"
    for cls in classes:
        obj = ET.SubElement(root, "object")
        ET.SubElement(obj, "name").text = cls
    path = directory / f"{stem}.xml"
    ET.ElementTree(root).write(path)
    return path


def _ingestor(tmp_path):
    database = MagicMock()
    database.config.SRC_PATH = str(tmp_path)
    return VOCIngestor(
        database=database,
        api_client=MagicMock(),
        table_name="t",
        category=TaskCategory.OBJECT_DETECTION,
        label_column="image_label",
        intent="train",
    )


def test_one_record_per_image_not_per_box(tmp_path):
    """The cardinality change this ticket exists for."""
    ann = tmp_path / "annotations"
    ann.mkdir()
    _write_voc(ann, "a", ["car", "car", "car", "sign"])
    _write_voc(ann, "b", ["car"])

    records = list(_ingestor(tmp_path).read_data(str(ann)))

    assert len(records) == 2, "one record per IMAGE, not one per bounding box"
    assert records[0] == {"filename": "a.jpg", "image_label": "car:3 sign:1"}
    # Boxes are preserved in the label, so the histogram can still count them.
    assert sum(decode_image_label(r["image_label"])["car"] for r in records) == 4


def test_count_records_counts_annotation_files(tmp_path):
    ann = tmp_path / "annotations"
    ann.mkdir()
    for i in range(3):
        _write_voc(ann, f"img{i}", ["car"])
    assert _ingestor(tmp_path)._count_records(str(ann)) == 3


def test_enumeration_order_is_stable(tmp_path):
    """content_hash retry-idempotency and reproducible batching both rely on a
    re-run enumerating identically."""
    ann = tmp_path / "annotations"
    ann.mkdir()
    for stem in ("c", "a", "b"):
        _write_voc(ann, stem, ["car"])
    ing = _ingestor(tmp_path)
    first = [r["filename"] for r in ing.read_data(str(ann))]
    second = [r["filename"] for r in ing.read_data(str(ann))]
    assert first == second == ["a.jpg", "b.jpg", "c.jpg"]


def test_filename_is_reduced_to_a_basename(tmp_path):
    """The XML is user-supplied. The enumerator must not manufacture a
    traversing filename for the transfer step to resolve — independent of
    ``file_transfer._safe_join``, which rejects one (data-ingestors#239)."""
    ann = tmp_path / "annotations"
    ann.mkdir()
    _write_voc(ann, "evil", ["car"], filename="../../../../etc/passwd")
    (record,) = list(_ingestor(tmp_path).read_data(str(ann)))
    assert record["filename"] == "passwd"
    assert ".." not in record["filename"]


def test_filename_falls_back_to_the_xml_stem(tmp_path):
    """``<filename>`` absent or unusable ⇒ the documented ``{image_name}.xml``
    pairing that FilePairingValidator already enforces."""
    ann = tmp_path / "annotations"
    ann.mkdir()
    _write_voc(ann, "img7", ["car"], filename="")
    _write_voc(ann, "img8", ["car"], filename="..")
    names = sorted(r["filename"] for r in _ingestor(tmp_path).read_data(str(ann)))
    assert names == ["img7", "img8"]


def test_unparseable_and_objectless_annotations_are_skipped(tmp_path):
    """PascalVOCXMLValidator is what fails a genuinely broken set; the
    enumerator skips rather than aborting a whole run."""
    ann = tmp_path / "annotations"
    ann.mkdir()
    _write_voc(ann, "good", ["car"])
    _write_voc(ann, "empty", [])
    (ann / "broken.xml").write_text("<annotation><unclosed>", encoding="utf-8")

    records = list(_ingestor(tmp_path).read_data(str(ann)))
    assert [r["filename"] for r in records] == ["good.jpg"]


def test_missing_annotations_directory_names_the_path_it_was_given(tmp_path):
    """An explicit source that does not exist must not silently fall back to
    SRC_PATH — that reports a directory the caller never named."""
    ing = _ingestor(tmp_path)
    with pytest.raises(FileNotFoundError, match="nope"):
        list(ing.read_data(str(tmp_path / "nope")))


def test_diversity_gate_rejects_a_single_class_dataset(tmp_path):
    ann = tmp_path / "annotations"
    ann.mkdir()
    _write_voc(ann, "a", ["car", "car"])
    _write_voc(ann, "b", ["car"])
    ing = _ingestor(tmp_path)
    list(ing.read_data(str(ann)))
    with pytest.raises(ValueError, match="at least 2"):
        ing._validate_label_diversity(str(ann))


def test_diversity_gate_accepts_multi_class_images(tmp_path):
    """The regression the naive 'distinct label cells' reading would cause: an
    every-image-is-car-and-sign dataset is multi-class and must pass."""
    ann = tmp_path / "annotations"
    ann.mkdir()
    _write_voc(ann, "a", ["car", "sign"])
    _write_voc(ann, "b", ["car", "sign"])
    ing = _ingestor(tmp_path)
    list(ing.read_data(str(ann)))
    ing._validate_label_diversity(str(ann))  # must not raise


# ---------------------------------------------------------------------------
# Registry trait + summary counting
# ---------------------------------------------------------------------------


def test_object_detection_declares_the_class_histogram_trait():
    """The trait is what selects ``get_class_histogram_counts`` over
    ``get_label_counts`` — without it the summary reports whole compositions as
    classes (the token_classification failure of backend#1747)."""
    assert REGISTRY.get(TaskCategory.OBJECT_DETECTION).label_is_class_histogram
    assert not REGISTRY.get(TaskCategory.IMAGE_CLASSIFICATION).label_is_class_histogram


def test_class_histogram_counts_weight_each_composition_by_its_row_count():
    """Two images of 'car:3 sign:1' contribute six cars, not three."""
    from tracebloc_ingestor.database import Database

    db = Database.__new__(Database)
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        ("car:3 sign:1", 2),
        ("car:1", 5),
        (None, 9),
    ]
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    db.engine = engine

    assert db.get_class_histogram_counts("tbl", "ing") == {"car": 11, "sign": 2}


# ---------------------------------------------------------------------------
# Schema — relax for object_detection WITHOUT relaxing anything else
# ---------------------------------------------------------------------------


def _validator():
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return jsonschema.validators.validator_for(schema)(schema)


def _doc(category, **extra):
    doc = {
        "apiVersion": "tracebloc.io/v1",
        "kind": "IngestConfig",
        "category": category,
        "table": "t",
        "intent": "train",
    }
    doc.update(extra)
    return doc


OD_DIRS = {"images": "/d/images/", "annotations": "/d/annotations/"}


def test_object_detection_needs_no_data_source_and_no_label():
    assert not list(_validator().iter_errors(_doc("object_detection", **OD_DIRS)))


@pytest.mark.parametrize("source", [{"csv": "l.csv"}, {"json": "l.json"}])
def test_object_detection_rejects_a_stale_manifest(source):
    """Rejected, not ignored — a leftover labels.csv must fail loudly rather
    than being silently dropped while the XML is enumerated behind it."""
    errors = list(
        _validator().iter_errors(_doc("object_detection", **OD_DIRS, **source))
    )
    assert errors


@pytest.mark.parametrize(
    "category,extra",
    [
        ("image_classification", {"images": "/d/i/", "label": "l"}),
        ("semantic_segmentation", {"images": "/d/i/", "masks": "/d/m/", "label": "l"}),
        ("token_classification", {"texts": "/d/t/", "label": "l"}),
    ],
)
def test_other_categories_still_require_exactly_one_data_source(category, extra):
    """The half that matters: widening for object_detection must not turn a
    missing data source into a legal config anywhere else."""
    v = _validator()
    assert list(v.iter_errors(_doc(category, **extra))), "no source must stay invalid"
    assert not list(v.iter_errors(_doc(category, csv="l.csv", **extra)))
    assert list(
        v.iter_errors(_doc(category, csv="l.csv", **{"json": "l.json"}, **extra))
    ), "both sources must stay invalid"
