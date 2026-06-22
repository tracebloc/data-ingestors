"""End-to-end vision preflight: image_classification now rejects the 0-record
and missing-label-column gaps that the NLP-only #303/#313 fixes had left open
for vision. Drives the REAL `map_validators` chain (minus the DB-bound
TableName/Duplicate validators) on staged image datasets.
"""

from __future__ import annotations

import os

from PIL import Image

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.utils.constants import TaskCategory
from tracebloc_ingestor.utils.validators_mapping import map_validators

_SKIP = {"TableNameValidator", "DuplicateValidator"}  # need a DB / not data-hygiene


def _img(d, name, size=(64, 64)):
    os.makedirs(os.path.join(d, "images"), exist_ok=True)
    Image.new("RGB", size, (120, 120, 120)).save(os.path.join(d, "images", name))


def _first_rejecter(tmp_path, csv_text, images=("a.jpeg", "b.jpeg")):
    d = str(tmp_path)
    for n in images:
        _img(d, n)
    csv = tmp_path / "labels.csv"
    csv.write_text(csv_text, encoding="utf-8")
    cfg = Config(SRC_PATH=d, TABLE_NAME="t")
    opts = {"extension": ".jpeg", "target_size": [64, 64], "label_column": "label"}
    for v in map_validators(TaskCategory.IMAGE_CLASSIFICATION, opts, cfg):
        if type(v).__name__ in _SKIP:
            continue
        r = v.validate(str(csv))
        if not r.is_valid:
            return type(v).__name__
    return None


def test_image_valid_passes(tmp_path):
    assert _first_rejecter(tmp_path, "filename,label\na.jpeg,cat\nb.jpeg,dog\n") is None


def test_image_empty_csv_rejected_at_preflight(tmp_path):
    # header-only CSV -> 0 records; previously ingested an orphan empty table.
    assert _first_rejecter(tmp_path, "filename,label\n") == "IngestableRecordsValidator"


def test_image_missing_label_column_rejected_at_preflight(tmp_path):
    # no label column -> previously ingested label=None -> late backend 400.
    assert (
        _first_rejecter(tmp_path, "filename\na.jpeg\nb.jpeg\n")
        == "LabelColumnValidator"
    )
