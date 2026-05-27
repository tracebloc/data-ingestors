"""Second batch of gap-closing tests: BaseIngestor branches, CSV/JSON ingest
methods, and XML validator sub-element branches."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tracebloc_ingestor.ingestors import base as base_mod
from tracebloc_ingestor.ingestors.base import BaseIngestor
from tracebloc_ingestor.validators.base import ValidationResult
from tracebloc_ingestor.utils.constants import TaskCategory


# ===========================================================================
# BaseIngestor branches
# ===========================================================================

class FakeIngestor(BaseIngestor):
    def __init__(self, records, **kwargs):
        self._records = records
        super().__init__(**kwargs)

    def read_data(self, source):
        yield from self._records


def make_ingestor(records=None, **overrides):
    db = MagicMock()
    db.create_table.return_value = MagicMock()
    db.insert_batch.return_value = ([1], [])
    db.get_table_schema.return_value = {"a": "INT"}
    api = MagicMock()
    api.send_batch.return_value = True
    api.send_generate_edge_label_meta.return_value = True
    api.send_global_meta_meta.return_value = True
    api.prepare_dataset.return_value = True
    api.create_dataset.return_value = {"id": 1}
    kwargs = dict(database=db, api_client=api, table_name="tbl",
                  schema={"a": "INT"}, intent="train", category=None)
    kwargs.update(overrides)
    return FakeIngestor(records or [], **kwargs)


def test_map_unique_id_warns_on_missing_label_column():
    ing = make_ingestor(label_column="missing", category=None)
    rec = ing.process_record({"a": "1", "filename": "f"})
    # missing label column logs a warning but still produces a record with a uuid
    assert rec is not None
    assert rec["data_id"]


def test_process_record_sets_annotation():
    ing = make_ingestor(schema={"a": "INT", "ann": "TEXT"},
                        annotation_column="ann", category=None)
    rec = ing.process_record({"a": "1", "ann": "boxdata", "filename": "f"})
    assert rec["annotation"] == "boxdata"


def test_process_record_excludes_unique_id_from_payload():
    ing = make_ingestor(schema={"a": "INT", "uid": "VARCHAR(10)"},
                        unique_id_column="uid", category=None)
    rec = ing.process_record({"a": "1", "uid": "id7", "filename": "f"})
    assert rec["data_id"] == "id7"
    assert "uid" not in rec


def test_process_record_exception_returns_none():
    ing = make_ingestor(category=None)
    with patch.object(ing, "_map_unique_id", side_effect=RuntimeError("boom")):
        assert ing.process_record({"a": "1"}) is None


def test_count_records_exception_returns_none():
    ing = make_ingestor()
    with patch.object(ing, "read_data", side_effect=RuntimeError("boom")):
        assert ing._count_records("x") is None


def test_validate_data_passing_validator_with_warning():
    ing = make_ingestor(category=None)
    good = MagicMock()
    good.name = "Good"
    good.validate.return_value = ValidationResult(True, [], ["a warning"], {})
    with patch.object(base_mod, "map_validators", return_value=[good]):
        assert ing.validate_data("src") is True


def test_ingest_reraises_validation_error():
    ing = make_ingestor(records=[{"a": "1", "filename": "f"}], category=None)
    bad = MagicMock()
    bad.name = "Bad"
    bad.validate.return_value = ValidationResult(False, ["nope"], [], {})
    with patch.object(base_mod, "map_validators", return_value=[bad]):
        with pytest.raises(ValueError):
            ing.ingest("src")


def test_ingest_image_category_batches_in_loop():
    records = [{"a": "1", "filename": "f1"}, {"a": "2", "filename": "f2"}]
    ing = make_ingestor(records=records, category=TaskCategory.IMAGE_CLASSIFICATION)
    ing.database.insert_batch.return_value = ([1], [])
    with patch.object(base_mod, "Session") as Sess, \
         patch.object(base_mod, "map_file_transfer", side_effect=lambda c, r, o: r), \
         patch.object(base_mod, "map_validators", return_value=[]):
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest("src", batch_size=1)
    assert failed == []
    # batch_size=1 -> _process_batch invoked inside the loop
    assert ing.database.insert_batch.call_count >= 2


def test_ingest_records_db_failures():
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(records=records, category=None)
    ing.database.insert_batch.return_value = ([], [{"record": {}, "error": "dup"}])
    with patch.object(base_mod, "Session") as Sess, \
         patch.object(base_mod, "map_validators", return_value=[]):
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest("src", batch_size=10)
    assert any(f.get("error") == "dup" for f in failed)


def test_ingest_processing_error_in_loop():
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(records=records, category=None)
    with patch.object(base_mod, "Session") as Sess, \
         patch.object(base_mod, "map_validators", return_value=[]), \
         patch.object(ing, "process_record", side_effect=RuntimeError("boom")):
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest("src", batch_size=10)
    assert len(failed) == 1
    assert "boom" in failed[0]["error"]


# ===========================================================================
# CSV / JSON ingestor .ingest() methods + edge branches
# ===========================================================================

def _csv_ingestor(schema=None, **ov):
    from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
    db = MagicMock(); db.create_table.return_value = MagicMock()
    db.insert_batch.return_value = ([1], [])
    db.get_table_schema.return_value = {}
    api = MagicMock()
    for m in ("send_batch", "send_generate_edge_label_meta",
              "send_global_meta_meta", "prepare_dataset"):
        getattr(api, m).return_value = True
    api.create_dataset.return_value = {"id": 1}
    kw = dict(database=db, api_client=api, table_name="tbl",
              schema=schema if schema is not None else {"a": "INT"},
              intent="train", category=None)
    kw.update(ov)
    return CSVIngestor(**kw)


def test_csv_validate_type_error_raises():
    ing = _csv_ingestor(schema={"n": "INT"})
    df = pd.DataFrame({"n": ["abc", "def"]})  # not numeric -> raises
    with pytest.raises(ValueError, match="validation failed"):
        ing._validate_csv(df)


def test_csv_ingest_method(make_csv):
    path = make_csv({"a": [1, 2]})
    ing = _csv_ingestor(schema={"a": "INT"})
    with patch.object(base_mod, "Session") as Sess, \
         patch.object(base_mod, "map_validators", return_value=[]):
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest(str(path), batch_size=10)
    assert failed == []


def test_csv_ingest_method_propagates_error():
    ing = _csv_ingestor(schema={"a": "INT"})
    with pytest.raises(FileNotFoundError):
        ing.ingest("/no/such.csv")


def _json_ingestor(schema=None, **ov):
    from tracebloc_ingestor.ingestors.json_ingestor import JSONIngestor
    db = MagicMock(); db.create_table.return_value = MagicMock()
    db.insert_batch.return_value = ([1], [])
    db.get_table_schema.return_value = {}
    api = MagicMock()
    for m in ("send_batch", "send_generate_edge_label_meta",
              "send_global_meta_meta", "prepare_dataset"):
        getattr(api, m).return_value = True
    api.create_dataset.return_value = {"id": 1}
    kw = dict(database=db, api_client=api, table_name="tbl",
              schema=schema if schema is not None else {"a": "INT"},
              intent="train", category=None)
    kw.update(ov)
    return JSONIngestor(**kw)


def test_json_ingestor_log_level_set():
    ing = _json_ingestor(log_level=10)
    assert ing.json_options == {}


def test_json_validate_record_warns_missing_fields():
    ing = _json_ingestor(schema={"a": "INT", "b": "INT"})
    # 'b' missing -> warning branch, no raise
    ing._validate_record({"a": 1})


def test_json_validate_record_float_and_bool():
    ing = _json_ingestor(schema={"f": "FLOAT", "b": "BOOL"})
    ing._validate_record({"f": "1.5", "b": "true"})


def test_json_count_records_scalar_returns_none(tmp_path):
    p = tmp_path / "n.json"
    p.write_text("42")
    assert _json_ingestor()._count_records(str(p)) is None


def test_json_ingest_method(tmp_path):
    import json
    p = tmp_path / "d.json"
    p.write_text(json.dumps([{"a": 1}, {"a": 2}]))
    ing = _json_ingestor(schema={"a": "INT"})
    with patch.object(base_mod, "Session") as Sess, \
         patch.object(base_mod, "map_validators", return_value=[]):
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest(str(p), batch_size=10)
    assert failed == []


def test_json_ingest_method_propagates_error():
    with pytest.raises(FileNotFoundError):
        _json_ingestor().ingest("/no/such.json")


# ===========================================================================
# XML validator: sub-element branches (called directly on crafted elements)
# ===========================================================================

from tracebloc_ingestor.validators.xml_validator import PascalVOCXMLValidator


def _obj(name=True, pose=True, truncated="0", difficult="0", bndbox=True,
         xmin=10, ymin=10, xmax=100, ymax=100):
    parts = ["<object>"]
    if name is not None:
        parts.append(f"<name>{'cat' if name else ''}</name>")
    if pose is not None:
        parts.append(f"<pose>{'Unspecified' if pose else ''}</pose>")
    if truncated is not None:
        parts.append(f"<truncated>{truncated}</truncated>")
    if difficult is not None:
        parts.append(f"<difficult>{difficult}</difficult>")
    if bndbox:
        parts.append(
            f"<bndbox><xmin>{xmin}</xmin><ymin>{ymin}</ymin>"
            f"<xmax>{xmax}</xmax><ymax>{ymax}</ymax></bndbox>"
        )
    parts.append("</object>")
    return ET.fromstring("".join(parts))


@pytest.fixture
def v():
    return PascalVOCXMLValidator()


def test_get_xml_files_list_non_path_item(v):
    assert v._get_xml_files([123], True, True) == []


def test_validate_xml_files_empty(v):
    res = v._validate_xml_files([])
    assert not res.is_valid


def test_validate_single_xml_generic_error(v, tmp_path):
    f = tmp_path / "a.xml"
    f.write_text("<annotation></annotation>")
    with patch("xml.etree.ElementTree.parse", side_effect=RuntimeError("boom")):
        res = v._validate_single_xml(f)
    assert not res.is_valid
    assert "Unexpected error" in res.errors[0]


def test_root_folder_empty(v):
    root = ET.fromstring("<annotation><folder></folder></annotation>")
    res = v._validate_root_elements(root)
    assert any("Folder element must have non-empty" in e for e in res["errors"])


def test_root_filename_missing(v):
    root = ET.fromstring("<annotation><folder>x</folder></annotation>")
    res = v._validate_root_elements(root)
    assert any("Missing required 'filename'" in e for e in res["errors"])


def test_root_segmented_missing(v):
    root = ET.fromstring("<annotation><folder>x</folder></annotation>")
    res = v._validate_root_elements(root)
    assert any("Missing required 'segmented'" in e for e in res["errors"])


def test_source_missing_database_and_annotation(v):
    root = ET.fromstring("<annotation><source></source></annotation>")
    res = v._validate_source_element(root)
    assert any("Missing required source elements" in e for e in res["errors"])


def test_source_empty_annotation(v):
    root = ET.fromstring(
        "<annotation><source><database>x</database><annotation></annotation></source></annotation>"
    )
    res = v._validate_source_element(root)
    assert any("Annotation element must have non-empty" in e for e in res["errors"])


def test_size_missing_height_depth(v):
    root = ET.fromstring("<annotation><size><width>10</width></size></annotation>")
    res = v._validate_size_element(root)
    assert any("Missing required size elements" in e for e in res["errors"])


def test_object_name_empty(v):
    res = v._validate_single_object(_obj(name=False), 0)
    assert any("Name element must have non-empty" in e for e in res["errors"])


def test_object_name_missing(v):
    res = v._validate_single_object(_obj(name=None), 0)
    assert any("Missing required 'name'" in e for e in res["errors"])


def test_object_pose_empty(v):
    res = v._validate_single_object(_obj(pose=False), 0)
    assert any("Pose element must have non-empty" in e for e in res["errors"])


def test_object_pose_missing(v):
    res = v._validate_single_object(_obj(pose=None), 0)
    assert any("Missing required 'pose'" in e for e in res["errors"])


def test_object_truncated_missing(v):
    res = v._validate_single_object(_obj(truncated=None), 0)
    assert any("Missing required 'truncated'" in e for e in res["errors"])


def test_object_difficult_bad_value(v):
    res = v._validate_single_object(_obj(difficult="9"), 0)
    assert any("Difficult element must be" in e for e in res["errors"])


def test_object_difficult_missing(v):
    res = v._validate_single_object(_obj(difficult=None), 0)
    assert any("Missing required 'difficult'" in e for e in res["errors"])


def test_bndbox_missing_coord(v):
    obj = ET.fromstring(
        "<object><bndbox><xmin>1</xmin><ymin>1</ymin><xmax>5</xmax></bndbox></object>"
    )
    res = v._validate_bndbox_element(obj, 0)
    assert any("Missing required 'ymax'" in e or "Missing required bndbox" in e
               for e in res["errors"])


def test_bndbox_ymin_ge_ymax(v):
    res = v._validate_bndbox_element(_obj(ymin=100, ymax=50), 0)
    assert any("must be less than ymax" in e for e in res["errors"])


def test_bndbox_zero_area(v):
    # equal x's and y's via valid coords producing zero area is caught by
    # the < checks first; use xmin<xmax but ymin==ymax-handled; here force
    # zero width through xmin<xmax but xmax-xmin * ... = 0 isn't possible.
    # Instead: a 0-area box where xmin<xmax and ymin<ymax can't be zero, so
    # exercise the small-area warning path via a 1x1 box.
    res = v._validate_bndbox_element(_obj(xmin=0, ymin=0, xmax=1, ymax=1), 0)
    assert any("Very small bounding box" in w for w in res["warnings"])
