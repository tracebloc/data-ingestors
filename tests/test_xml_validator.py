"""Tests for PascalVOCXMLValidator — full VOC structure compliance."""

from __future__ import annotations

import xml.etree.ElementTree as ET

import pytest

from tracebloc_ingestor.validators.xml_validator import PascalVOCXMLValidator

# ---------------------------------------------------------------------------
# Builders for valid / customizable VOC XML.
# ---------------------------------------------------------------------------


def _object_xml(
    name="cat",
    pose="Unspecified",
    truncated="0",
    difficult="0",
    xmin=10,
    ymin=10,
    xmax=100,
    ymax=100,
):
    return f"""
  <object>
    <name>{name}</name>
    <pose>{pose}</pose>
    <truncated>{truncated}</truncated>
    <difficult>{difficult}</difficult>
    <bndbox>
      <xmin>{xmin}</xmin>
      <ymin>{ymin}</ymin>
      <xmax>{xmax}</xmax>
      <ymax>{ymax}</ymax>
    </bndbox>
  </object>"""


def _voc_xml(objects=None, width=640, height=480, depth=3, segmented="0"):
    if objects is None:
        objects = [_object_xml()]
    return f"""<annotation>
  <folder>images</folder>
  <filename>img.jpg</filename>
  <source>
    <database>Unknown</database>
    <annotation>PASCAL VOC</annotation>
  </source>
  <size>
    <width>{width}</width>
    <height>{height}</height>
    <depth>{depth}</depth>
  </size>
  <segmented>{segmented}</segmented>
{"".join(objects)}
</annotation>"""


@pytest.fixture
def validator():
    return PascalVOCXMLValidator()


# ---------------------------------------------------------------------------
# validate() flow (reads config.SRC_PATH directory)
# ---------------------------------------------------------------------------


def test_valid_file_passes(clean_env, tmp_path, validator):
    (tmp_path / "ann.xml").write_text(_voc_xml(), encoding="utf-8")
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = validator.validate(None)
    assert result.is_valid, result.errors
    assert result.metadata["valid_files"] == 1


def test_no_xml_files_fails(clean_env, tmp_path, validator):
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = validator.validate(None)
    assert not result.is_valid
    assert "No XML files found" in result.errors[0]


def test_invalid_file_reports_errors(clean_env, tmp_path, validator):
    bad = _voc_xml(objects=[_object_xml(xmin=100, xmax=100)])  # zero width box
    (tmp_path / "ann.xml").write_text(bad, encoding="utf-8")
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = validator.validate(None)
    assert not result.is_valid
    assert result.metadata["invalid_files"] == 1


def test_nonexistent_src_path_fails(clean_env, validator):
    clean_env.setenv("SRC_PATH", "/definitely/not/here")
    result = validator.validate(None)
    assert not result.is_valid


# ---------------------------------------------------------------------------
# _get_xml_files
# ---------------------------------------------------------------------------


def test_get_xml_files_single_file(tmp_path, validator):
    f = tmp_path / "a.xml"
    f.write_text(_voc_xml(), encoding="utf-8")
    files = validator._get_xml_files(str(f), True, True)
    assert files == [f]


def test_get_xml_files_list_input(tmp_path, validator):
    f = tmp_path / "a.xml"
    f.write_text(_voc_xml(), encoding="utf-8")
    files = validator._get_xml_files(
        [str(f), str(tmp_path / "missing.xml")], True, True
    )
    assert files == [f]


def test_get_xml_files_unsupported_type_raises(validator):
    with pytest.raises(ValueError):
        validator._get_xml_files(123, True, True)


def test_get_xml_files_ignores_hidden(tmp_path, validator):
    (tmp_path / ".hidden.xml").write_text(_voc_xml(), encoding="utf-8")
    visible = tmp_path / "v.xml"
    visible.write_text(_voc_xml(), encoding="utf-8")
    files = validator._get_xml_files(str(tmp_path), True, True)
    assert visible in files
    assert all(not p.name.startswith(".") for p in files)


# ---------------------------------------------------------------------------
# _validate_single_xml + sub-validators
# ---------------------------------------------------------------------------


def test_single_xml_valid(tmp_path, validator):
    f = tmp_path / "a.xml"
    f.write_text(_voc_xml(), encoding="utf-8")
    result = validator._validate_single_xml(f)
    assert result.is_valid, result.errors


def test_wrong_root_tag_fails(tmp_path, validator):
    f = tmp_path / "a.xml"
    f.write_text("<dataset></dataset>", encoding="utf-8")
    result = validator._validate_single_xml(f)
    assert not result.is_valid
    assert "Root element must be 'annotation'" in result.errors[0]


def test_parse_error_fails(tmp_path, validator):
    f = tmp_path / "a.xml"
    f.write_text("<annotation><unclosed>", encoding="utf-8")
    result = validator._validate_single_xml(f)
    assert not result.is_valid
    assert "XML parsing error" in result.errors[0]


def _root(xml_text):
    return ET.fromstring(xml_text)


def test_root_elements_missing():
    v = PascalVOCXMLValidator()
    root = _root("<annotation><filename>x.jpg</filename></annotation>")
    res = v._validate_root_elements(root)
    assert any("Missing required root elements" in e for e in res["errors"])


def test_root_empty_filename_fails():
    v = PascalVOCXMLValidator()
    root = _root(
        _voc_xml().replace("<filename>img.jpg</filename>", "<filename></filename>")
    )
    res = v._validate_root_elements(root)
    assert any("Filename element must have non-empty" in e for e in res["errors"])


def test_root_bad_segmented_fails():
    v = PascalVOCXMLValidator()
    root = _root(_voc_xml(segmented="2"))
    res = v._validate_root_elements(root)
    assert any("Segmented element must be" in e for e in res["errors"])


def test_source_missing_fails():
    v = PascalVOCXMLValidator()
    root = _root("<annotation></annotation>")
    res = v._validate_source_element(root)
    assert any("Missing required 'source'" in e for e in res["errors"])


def test_source_empty_database_fails():
    v = PascalVOCXMLValidator()
    root = _root(
        _voc_xml().replace("<database>Unknown</database>", "<database></database>")
    )
    res = v._validate_source_element(root)
    assert any("Database element must have non-empty" in e for e in res["errors"])


def test_size_missing_fails():
    v = PascalVOCXMLValidator()
    root = _root("<annotation></annotation>")
    res = v._validate_size_element(root)
    assert any("Missing required 'size'" in e for e in res["errors"])


def test_size_non_positive_fails():
    v = PascalVOCXMLValidator()
    root = _root(_voc_xml(width=0))
    res = v._validate_size_element(root)
    assert any("width must be a positive integer" in e for e in res["errors"])


def test_size_non_integer_fails():
    v = PascalVOCXMLValidator()
    root = _root(_voc_xml().replace("<width>640</width>", "<width>abc</width>"))
    res = v._validate_size_element(root)
    assert any("width must be a valid integer" in e for e in res["errors"])


def test_objects_none_warns():
    v = PascalVOCXMLValidator()
    root = _root(_voc_xml(objects=[]))
    res = v._validate_objects(root)
    assert res["errors"] == []
    assert any("No objects found" in w for w in res["warnings"])


def test_object_missing_bndbox_fails():
    v = PascalVOCXMLValidator()
    obj = _root("""
      <object>
        <name>cat</name><pose>Unspecified</pose>
        <truncated>0</truncated><difficult>0</difficult>
      </object>""")
    res = v._validate_single_object(obj, 0)
    assert any("Missing required 'bndbox'" in e for e in res["errors"])


def test_object_bad_truncated_fails():
    v = PascalVOCXMLValidator()
    obj = _root(_object_xml(truncated="5"))
    res = v._validate_single_object(obj, 0)
    assert any("Truncated element must be" in e for e in res["errors"])


def test_bndbox_xmin_ge_xmax_fails():
    v = PascalVOCXMLValidator()
    obj = _root(_object_xml(xmin=100, xmax=50))
    res = v._validate_bndbox_element(obj, 0)
    assert any("must be less than xmax" in e for e in res["errors"])


def test_bndbox_negative_coord_fails():
    v = PascalVOCXMLValidator()
    obj = _root(_object_xml(xmin=-5))
    res = v._validate_bndbox_element(obj, 0)
    assert any("must be non-negative" in e for e in res["errors"])


def test_bndbox_non_integer_coord_fails():
    v = PascalVOCXMLValidator()
    obj = _root(_object_xml().replace("<xmin>10</xmin>", "<xmin>abc</xmin>"))
    res = v._validate_bndbox_element(obj, 0)
    assert any("must be a valid integer" in e for e in res["errors"])


def test_bndbox_small_area_warns():
    v = PascalVOCXMLValidator()
    obj = _root(_object_xml(xmin=10, ymin=10, xmax=12, ymax=12))  # area 4 < 10
    res = v._validate_bndbox_element(obj, 0)
    assert any("Very small bounding box" in w for w in res["warnings"])


# --- bbox bounded by declared <size> (#314 vision-hardening follow-up) --------


def test_bbox_exceeding_declared_width_fails(clean_env, tmp_path, validator):
    xml = _voc_xml(
        width=64, height=64, objects=[_object_xml(xmin=5, ymin=5, xmax=9999, ymax=30)]
    )
    (tmp_path / "ann.xml").write_text(xml, encoding="utf-8")
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = validator.validate(str(tmp_path))
    assert not result.is_valid
    assert any("exceeds image width" in e for e in result.errors)


def test_bbox_exceeding_declared_height_fails(clean_env, tmp_path, validator):
    xml = _voc_xml(
        width=64, height=64, objects=[_object_xml(xmin=5, ymin=5, xmax=30, ymax=9999)]
    )
    (tmp_path / "ann.xml").write_text(xml, encoding="utf-8")
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = validator.validate(str(tmp_path))
    assert not result.is_valid
    assert any("exceeds image height" in e for e in result.errors)


def test_bbox_within_declared_size_passes(clean_env, tmp_path, validator):
    xml = _voc_xml(
        width=64, height=64, objects=[_object_xml(xmin=5, ymin=5, xmax=60, ymax=60)]
    )
    (tmp_path / "ann.xml").write_text(xml, encoding="utf-8")
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = validator.validate(str(tmp_path))
    assert result.is_valid, result.errors


# --- declared <size> cross-checked against the actual image -------------------


def _setup_with_image(tmp_path, declared_wh, image_wh):
    from PIL import Image

    (tmp_path / "annotations").mkdir()
    (tmp_path / "images").mkdir()
    # The image is located by XML STEM (matching the ingestor's pairing), so the
    # annotation "ann.xml" pairs with the image "ann.<ext>".
    Image.new("RGB", image_wh, (120, 120, 120)).save(tmp_path / "images" / "ann.jpg")
    # bbox kept small so it fits inside the declared sizes used by callers —
    # we're exercising the size<->image cross-check, not the bbox-bounds check.
    xml = _voc_xml(
        width=declared_wh[0],
        height=declared_wh[1],
        objects=[_object_xml(xmin=5, ymin=5, xmax=40, ymax=40)],
    )
    (tmp_path / "annotations" / "ann.xml").write_text(xml, encoding="utf-8")


def test_declared_size_matching_actual_image_passes(clean_env, tmp_path, validator):
    _setup_with_image(tmp_path, declared_wh=(64, 64), image_wh=(64, 64))
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = validator.validate(str(tmp_path))
    assert result.is_valid, result.errors


def test_declared_size_mismatching_actual_image_fails(clean_env, tmp_path, validator):
    _setup_with_image(tmp_path, declared_wh=(128, 128), image_wh=(64, 64))
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = validator.validate(str(tmp_path))
    assert not result.is_valid
    assert any("does not match the actual image" in e for e in result.errors)


def test_size_check_skipped_when_image_absent(clean_env, tmp_path, validator):
    # No images/ dir -> the size cross-check is a benign skip (FilePairing /
    # FileType own the missing-image case), so a lone valid XML still passes.
    xml = _voc_xml(width=128, height=128)
    (tmp_path / "ann.xml").write_text(xml, encoding="utf-8")
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = validator.validate(str(tmp_path))
    assert result.is_valid, result.errors
