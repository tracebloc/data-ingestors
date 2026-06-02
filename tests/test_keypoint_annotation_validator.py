"""Tests for KeypointAnnotationValidator — JSON structure / coordinate checks."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from tracebloc_ingestor.validators.keypoint_annotation_validator import (
    KeypointAnnotationValidator,
)


@pytest.fixture
def validator():
    return KeypointAnnotationValidator()


def _df(annotations):
    return pd.DataFrame({"Annotation": [json.dumps(a) for a in annotations]})


def test_valid_keypoints_pass(validator):
    df = _df([
        {"nose": [10, 20], "eye": [30, 40]},
        {"nose": [11, 21], "eye": [31, 41]},
    ])
    result = validator.validate(df)
    assert result.is_valid
    assert result.metadata["rows_checked"] == 2


def test_dict_coordinate_form_passes(validator):
    df = _df([{"nose": {"x": 1, "y": 2}, "eye": {"x": 3, "y": 4}}])
    result = validator.validate(df)
    assert result.is_valid


def test_empty_dataframe_fails(validator):
    result = validator.validate(pd.DataFrame())
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_missing_column_fails(validator):
    result = validator.validate(pd.DataFrame({"other": [1]}))
    assert not result.is_valid
    assert "Missing required column" in result.errors[0]


def test_invalid_json_fails(validator):
    df = pd.DataFrame({"Annotation": ["{not json"]})
    result = validator.validate(df)
    assert not result.is_valid
    assert "Invalid JSON" in result.errors[0]


def test_non_dict_annotation_fails(validator):
    df = pd.DataFrame({"Annotation": [json.dumps([1, 2, 3])]})
    result = validator.validate(df)
    assert not result.is_valid
    assert "must be a JSON object" in result.errors[0]


def test_wrong_coordinate_count_fails(validator):
    df = _df([{"nose": [1, 2, 3]}])
    result = validator.validate(df)
    assert not result.is_valid
    assert "exactly" in result.errors[0]


def test_non_numeric_coordinates_fail(validator):
    df = _df([{"nose": ["a", "b"], "eye": [1, 2]}])
    result = validator.validate(df)
    assert not result.is_valid
    assert any("must be numeric" in e for e in result.errors)


def test_negative_coordinates_fail(validator):
    df = _df([{"nose": [-1, 2], "eye": [3, 4]}])
    result = validator.validate(df)
    assert not result.is_valid
    assert any("negative coordinates" in e for e in result.errors)


def test_malformed_keypoint_value_fails(validator):
    df = _df([{"nose": 5}])  # neither list nor x/y dict
    result = validator.validate(df)
    assert not result.is_valid
    assert any("[x, y] list" in e for e in result.errors)


def test_degenerate_bounding_box_fails(validator):
    # All keypoints share the same coordinates -> zero width/height.
    df = _df([{"a": [5, 5], "b": [5, 5]}])
    result = validator.validate(df)
    assert not result.is_valid
    assert any("degenerate bounding box" in e for e in result.errors)


def test_inconsistent_keypoint_names_fail(validator):
    df = _df([
        {"nose": [1, 2], "eye": [3, 4]},
        {"nose": [1, 2], "ear": [3, 4]},  # different key set
    ])
    result = validator.validate(df)
    assert not result.is_valid
    assert any("differ from first record" in e for e in result.errors)
