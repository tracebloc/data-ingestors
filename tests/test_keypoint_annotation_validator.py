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


# ---------------------------------------------------------------------------
# num_keypoints — per-row count must match the dataset's declared K
# ---------------------------------------------------------------------------


def test_num_keypoints_match_passes():
    """When every row has exactly the declared K, validation passes."""
    v = KeypointAnnotationValidator(num_keypoints=2)
    df = _df([
        {"nose": [1, 2], "eye": [3, 4]},
        {"nose": [5, 6], "eye": [7, 8]},
    ])
    result = v.validate(df)
    assert result.is_valid


def test_num_keypoints_mismatch_fails():
    """Reproduces the staging incident — dataset declared K=14 but rows
    name only 4 keypoints. Pre-fix this passed ingest silently and
    crashed the runtime mid-training; post-fix the ingest rejects with
    a clear error pointing the dataset author at the right line."""
    v = KeypointAnnotationValidator(num_keypoints=14)
    df = _df([
        {"a": [1, 2], "b": [3, 4], "c": [5, 6], "d": [7, 8]},
    ])
    result = v.validate(df)
    assert not result.is_valid
    err = next(e for e in result.errors if "4 keypoint" in e)
    assert "num_keypoints=14" in err
    assert "mismatched counts cause silent shape crashes" in err


def test_num_keypoints_mismatch_reports_every_offending_row():
    """Mismatches surface per-row so the dataset author can fix them
    in one pass — not just the first one."""
    v = KeypointAnnotationValidator(num_keypoints=3)
    df = _df([
        {"a": [1, 2], "b": [3, 4]},                     # row 1: 2 kp (bad)
        {"a": [1, 2], "b": [3, 4], "c": [5, 6]},        # row 2: 3 kp (ok)
        {"a": [1, 2]},                                  # row 3: 1 kp (bad)
    ])
    result = v.validate(df)
    assert not result.is_valid
    count_errs = [e for e in result.errors if "num_keypoints=3" in e]
    assert len(count_errs) == 2  # rows 1 and 3
    assert any("Row 1" in e and "2 keypoint" in e for e in count_errs)
    assert any("Row 3" in e and "1 keypoint" in e for e in count_errs)


def test_num_keypoints_none_preserves_legacy_behavior():
    """When ``num_keypoints`` isn't set we skip the count check —
    matches the pre-fix behavior for ingests that haven't yet declared
    a count."""
    v = KeypointAnnotationValidator(num_keypoints=None)
    df = _df([{"a": [1, 2], "b": [3, 4]}])
    result = v.validate(df)
    assert result.is_valid


def test_num_keypoints_check_compounds_with_coordinate_errors():
    """A mismatched count shouldn't suppress per-keypoint coordinate
    errors — operator sees both kinds in one pass."""
    v = KeypointAnnotationValidator(num_keypoints=3)
    df = _df([
        {"a": [-1, 2], "b": [3, 4]},  # wrong count AND a negative coord
    ])
    result = v.validate(df)
    assert not result.is_valid
    assert any("num_keypoints=3" in e for e in result.errors)
    assert any("negative coordinates" in e for e in result.errors)
