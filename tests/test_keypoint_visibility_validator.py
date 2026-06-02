"""Tests for KeypointVisibilityValidator — visibility 0/1 + key consistency."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from tracebloc_ingestor.validators.keypoint_visibility_validator import (
    KeypointVisibilityValidator,
)


@pytest.fixture
def validator():
    return KeypointVisibilityValidator()


def test_valid_visibility_passes(validator):
    df = pd.DataFrame({
        "Annotation": [json.dumps({"nose": [1, 2], "eye": [3, 4]})],
        "Visibility": [json.dumps({"nose": 1, "eye": 0})],
    })
    result = validator.validate(df)
    assert result.is_valid


def test_visibility_without_annotation_column_passes(validator):
    df = pd.DataFrame({"Visibility": [json.dumps({"nose": 1})]})
    result = validator.validate(df)
    assert result.is_valid


def test_empty_dataframe_fails(validator):
    result = validator.validate(pd.DataFrame())
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_missing_visibility_column_fails(validator):
    result = validator.validate(pd.DataFrame({"Annotation": ["{}"]}))
    assert not result.is_valid
    assert "Missing required column" in result.errors[0]


def test_invalid_json_fails(validator):
    df = pd.DataFrame({"Visibility": ["{bad"]})
    result = validator.validate(df)
    assert not result.is_valid
    assert "Invalid JSON" in result.errors[0]


def test_non_dict_visibility_fails(validator):
    df = pd.DataFrame({"Visibility": [json.dumps([1, 0])]})
    result = validator.validate(df)
    assert not result.is_valid
    assert "must be a JSON object" in result.errors[0]


def test_out_of_range_value_fails(validator):
    df = pd.DataFrame({"Visibility": [json.dumps({"nose": 2})]})
    result = validator.validate(df)
    assert not result.is_valid
    assert any("must be 0 or 1" in e for e in result.errors)


def test_missing_keys_vs_annotation_fails(validator):
    df = pd.DataFrame({
        "Annotation": [json.dumps({"nose": [1, 2], "eye": [3, 4]})],
        "Visibility": [json.dumps({"nose": 1})],  # missing "eye"
    })
    result = validator.validate(df)
    assert not result.is_valid
    assert any("missing keys" in e for e in result.errors)


def test_extra_keys_vs_annotation_fails(validator):
    df = pd.DataFrame({
        "Annotation": [json.dumps({"nose": [1, 2]})],
        "Visibility": [json.dumps({"nose": 1, "eye": 0})],  # extra "eye"
    })
    result = validator.validate(df)
    assert not result.is_valid
    assert any("extra keys" in e for e in result.errors)
