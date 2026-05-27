"""Tests for NumericColumnsValidator — non-timestamp schema columns must be numeric/non-null."""

from __future__ import annotations

import pytest

from tracebloc_ingestor.validators.numeric_columns_validator import (
    NumericColumnsValidator,
)


SCHEMA = {"timestamp": "TIMESTAMP", "value": "FLOAT", "count": "INT"}


def test_all_numeric_passes(make_csv):
    path = make_csv({
        "timestamp": ["2024-01-01", "2024-01-02"],
        "value": [1.0, 2.0],
        "count": [3, 4],
    })
    result = NumericColumnsValidator(schema=SCHEMA).validate(str(path))
    assert result.is_valid
    assert result.metadata["columns_to_validate"] == 2


def test_null_value_fails(make_csv):
    path = make_csv({
        "timestamp": ["2024-01-01", "2024-01-02"],
        "value": [1.0, None],
        "count": [3, 4],
    })
    result = NumericColumnsValidator(schema=SCHEMA).validate(str(path))
    assert not result.is_valid
    assert any("null/missing" in e for e in result.errors)


def test_non_numeric_fails(make_csv):
    path = make_csv({
        "timestamp": ["2024-01-01", "2024-01-02"],
        "value": ["x", "y"],
        "count": [3, 4],
    })
    result = NumericColumnsValidator(schema=SCHEMA).validate(str(path))
    assert not result.is_valid
    assert any("non-numeric" in e for e in result.errors)


def test_timestamp_column_excluded(make_csv):
    # timestamp is non-numeric but must be skipped.
    path = make_csv({
        "timestamp": ["2024-01-01", "2024-01-02"],
        "value": [1.0, 2.0],
    })
    result = NumericColumnsValidator(schema={"timestamp": "TIMESTAMP", "value": "FLOAT"}).validate(str(path))
    assert result.is_valid


def test_no_schema_skips_validation(make_csv):
    path = make_csv({"value": ["anything"]})
    result = NumericColumnsValidator().validate(str(path))
    assert result.is_valid
    assert "No schema provided" in result.metadata["message"]


def test_no_overlapping_columns_passes(make_csv):
    # Schema columns aren't present in the CSV (besides timestamp) -> nothing to check.
    path = make_csv({"timestamp": ["2024-01-01"], "other": [1]})
    result = NumericColumnsValidator(schema={"timestamp": "TIMESTAMP", "value": "FLOAT"}).validate(str(path))
    assert result.is_valid
    assert "No schema columns to validate" in result.metadata["message"]


def test_no_data_for_non_csv():
    result = NumericColumnsValidator(schema=SCHEMA).validate("/missing.csv")
    assert not result.is_valid
    assert "No data found" in result.errors[0]
