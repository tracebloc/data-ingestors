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


def test_null_value_is_valid(make_csv):
    # Issue #195: nulls in a numeric column are legitimate missing values
    # (stored as SQL NULL), NOT a validation error. The shipped time-series
    # template explicitly documents lag/window features as leading-null, and
    # the validator was rejecting its own shipped sample. Null count is
    # still surfaced via metadata for observability.
    path = make_csv({
        "timestamp": ["2024-01-01", "2024-01-02"],
        "value": [1.0, None],
        "count": [3, 4],
    })
    result = NumericColumnsValidator(schema=SCHEMA).validate(str(path))
    assert result.is_valid, f"expected valid; errors={result.errors}"
    assert result.metadata["value_null_count"] == 1


def test_time_series_lag_window_leading_nulls_pass(make_csv):
    # End-to-end repro from #195: the shipped time-series template README
    # documents `lag_1` as blank for row 1 and `moving_avg_7` as blank
    # until enough history accumulates. The shipped sample CSV ships with
    # exactly those nulls, and the validator rejected its own sample.
    path = make_csv({
        "timestamp": ["2024-01-0%d" % i for i in range(1, 9)],
        "value": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0],
        "lag_1":          [None, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0],
        "moving_avg_7":   [None, None, None, None, None, None, 13.0, 14.0],
    })
    schema = {
        "timestamp": "TIMESTAMP", "value": "FLOAT",
        "lag_1": "FLOAT", "moving_avg_7": "FLOAT",
    }
    result = NumericColumnsValidator(schema=schema).validate(str(path))
    assert result.is_valid, f"expected valid; errors={result.errors}"


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


# ---------------------------------------------------------------------------
# excluded_columns parameter (backend#1054 WS1): time_series_classification
# excludes {sequence_id, timestamp} — its group key is legitimately VARCHAR.
# ---------------------------------------------------------------------------

TSC_SCHEMA = {
    "sequence_id": "VARCHAR(64)",
    "timestamp": "TIMESTAMP",
    "value": "FLOAT",
}


def test_default_excluded_is_timestamp_only(make_csv):
    # Without the parameter, a VARCHAR sequence_id column is (correctly for
    # forecasting) flagged non-numeric — the pre-#1054 behavior is unchanged.
    path = make_csv({
        "sequence_id": ["p1", "p2"],
        "timestamp": ["2024-01-01", "2024-01-02"],
        "value": [1.0, 2.0],
    })
    result = NumericColumnsValidator(schema=TSC_SCHEMA).validate(str(path))
    assert not result.is_valid
    assert "sequence_id" in result.errors[0]


def test_excluded_columns_skips_sequence_id(make_csv):
    path = make_csv({
        "sequence_id": ["p1", "p2"],
        "timestamp": ["2024-01-01", "2024-01-02"],
        "value": [1.0, 2.0],
    })
    result = NumericColumnsValidator(
        schema=TSC_SCHEMA, excluded_columns={"sequence_id", "timestamp"}
    ).validate(str(path))
    assert result.is_valid
    assert result.metadata["columns_to_validate"] == 1


def test_excluded_columns_match_case_insensitively(make_csv):
    path = make_csv({
        "Sequence_ID": ["p1", "p2"],
        "value": [1.0, 2.0],
    })
    result = NumericColumnsValidator(
        schema={"Sequence_ID": "VARCHAR(64)", "value": "FLOAT"},
        excluded_columns={"sequence_id"},
    ).validate(str(path))
    assert result.is_valid


def test_excluded_columns_still_flags_other_non_numeric(make_csv):
    # The exclusion is narrow: a genuinely non-numeric FEATURE still fails.
    path = make_csv({
        "sequence_id": ["p1", "p2"],
        "timestamp": ["2024-01-01", "2024-01-02"],
        "value": ["high", "low"],
    })
    result = NumericColumnsValidator(
        schema=TSC_SCHEMA, excluded_columns={"sequence_id", "timestamp"}
    ).validate(str(path))
    assert not result.is_valid
    assert "value" in result.errors[0]
