"""Tests for the time-series validators: format, ordering, before-today, to-event."""

from __future__ import annotations

import pandas as pd
import pytest

from tracebloc_ingestor.validators.time_format_validator import TimeFormatValidator
from tracebloc_ingestor.validators.time_ordered_validator import TimeOrderedValidator
from tracebloc_ingestor.validators.time_before_today_validator import (
    TimeBeforeTodayValidator,
)
from tracebloc_ingestor.validators.time_to_event_validator import TimeToEventValidator


# ---------------------------------------------------------------------------
# TimeFormatValidator
# ---------------------------------------------------------------------------

def test_format_valid_timestamps_pass(make_csv):
    path = make_csv({"timestamp": ["2024-01-01", "2024-01-02"], "v": [1, 2]})
    result = TimeFormatValidator().validate(str(path))
    assert result.is_valid
    assert result.metadata["rows_checked"] == 2


def test_format_invalid_timestamp_fails(make_csv):
    path = make_csv({"timestamp": ["2024-01-01", "not-a-date"], "v": [1, 2]})
    result = TimeFormatValidator().validate(str(path))
    assert not result.is_valid
    assert "invalid timestamp" in result.errors[0]
    assert result.metadata["invalid_timestamps"] == 1


def test_format_missing_column_fails(make_csv):
    path = make_csv({"ts": ["2024-01-01"], "v": [1]})
    result = TimeFormatValidator().validate(str(path))
    assert not result.is_valid
    assert "not found" in result.errors[0]


def test_format_no_data_when_path_not_csv():
    result = TimeFormatValidator().validate("/nonexistent/path.txt")
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_format_schema_missing_timestamp_fails():
    v = TimeFormatValidator(schema={"value": "FLOAT"})
    result = v.validate("ignored")
    assert not result.is_valid
    assert "must contain a 'timestamp' column" in result.errors[0]


def test_format_schema_wrong_type_fails():
    v = TimeFormatValidator(schema={"timestamp": "DATE"})
    result = v.validate("ignored")
    assert not result.is_valid
    assert "must be of type 'TIMESTAMP'" in result.errors[0]


def test_format_schema_timestamp_with_precision_passes(make_csv):
    path = make_csv({"timestamp": ["2024-01-01"], "v": [1]})
    v = TimeFormatValidator(schema={"timestamp": "TIMESTAMP(6)"})
    result = v.validate(str(path))
    assert result.is_valid


# ---------------------------------------------------------------------------
# TimeOrderedValidator
# ---------------------------------------------------------------------------

def test_ordered_monotonic_passes(make_csv):
    path = make_csv({"timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"]})
    result = TimeOrderedValidator().validate(str(path))
    assert result.is_valid
    assert result.metadata["is_ordered"] is True


def test_ordered_out_of_order_fails(make_csv):
    path = make_csv({"timestamp": ["2024-01-03", "2024-01-01"]})
    result = TimeOrderedValidator().validate(str(path))
    assert not result.is_valid
    assert "out-of-order" in result.errors[0]
    assert result.metadata["out_of_order_pairs"] == 1


def test_ordered_missing_column_fails(make_csv):
    path = make_csv({"ts": ["2024-01-01"]})
    result = TimeOrderedValidator().validate(str(path))
    assert not result.is_valid
    assert "not found" in result.errors[0]


def test_ordered_no_data_for_non_csv():
    result = TimeOrderedValidator().validate("/nope.parquet")
    assert not result.is_valid


# ---------------------------------------------------------------------------
# TimeBeforeTodayValidator
# ---------------------------------------------------------------------------

def test_before_today_past_passes(make_csv):
    path = make_csv({"timestamp": ["2000-01-01", "2001-01-01"]})
    result = TimeBeforeTodayValidator().validate(str(path))
    assert result.is_valid
    assert "earliest" in result.metadata


def test_before_today_future_fails(make_csv):
    future = (pd.Timestamp.now() + pd.Timedelta(days=365)).strftime("%Y-%m-%d")
    path = make_csv({"timestamp": ["2000-01-01", future]})
    result = TimeBeforeTodayValidator().validate(str(path))
    assert not result.is_valid
    assert "not before today" in result.errors[0]
    assert result.metadata["future_timestamps"] == 1


def test_before_today_missing_column_fails(make_csv):
    path = make_csv({"ts": ["2000-01-01"]})
    result = TimeBeforeTodayValidator().validate(str(path))
    assert not result.is_valid


# ---------------------------------------------------------------------------
# TimeToEventValidator (accepts DataFrames directly)
# ---------------------------------------------------------------------------

def test_to_event_valid_passes():
    df = pd.DataFrame({"time": [0, 1.5, 10], "event": [1, 0, 1]})
    result = TimeToEventValidator().validate(df)
    assert result.is_valid
    assert result.metadata["min_time"] == 0.0
    assert result.metadata["max_time"] == 10.0


def test_to_event_missing_column_fails():
    df = pd.DataFrame({"duration": [1, 2]})
    result = TimeToEventValidator().validate(df)
    assert not result.is_valid
    assert "Required time column 'time' not found" in result.errors[0]


def test_to_event_non_numeric_fails():
    df = pd.DataFrame({"time": [1, "abc", 3]})
    result = TimeToEventValidator().validate(df)
    assert not result.is_valid
    assert "non-numeric" in result.errors[0]
    assert result.metadata["non_numeric_count"] == 1


def test_to_event_negative_fails():
    df = pd.DataFrame({"time": [1, -2, 3]})
    result = TimeToEventValidator().validate(df)
    assert not result.is_valid
    assert "negative" in result.errors[0]
    assert result.metadata["negative_count"] == 1


def test_to_event_null_warns_but_can_pass():
    df = pd.DataFrame({"time": [1.0, None, 3.0]})
    result = TimeToEventValidator().validate(df)
    assert result.is_valid
    assert any("null/missing" in w for w in result.warnings)


def test_to_event_empty_dataframe_fails():
    result = TimeToEventValidator().validate(pd.DataFrame())
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_to_event_custom_column_name():
    df = pd.DataFrame({"duration": [1, 2, 3]})
    result = TimeToEventValidator(time_column="duration").validate(df)
    assert result.is_valid


def test_to_event_loads_from_csv(make_csv):
    path = make_csv({"time": [1, 2, 3]})
    result = TimeToEventValidator().validate(str(path))
    assert result.is_valid


def test_to_event_sample_size_limits_rows():
    df = pd.DataFrame({"time": list(range(100))})
    result = TimeToEventValidator().validate(df, sample_size=10)
    assert result.metadata["rows_checked"] == 10
