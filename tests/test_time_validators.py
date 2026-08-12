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


@pytest.mark.parametrize(
    "wrong_type", ["VARCHAR(32)", "TEXT", "INT", "FLOAT", "BOOLEAN"]
)
def test_format_schema_wrong_type_fails(wrong_type):
    """A non-calendar type is still rejected.

    Previously this asserted on ``DATE``, which #489 makes legal — inferred
    schemas can only ever produce ``DATE`` / ``DATETIME``. The check the test
    exists for is "a timestamp column that isn't a date/time type is refused",
    so it now covers the types that genuinely aren't.
    """
    v = TimeFormatValidator(schema={"timestamp": wrong_type})
    result = v.validate("ignored")
    assert not result.is_valid
    assert "must be a date/time type" in result.errors[0]
    assert wrong_type in result.errors[0]


@pytest.mark.parametrize(
    "declared",
    [
        "TIMESTAMP",
        "DATETIME",
        "DATE",
        # Case and precision specifiers must not change the verdict.
        "timestamp",
        "datetime",
        "date",
        "DATETIME(6)",
    ],
)
def test_format_schema_accepts_every_calendar_type(make_csv, declared):
    """#489: every type an INFERRED schema can carry must be accepted.

    ``schema_inference._infer_datetime`` emits ``DATETIME`` when a value has a
    time of day and ``DATE`` otherwise — never ``TIMESTAMP``. Requiring exactly
    ``TIMESTAMP`` rejected every inferred time-series-forecasting schema, so a
    CLI ingest failed regardless of the data.
    """
    path = make_csv({"timestamp": ["2024-01-01", "2024-01-02"], "v": [1, 2]})
    result = TimeFormatValidator(schema={"timestamp": declared}).validate(str(path))
    assert result.is_valid, result.errors


def test_format_schema_date_accepts_date_only_values(make_csv):
    """The exact shape that failed in the field: date-only values, inferred DATE."""
    path = make_csv(
        {
            "timestamp": ["2023-10-01", "2023-10-02", "2023-10-03"],
            "value": [1.0, 2.0, 3.0],
        }
    )
    result = TimeFormatValidator(
        schema={"timestamp": "DATE", "value": "FLOAT"}
    ).validate(str(path))
    assert result.is_valid, result.errors


def test_format_accepted_types_match_what_inference_can_emit():
    """Pin the cross-module contract that #489 broke.

    ``_infer_datetime`` is the only producer of a timestamp type for an inferred
    schema. Every value it can return must be accepted here, or the CLI path
    breaks again the next time either side changes.
    """
    from tracebloc_ingestor.schema_inference import _infer_datetime
    from tracebloc_ingestor.validators.time_format_validator import (
        TEMPORAL_TIMESTAMP_TYPES,
    )

    emitted = {
        _infer_datetime(["2024-01-01", "2024-01-02"]),  # date only
        _infer_datetime(["2024-01-01 09:30:00", "2024-01-02 10:00:00"]),  # with time
    }
    assert None not in emitted
    assert emitted <= TEMPORAL_TIMESTAMP_TYPES, (
        f"schema inference can emit {emitted - TEMPORAL_TIMESTAMP_TYPES}, which "
        f"TimeFormatValidator would reject"
    )


def test_format_schema_timestamp_with_precision_passes(make_csv):
    path = make_csv({"timestamp": ["2024-01-01"], "v": [1]})
    v = TimeFormatValidator(schema={"timestamp": "TIMESTAMP(6)"})
    result = v.validate(str(path))
    assert result.is_valid


# --- locale-ambiguous dates (silent-corruption guard) ----------------------


def test_format_ambiguous_eu_dates_rejected(make_csv):
    # "03.04.2026" is Apr 3 (day-first/EU) or Mar 4 (month-first/US); pandas'
    # format='mixed' would silently pick one. Both interpretations are valid but
    # different, so reject as ambiguous instead of corrupting the series.
    path = make_csv({"timestamp": ["03.04.2026", "07.08.2026"], "v": [1, 2]})
    result = TimeFormatValidator().validate(str(path))
    assert not result.is_valid
    assert "ambiguous" in result.errors[0].lower()


def test_format_iso_dates_not_ambiguous(make_csv):
    # ISO 8601 is unambiguous and must pass cleanly.
    path = make_csv({"timestamp": ["2024-01-03", "2024-04-05"], "v": [1, 2]})
    assert TimeFormatValidator().validate(str(path)).is_valid


def test_format_unambiguous_dates_not_flagged(make_csv):
    # day > 12 has only one valid interpretation (here month-first), so it is
    # NOT ambiguous and must not be falsely rejected.
    path = make_csv({"timestamp": ["01/25/2024", "02/26/2024"], "v": [1, 2]})
    assert TimeFormatValidator().validate(str(path)).is_valid


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
