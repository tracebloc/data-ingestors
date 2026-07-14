"""Tests for SequenceGroupValidator — the sequence_id group column of grouped
time-series data (backend#1054 WS1): presence, no null ids, per-sequence
metadata, and the data_id-strategy guard (T6)."""

from __future__ import annotations

import pandas as pd
import pytest

from tracebloc_ingestor.validators.sequence_group_validator import (
    SequenceGroupValidator,
)


def _toy_df():
    """The done-contract 3-patient toy shape: T = 5 / 3 / 7 -> 15 rows."""
    rows = []
    for pid, T, label in (("p1", 5, "1"), ("p2", 3, "0"), ("p3", 7, "0")):
        rows += [
            {"sequence_id": pid, "timestamp": t, "hr": 70 + t, "label": label}
            for t in range(T)
        ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Happy path + per-sequence metadata
# ---------------------------------------------------------------------------


def test_valid_sequences_pass_with_metadata():
    result = SequenceGroupValidator().validate(_toy_df())
    assert result.is_valid
    assert result.metadata["rows_checked"] == 15
    # Per-sequence stats were removed as unused duplication (review: #359):
    # the shipping number_of_sequences comes from ingestors/base.py's
    # post-insert DB counts, not validator metadata.
    assert "number_of_sequences" not in result.metadata
    assert "min_sequence_length" not in result.metadata
    assert "max_sequence_length" not in result.metadata
    assert "median_sequence_length" not in result.metadata


def test_valid_csv_path_accepted(make_csv):
    path = make_csv(_toy_df())
    result = SequenceGroupValidator().validate(str(path))
    assert result.is_valid
    assert "number_of_sequences" not in result.metadata


def test_column_resolved_case_insensitively():
    # #340 rule: header spelling may differ in case/whitespace.
    df = _toy_df().rename(columns={"sequence_id": " Sequence_ID "})
    result = SequenceGroupValidator().validate(df)
    assert result.is_valid
    assert result.metadata["sequence_column"] == " Sequence_ID "


# ---------------------------------------------------------------------------
# Rejections
# ---------------------------------------------------------------------------


def test_missing_sequence_column_fails():
    df = _toy_df().drop(columns=["sequence_id"])
    result = SequenceGroupValidator().validate(df)
    assert not result.is_valid
    assert "sequence column 'sequence_id'" in result.errors[0].lower()
    # Readable: the message names what the column is for.
    assert "grouping" in result.errors[0]


def test_null_sequence_id_fails():
    df = _toy_df()
    df.loc[4, "sequence_id"] = None
    result = SequenceGroupValidator().validate(df)
    assert not result.is_valid
    assert "null/empty" in result.errors[0]
    assert result.metadata["null_count"] == 1
    assert "rows [4]" in result.errors[0]


def test_empty_string_sequence_id_fails():
    df = _toy_df()
    df.loc[0, "sequence_id"] = "   "
    result = SequenceGroupValidator().validate(df)
    assert not result.is_valid
    assert result.metadata["null_count"] == 1


def test_data_id_column_on_sequence_column_rejected():
    # T6: data_id is UNIQUE per row — mapping it from sequence_id would
    # upsert-collapse every sequence to one row. Must fail even without data.
    v = SequenceGroupValidator(unique_id_column="sequence_id")
    result = v.validate(_toy_df())
    assert not result.is_valid
    assert "data_id" in result.errors[0]
    assert "collapse" in result.errors[0]
    # And the message steers to the safe strategies.
    assert "content_hash" in result.errors[0]


def test_data_id_guard_is_case_insensitive():
    v = SequenceGroupValidator(unique_id_column=" SEQUENCE_ID ")
    result = v.validate(_toy_df())
    assert not result.is_valid
    assert "data_id" in result.errors[0]


def test_data_id_on_other_column_passes():
    v = SequenceGroupValidator(unique_id_column="row_uuid")
    result = v.validate(_toy_df())
    assert result.is_valid


def test_no_data_fails():
    result = SequenceGroupValidator().validate("/nonexistent/path.txt")
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_empty_dataframe_fails():
    result = SequenceGroupValidator().validate(pd.DataFrame())
    assert not result.is_valid


# ---------------------------------------------------------------------------
# Schema advisory
# ---------------------------------------------------------------------------


def test_non_varchar_schema_type_warns_but_passes():
    v = SequenceGroupValidator(schema={"sequence_id": "INT"})
    result = v.validate(_toy_df())
    assert result.is_valid
    assert result.warnings and "VARCHAR" in result.warnings[0]


def test_varchar_schema_type_no_warning():
    v = SequenceGroupValidator(schema={"sequence_id": "VARCHAR(64)"})
    result = v.validate(_toy_df())
    assert result.is_valid
    assert result.warnings == []


# ---------------------------------------------------------------------------
# Privacy: sequence ids never leak into errors/metadata (#226 policy)
# ---------------------------------------------------------------------------


def test_errors_and_metadata_carry_no_sequence_ids():
    df = _toy_df()
    df.loc[2, "sequence_id"] = None
    result = SequenceGroupValidator().validate(df)
    blob = " ".join(result.errors) + repr(result.metadata)
    assert "p1" not in blob and "p2" not in blob and "p3" not in blob


def test_sample_size_kwarg_limits_rows():
    result = SequenceGroupValidator().validate(_toy_df(), sample_size=5)
    assert result.metadata["rows_checked"] == 5


def test_unsupported_data_type_fails():
    result = SequenceGroupValidator().validate(12345)
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_non_csv_suffix_fails(tmp_path):
    p = tmp_path / "data.parquet"
    p.write_text("x")
    result = SequenceGroupValidator().validate(str(p))
    assert not result.is_valid


def test_internal_exception_becomes_error(monkeypatch):
    v = SequenceGroupValidator()
    monkeypatch.setattr(
        v, "_load_data", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    result = v.validate(_toy_df())
    assert not result.is_valid
    assert "Sequence group validation error" in result.errors[0]
    # #226: the raw exception text (which can embed cell contents) must
    # never reach the customer-facing error — type name only.
    assert "boom" not in result.errors[0]
    assert "RuntimeError" in result.errors[0]


def test_missing_sequence_column_error_is_bounded_on_wide_panels():
    # Bugbot (review: #359): a wide genomics/proteomics panel must not dump
    # thousands of column names into one error string. The message shows a
    # capped preview; the FULL list stays in metadata.
    df = pd.DataFrame({f"gene_{i}": [1.0] for i in range(500)})
    result = SequenceGroupValidator().validate(df)
    assert not result.is_valid
    [error] = result.errors
    assert len(error) < 600
    assert "(+490 more of 500)" in error
    assert result.metadata["available_columns"] == list(df.columns)
