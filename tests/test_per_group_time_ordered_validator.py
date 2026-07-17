"""Tests for PerGroupTimeOrderedValidator — per-sequence monotonic timestamps
(backend#1054 WS1, T4): the global TimeOrderedValidator rejects interleaved
multi-sequence files; this one orders WITHIN each sequence only. Covers both
the TIMESTAMP branch (with the locale-ambiguity guard reused from
TimeFormatValidator) and the numeric step-index branch (Decision-2)."""

from __future__ import annotations

import pandas as pd
import pytest

from tracebloc_ingestor.validators.per_group_time_ordered_validator import (
    PerGroupTimeOrderedValidator,
)

TS_SCHEMA = {"sequence_id": "VARCHAR(64)", "timestamp": "TIMESTAMP", "v": "FLOAT"}
STEP_SCHEMA = {"sequence_id": "VARCHAR(64)", "timestamp": "INT", "v": "FLOAT"}


def _ts_df():
    rows = []
    for pid, T in (("p1", 3), ("p2", 4)):
        rows += [
            {
                "sequence_id": pid,
                "timestamp": f"2024-01-01 {8 + t:02d}:00:00",
                "v": float(t),
            }
            for t in range(T)
        ]
    return pd.DataFrame(rows)


def _step_df():
    rows = []
    for pid, T in (("p1", 5), ("p2", 3)):
        rows += [{"sequence_id": pid, "timestamp": t, "v": float(t)} for t in range(T)]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# TIMESTAMP branch
# ---------------------------------------------------------------------------


def test_per_group_ordered_timestamps_pass():
    result = PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(_ts_df())
    assert result.is_valid
    assert result.metadata["is_ordered"] is True
    assert result.metadata["sequences_checked"] == 2
    assert result.metadata["time_kind"] == "timestamp"


def test_honors_csv_options_delimiter(tmp_path):
    # #371 bugbot: parse a non-comma manifest with the run's csv_options. Under
    # the default comma parse the semicolon file is one squashed column (the
    # sequence/time columns are absent -> fail); sep=';' resolves it.
    path = tmp_path / "semi.csv"
    _ts_df().to_csv(path, index=False, sep=";")
    assert not PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(
        str(path)
    ).is_valid
    assert PerGroupTimeOrderedValidator(
        schema=TS_SCHEMA, csv_options={"sep": ";"}
    ).validate(str(path)).is_valid


def test_interleaved_sequences_pass_where_global_validator_would_fail():
    # T4: p1 08:00, p2 08:00, p1 09:00, ... is globally NON-monotonic but
    # perfectly ordered per sequence — the whole reason this validator exists.
    df = _ts_df().sort_values("timestamp", kind="stable").reset_index(drop=True)
    from tracebloc_ingestor.validators.time_ordered_validator import (
        TimeOrderedValidator,
    )

    result = PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(df)
    assert result.is_valid

    # Control: the global validator rejects the same file (over a CSV path,
    # its only input form) — proving the per-group variant was required.
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "interleaved.csv"
        df.to_csv(p, index=False)
        # Same times across sequences ARE non-decreasing globally here, so
        # shuffle to a genuinely global-breaking order: p2's early row after
        # p1's late row.
        df2 = pd.concat([df[df.sequence_id == "p1"], df[df.sequence_id == "p2"]])
        p2 = Path(d) / "grouped.csv"
        df2.to_csv(p2, index=False)
        assert not TimeOrderedValidator().validate(str(p2)).is_valid
        assert PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(str(p2)).is_valid


def test_out_of_order_within_sequence_fails():
    df = _ts_df()
    df.loc[1, "timestamp"] = "2024-01-01 23:00:00"  # p1 jumps back afterwards
    result = PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(df)
    assert not result.is_valid
    assert result.metadata["out_of_order_sequences"] == 1
    msg = result.errors[0]
    assert "out-of-order" in msg
    # Actionable: says what to do, and clarifies interleaving is fine.
    assert "sort each sequence" in msg.lower()
    assert "Interleaving different" in msg


def test_equal_timestamps_within_sequence_pass():
    # Monotonic NON-DECREASING: duplicated times are legal (matches the
    # global validator's semantics).
    df = _ts_df()
    df.loc[1, "timestamp"] = df.loc[0, "timestamp"]
    assert PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(df).is_valid


def test_invalid_timestamp_fails():
    df = _ts_df()
    df.loc[2, "timestamp"] = "not-a-date"
    result = PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(df)
    assert not result.is_valid
    assert result.metadata["invalid_timestamps"] == 1
    assert "rows [2]" in result.errors[0]


def test_null_timestamp_fails():
    df = _ts_df()
    df.loc[0, "timestamp"] = None
    result = PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(df)
    assert not result.is_valid
    assert result.metadata["invalid_timestamps"] == 1


def test_ambiguous_locale_dates_rejected():
    # Reuses TimeFormatValidator's silent-corruption guard: "03.04.2026"
    # parses to different dates day-first vs month-first.
    df = pd.DataFrame(
        {
            "sequence_id": ["p1", "p1"],
            "timestamp": ["03.04.2026", "07.08.2026"],
            "v": [1.0, 2.0],
        }
    )
    result = PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(df)
    assert not result.is_valid
    assert any("ambiguous" in e.lower() for e in result.errors)


def test_iso_dates_not_flagged_ambiguous():
    assert PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(_ts_df()).is_valid


# ---------------------------------------------------------------------------
# Numeric step-index branch (Decision-2: timestamp may be a step index)
# ---------------------------------------------------------------------------


def test_numeric_step_index_passes():
    result = PerGroupTimeOrderedValidator(schema=STEP_SCHEMA).validate(_step_df())
    assert result.is_valid
    assert result.metadata["time_kind"] == "numeric"


def test_numeric_out_of_order_fails():
    df = _step_df()
    df.loc[1, "timestamp"] = 99
    result = PerGroupTimeOrderedValidator(schema=STEP_SCHEMA).validate(df)
    assert not result.is_valid
    assert result.metadata["out_of_order_sequences"] == 1


def test_numeric_non_numeric_value_fails():
    df = _step_df()
    df["timestamp"] = df["timestamp"].astype(object)
    df.loc[0, "timestamp"] = "abc"
    result = PerGroupTimeOrderedValidator(schema=STEP_SCHEMA).validate(df)
    assert not result.is_valid
    assert result.metadata["invalid_timestamps"] == 1


# ---------------------------------------------------------------------------
# Branch inference without a schema
# ---------------------------------------------------------------------------


def test_no_schema_defaults_to_timestamp_branch():
    # The ingest schema contract requires a declared type for the time
    # column, so the validator no longer sniffs the data when constructed
    # without a schema (review: #359) — it defaults to the TIMESTAMP branch.
    result = PerGroupTimeOrderedValidator().validate(_ts_df())
    assert result.is_valid
    assert result.metadata["time_kind"] == "timestamp"


def test_numeric_branch_requires_declared_schema_type():
    # A numeric step index is selected by the schema declaration, never by
    # data sniffing (the former no-schema inference branch was unreachable
    # through the pipeline and was removed — review: #359).
    result = PerGroupTimeOrderedValidator(
        schema={"sequence_id": "VARCHAR(64)", "timestamp": "INT"}
    ).validate(_step_df())
    assert result.is_valid
    assert result.metadata["time_kind"] == "numeric"


# ---------------------------------------------------------------------------
# Structural rejections
# ---------------------------------------------------------------------------


def test_missing_sequence_column_fails():
    df = _ts_df().drop(columns=["sequence_id"])
    result = PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(df)
    assert not result.is_valid
    assert "sequence column" in result.errors[0]


def test_missing_timestamp_column_fails():
    df = _ts_df().drop(columns=["timestamp"])
    result = PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(df)
    assert not result.is_valid
    assert "'timestamp' not found" in result.errors[0]


def test_columns_resolved_case_insensitively():
    df = _ts_df().rename(
        columns={"timestamp": "Timestamp", "sequence_id": "Sequence_Id"}
    )
    assert PerGroupTimeOrderedValidator(schema=TS_SCHEMA).validate(df).is_valid


def test_no_data_fails():
    result = PerGroupTimeOrderedValidator().validate("/nope.parquet")
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_unsupported_data_type_fails():
    result = PerGroupTimeOrderedValidator().validate(12345)
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_internal_exception_becomes_error(monkeypatch):
    v = PerGroupTimeOrderedValidator()
    monkeypatch.setattr(
        v, "_load_data", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    result = v.validate(_ts_df())
    assert not result.is_valid
    assert "Validation error" in result.errors[0]
    # #226: the raw exception text (which can embed cell contents) must
    # never reach the customer-facing error — type name only.
    assert "boom" not in result.errors[0]
    assert "RuntimeError" in result.errors[0]
