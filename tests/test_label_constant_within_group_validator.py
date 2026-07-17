"""Tests for LabelConstantWithinGroupValidator — one label per sequence
(backend#1054 WS1): a mid-sequence label flip is rejected with a readable
error."""

from __future__ import annotations

import pandas as pd
import pytest

from tracebloc_ingestor.validators.label_constant_within_group_validator import (
    LabelConstantWithinGroupValidator,
)


def _toy_df():
    rows = []
    for pid, T, label in (("p1", 5, "1"), ("p2", 3, "0"), ("p3", 7, "0")):
        rows += [
            {"sequence_id": pid, "timestamp": t, "hr": 70 + t, "label": label}
            for t in range(T)
        ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_constant_labels_pass():
    result = LabelConstantWithinGroupValidator().validate(_toy_df())
    assert result.is_valid
    assert result.metadata["sequences_checked"] == 3
    assert result.metadata["rows_checked"] == 15


def test_csv_path_accepted(make_csv):
    path = make_csv(_toy_df())
    assert LabelConstantWithinGroupValidator().validate(str(path)).is_valid


def test_honors_csv_options_delimiter(tmp_path):
    # #371 bugbot: parse a non-comma manifest with the run's csv_options. Under
    # the default comma parse the semicolon file is one squashed column (no
    # sequence/label columns -> fail); sep=';' resolves it.
    path = tmp_path / "semi.csv"
    _toy_df().to_csv(path, index=False, sep=";")
    assert not LabelConstantWithinGroupValidator().validate(str(path)).is_valid
    assert LabelConstantWithinGroupValidator(
        csv_options={"sep": ";"}
    ).validate(str(path)).is_valid


def test_interleaved_sequences_pass():
    # Row order doesn't matter for constancy — only the per-sequence value set.
    df = _toy_df().sample(frac=1.0, random_state=7).reset_index(drop=True)
    assert LabelConstantWithinGroupValidator().validate(df).is_valid


def test_columns_resolved_case_insensitively():
    df = _toy_df().rename(columns={"label": "Label", "sequence_id": "SEQUENCE_ID"})
    result = LabelConstantWithinGroupValidator().validate(df)
    assert result.is_valid


def test_custom_label_column():
    df = _toy_df().rename(columns={"label": "outcome"})
    result = LabelConstantWithinGroupValidator(label_column="outcome").validate(df)
    assert result.is_valid


# ---------------------------------------------------------------------------
# Rejections
# ---------------------------------------------------------------------------


def test_mid_sequence_label_flip_fails():
    df = _toy_df()
    # Flip one timestep of p3 (rows 8-14): the file is no longer
    # one-label-per-sequence data.
    df.loc[10, "label"] = "1"
    result = LabelConstantWithinGroupValidator().validate(df)
    assert not result.is_valid
    assert result.metadata["inconsistent_sequences"] == 1
    msg = result.errors[0]
    # Readable: names the rule and where to look, without leaking the ids.
    assert "changes mid-sequence" in msg
    assert "ONE label per" in msg
    assert "rows [8]" in msg  # first row of the offending sequence
    assert "p3" not in msg


def test_null_mixed_with_value_fails():
    # A null label on some timesteps is still an inconsistent outcome.
    df = _toy_df()
    df.loc[1, "label"] = None
    result = LabelConstantWithinGroupValidator().validate(df)
    assert not result.is_valid
    assert result.metadata["inconsistent_sequences"] == 1


def test_multiple_offending_sequences_counted():
    df = _toy_df()
    df.loc[0, "label"] = "9"
    df.loc[6, "label"] = "9"
    result = LabelConstantWithinGroupValidator().validate(df)
    assert not result.is_valid
    assert result.metadata["inconsistent_sequences"] == 2


def test_missing_sequence_column_fails():
    df = _toy_df().drop(columns=["sequence_id"])
    result = LabelConstantWithinGroupValidator().validate(df)
    assert not result.is_valid
    assert "sequence column" in result.errors[0]


def test_missing_label_column_fails():
    df = _toy_df().drop(columns=["label"])
    result = LabelConstantWithinGroupValidator().validate(df)
    assert not result.is_valid
    assert "label column" in result.errors[0]


def test_no_data_fails():
    result = LabelConstantWithinGroupValidator().validate("/nonexistent/nope.txt")
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_empty_dataframe_fails():
    assert not LabelConstantWithinGroupValidator().validate(pd.DataFrame()).is_valid


def test_unsupported_data_type_fails():
    result = LabelConstantWithinGroupValidator().validate(12345)
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_non_csv_suffix_fails(tmp_path):
    p = tmp_path / "data.parquet"
    p.write_text("x")
    result = LabelConstantWithinGroupValidator().validate(str(p))
    assert not result.is_valid


def test_internal_exception_becomes_error(monkeypatch):
    v = LabelConstantWithinGroupValidator()
    monkeypatch.setattr(
        v, "_load_data", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    result = v.validate(_toy_df())
    assert not result.is_valid
    assert "Label constancy validation error" in result.errors[0]
    # #226: the raw exception text (which can embed cell contents) must
    # never reach the customer-facing error — type name only.
    assert "boom" not in result.errors[0]
    assert "RuntimeError" in result.errors[0]
