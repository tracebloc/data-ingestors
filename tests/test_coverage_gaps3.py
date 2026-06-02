"""Third batch of cheap gap-closers: non-string type checks and _load_data
exception fallbacks across validators."""

from __future__ import annotations

import importlib
from unittest.mock import patch

import pandas as pd
import pytest

from tracebloc_ingestor.validators.data_validator import DataValidator


# ---- data_validator non-string type checks -------------------------------

def test_varchar_non_string_values_flagged():
    df = pd.DataFrame({"s": [1, 2]})  # ints under VARCHAR -> non-string
    res = DataValidator(schema={"s": "VARCHAR(10)"}).validate(df)
    assert not res.is_valid
    assert any("non-string" in e for e in res.errors)


def test_char_non_string_values_flagged():
    df = pd.DataFrame({"s": [1, 2]})
    res = DataValidator(schema={"s": "CHAR(1)"}).validate(df)
    assert not res.is_valid


def test_text_non_string_values_flagged():
    df = pd.DataFrame({"s": [1, 2]})
    res = DataValidator(schema={"s": "TEXT"}).validate(df)
    assert not res.is_valid
    assert any("non-string" in e for e in res.errors)


def test_data_validator_load_data_exception(make_csv):
    v = DataValidator(schema={"a": "INT"})
    path = make_csv({"a": [1]})
    with patch("tracebloc_ingestor.validators.data_validator.pd.read_csv",
               side_effect=RuntimeError("boom")):
        assert v._load_data(str(path), 100) is None


# ---- numeric_columns _load_data exception ---------------------------------

def test_numeric_columns_load_data_exception(make_csv):
    from tracebloc_ingestor.validators.numeric_columns_validator import (
        NumericColumnsValidator,
    )
    path = make_csv({"a": [1]})
    v = NumericColumnsValidator(schema={"a": "INT"})
    with patch("tracebloc_ingestor.validators.numeric_columns_validator.pd.read_csv",
               side_effect=RuntimeError("boom")):
        assert v._load_data(str(path)) is None


# ---- time validators: _load_data exception fallback -----------------------

@pytest.mark.parametrize("modname,clsname", [
    ("time_format_validator", "TimeFormatValidator"),
    ("time_ordered_validator", "TimeOrderedValidator"),
    ("time_before_today_validator", "TimeBeforeTodayValidator"),
])
def test_time_validators_load_data_read_exception(make_csv, modname, clsname):
    mod = importlib.import_module(f"tracebloc_ingestor.validators.{modname}")
    v = getattr(mod, clsname)()
    path = make_csv({"timestamp": ["2024-01-01"]})
    with patch(f"tracebloc_ingestor.validators.{modname}.pd.read_csv",
               side_effect=RuntimeError("boom")):
        assert v._load_data(str(path)) is None


def test_time_before_today_empty_dataframe(tmp_path):
    from tracebloc_ingestor.validators.time_before_today_validator import (
        TimeBeforeTodayValidator,
    )
    p = tmp_path / "e.csv"
    p.write_text("timestamp\n")  # header only -> empty df
    res = TimeBeforeTodayValidator().validate(str(p))
    assert not res.is_valid
    assert "No data found" in res.errors[0]
