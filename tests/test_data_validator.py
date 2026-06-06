"""Tests for DataValidator — per-type schema compliance checks."""

from __future__ import annotations

import pandas as pd
import pytest

from tracebloc_ingestor.validators.data_validator import DataValidator


# ---------------------------------------------------------------------------
# validate() top-level flow
# ---------------------------------------------------------------------------

def test_no_schema_passes_with_warning():
    result = DataValidator().validate(pd.DataFrame({"a": [1]}))
    assert result.is_valid
    assert result.metadata["schema_provided"] is False


def test_empty_dataframe_fails():
    result = DataValidator(schema={"a": "INT"}).validate(pd.DataFrame())
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_non_csv_path_returns_no_data():
    result = DataValidator(schema={"a": "INT"}).validate("/nope.parquet")
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_loads_from_csv(make_csv):
    path = make_csv({"age": [1, 2, 3]})
    result = DataValidator(schema={"age": "INT"}).validate(str(path))
    assert result.is_valid


def test_unknown_type_fails():
    df = pd.DataFrame({"a": [1]})
    result = DataValidator(schema={"a": "WEIRDTYPE"}).validate(df)
    assert not result.is_valid
    assert "Unknown data type" in result.errors[0]


def test_schema_column_not_in_df_is_ignored():
    df = pd.DataFrame({"a": [1]})
    result = DataValidator(schema={"b": "INT"}).validate(df)
    # 'b' isn't in df.columns, so nothing is validated -> valid.
    assert result.is_valid


# ---------------------------------------------------------------------------
# INT / BIGINT
# ---------------------------------------------------------------------------

def test_int_valid():
    df = pd.DataFrame({"n": [1, 2, 3]})
    assert DataValidator(schema={"n": "INT"}).validate(df).is_valid


def test_int_non_numeric_fails():
    df = pd.DataFrame({"n": ["a", "b"]})
    result = DataValidator(schema={"n": "INT"}).validate(df)
    assert not result.is_valid
    assert "non-numeric" in result.errors[0]


def test_int_non_integer_float_fails():
    df = pd.DataFrame({"n": [1.5, 2.0]})
    result = DataValidator(schema={"n": "INT"}).validate(df)
    assert not result.is_valid
    assert "non-integer" in result.errors[0]


def test_bigint_delegates_to_int():
    df = pd.DataFrame({"n": [10_000_000_000]})
    assert DataValidator(schema={"n": "BIGINT"}).validate(df).is_valid


# ---------------------------------------------------------------------------
# FLOAT / DOUBLE / DECIMAL
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dtype", ["FLOAT", "DOUBLE", "DECIMAL(10,2)"])
def test_float_family_valid(dtype):
    df = pd.DataFrame({"x": [1.5, 2.25]})
    assert DataValidator(schema={"x": dtype}).validate(df).is_valid


def test_float_non_numeric_fails():
    df = pd.DataFrame({"x": ["a", "b"]})
    result = DataValidator(schema={"x": "FLOAT"}).validate(df)
    assert not result.is_valid


# ---------------------------------------------------------------------------
# Missing values are NULL, not "non-numeric" (regression)
# ---------------------------------------------------------------------------

def test_int_with_missing_values_is_valid():
    # NaN/empty in an INT column is a missing value (stored as NULL), not a
    # non-numeric one — and not a "non-integer" one either.
    df = pd.DataFrame({"n": [1, None, 3]})
    assert DataValidator(schema={"n": "INT"}).validate(df).is_valid


def test_float_with_missing_values_is_valid():
    # Regression: a float column with genuine NaN was wrongly reported as
    # containing "non-numeric values" — and inserting NaN could never clear it.
    df = pd.DataFrame({"x": [1.5, None, 2.25]})
    assert DataValidator(schema={"x": "FLOAT"}).validate(df).is_valid


def test_missing_values_do_not_mask_real_non_numeric():
    # Only the genuinely unparseable value is flagged; the NaN is ignored.
    df = pd.DataFrame({"x": [1.5, None, "oops"]})
    result = DataValidator(schema={"x": "FLOAT"}).validate(df)
    assert not result.is_valid
    assert "1 non-numeric" in result.errors[0]
    assert "oops" in result.errors[0]  # the offending value is surfaced


def test_non_numeric_error_includes_sample_values():
    df = pd.DataFrame({"x": ["abc", "def"]})
    result = DataValidator(schema={"x": "FLOAT"}).validate(df)
    assert not result.is_valid
    assert "Sample invalid values" in result.errors[0]
    assert "abc" in result.errors[0]


# ---------------------------------------------------------------------------
# VARCHAR / CHAR / TEXT
# ---------------------------------------------------------------------------

def test_varchar_valid():
    df = pd.DataFrame({"s": ["abc", "de"]})
    assert DataValidator(schema={"s": "VARCHAR(255)"}).validate(df).is_valid


def test_varchar_too_long_fails():
    df = pd.DataFrame({"s": ["abcdef", "gh"]})
    result = DataValidator(schema={"s": "VARCHAR(3)"}).validate(df)
    assert not result.is_valid
    assert "exceeding max length" in result.errors[0]


def test_char_wrong_length_fails():
    df = pd.DataFrame({"s": ["ab", "cde"]})
    result = DataValidator(schema={"s": "CHAR(2)"}).validate(df)
    assert not result.is_valid
    assert "length !=" in result.errors[0]


def test_text_valid():
    df = pd.DataFrame({"s": ["any length text here"]})
    assert DataValidator(schema={"s": "TEXT"}).validate(df).is_valid


# ---------------------------------------------------------------------------
# BOOLEAN across dtypes
# ---------------------------------------------------------------------------

def test_boolean_bool_dtype_valid():
    df = pd.DataFrame({"b": [True, False]})
    assert DataValidator(schema={"b": "BOOLEAN"}).validate(df).is_valid


def test_boolean_int_valid():
    df = pd.DataFrame({"b": [0, 1, 1]})
    assert DataValidator(schema={"b": "BOOL"}).validate(df).is_valid


def test_boolean_int_out_of_range_fails():
    df = pd.DataFrame({"b": [0, 2]})
    result = DataValidator(schema={"b": "BOOLEAN"}).validate(df)
    assert not result.is_valid
    assert "non-boolean" in result.errors[0]


def test_boolean_string_valid():
    df = pd.DataFrame({"b": ["true", "no", "1", "False"]})
    assert DataValidator(schema={"b": "BOOLEAN"}).validate(df).is_valid


def test_boolean_string_invalid_fails():
    df = pd.DataFrame({"b": ["maybe", "true"]})
    result = DataValidator(schema={"b": "BOOLEAN"}).validate(df)
    assert not result.is_valid


def test_boolean_all_null_valid():
    df = pd.DataFrame({"b": pd.Series([None, None], dtype="object")})
    assert DataValidator(schema={"b": "BOOLEAN"}).validate(df).is_valid


def test_boolean_float_valid():
    df = pd.DataFrame({"b": [0.0, 1.0]})
    assert DataValidator(schema={"b": "BOOLEAN"}).validate(df).is_valid


def test_boolean_float_invalid_fails():
    df = pd.DataFrame({"b": [0.0, 0.5]})
    result = DataValidator(schema={"b": "BOOLEAN"}).validate(df)
    assert not result.is_valid


# ---------------------------------------------------------------------------
# DATE / DATETIME / TIMESTAMP / TIME
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dtype", ["DATE", "DATETIME", "TIMESTAMP", "TIME"])
def test_date_family_valid(dtype):
    df = pd.DataFrame({"d": ["2024-01-01", "2024-02-02"]})
    assert DataValidator(schema={"d": dtype}).validate(df).is_valid


def test_date_invalid_fails():
    df = pd.DataFrame({"d": ["not-a-date", "2024-01-01"]})
    result = DataValidator(schema={"d": "DATE"}).validate(df)
    assert not result.is_valid
    assert "invalid date" in result.errors[0]


# ---------------------------------------------------------------------------
# type parsing + auto-detect helper
# ---------------------------------------------------------------------------

def test_constraints_are_stripped_from_type():
    df = pd.DataFrame({"n": [1, 2]})
    # "INT NOT NULL" should resolve to the INT validator.
    assert DataValidator(schema={"n": "INT NOT NULL"}).validate(df).is_valid


@pytest.mark.parametrize(
    "series,expected",
    [
        (pd.Series([True, False]), "BOOLEAN"),
        (pd.Series([1, 2, 3]), "INT"),
        (pd.Series([1.0, 2.5]), "FLOAT"),
        (pd.Series(pd.to_datetime(["2024-01-01"])), "DATETIME"),
        (pd.Series(["x", "y"]), "VARCHAR(255)"),
    ],
)
def test_detect_column_type(series, expected):
    assert DataValidator()._detect_column_type(series) == expected
