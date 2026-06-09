"""Tests for DataValidator — per-type schema compliance checks."""

from __future__ import annotations

import numpy as np
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


def test_streaming_validator_rejects_duplicate_headers(tmp_path):
    # Regression (#190 bugbot): the streaming CSV validator used to accept
    # duplicate headers (pandas silently disambiguates "a, a" to "a, a.1"),
    # while CSVIngestor.read_data rejects them outright — so a file passed
    # validation and then exploded at ingest with a structural error the
    # preflight step never surfaced. Align: reject up front.
    p = tmp_path / "dups.csv"
    p.write_text("a,a\n1,2\n3,4\n")
    result = DataValidator(schema={"a": "INT"}).validate(str(p))
    assert not result.is_valid
    assert "Duplicate column" in result.errors[0]


def test_streaming_validator_rejects_ragged_rows(tmp_path):
    # Regression (#190 bugbot): the streaming validator used on_bad_lines=
    # "warn" — a ragged row was silently dropped, validation reported success,
    # then CSVIngestor.read_data (on_bad_lines="error") failed at ingest. Now
    # both fail at the same point.
    p = tmp_path / "ragged.csv"
    p.write_text("a,b\n1,2\n3,4,5\n6,7\n")
    result = DataValidator(schema={"a": "INT", "b": "INT"}).validate(str(p))
    assert not result.is_valid


def test_loads_from_json(tmp_path):
    """JSON top-level array of records must validate, mirroring the file shape
    JSONIngestor.read_data consumes.

    Regression: DataValidator._load_data only recognised .csv. Any .json input
    returned None (logged "Unsupported file type"), the caller raised
    "No data found to validate", and JSON ingestion failed end-to-end on the
    very first validator — the recent per-record null-tolerance fix (#170)
    lived behind an unreachable gate.

    Surfaced by an end-to-end cluster ingestion: a 20-record JSON file with
    explicit schema {id INT, age INT, score FLOAT, active BOOL, label
    VARCHAR(20)} failed with "Data Validator Validator failed: No data found
    to validate" before any record was read.
    """
    p = tmp_path / "d.json"
    p.write_text(
        '[{"id": 1, "age": 30, "score": 0.5, "active": true, "label": "A"},'
        ' {"id": 2, "age": null, "score": 0.6, "active": false, "label": "B"}]'
    )
    result = DataValidator(
        schema={
            "id": "INT",
            "age": "INT",
            "score": "FLOAT",
            "active": "BOOL",
            "label": "VARCHAR(20)",
        }
    ).validate(str(p))
    assert result.is_valid, f"expected valid; errors={result.errors}"


def test_loads_from_json_genuine_type_error_still_caught(tmp_path):
    """JSON support must not weaken type validation — a non-numeric in an INT
    column must still fail. Pairs with the positive test to pin the boundary.
    """
    p = tmp_path / "bad.json"
    p.write_text('[{"age": "not-an-int"}]')
    result = DataValidator(schema={"age": "INT"}).validate(str(p))
    assert not result.is_valid


def test_loads_from_json_empty_string_is_missing(tmp_path):
    """Empty strings in JSON INT/FLOAT cells must be treated as missing, the
    same way `null` is — matching JSONIngestor._validate_record's convention
    (`if value is None or value == ""`, #170).

    Regression: pd.read_json (unlike pd.read_csv with keep_default_na=True)
    preserves "" as the literal empty string. The INT validator then reported
    `"Column 'age' contains N non-numeric value(s). Sample invalid values:
    ['', '']"` even though those rows would have been ingested as missing —
    so a JSON file that JSONIngestor handles fine was rejected by the
    validator that runs before it.

    Surfaced by an end-to-end cluster ingestion against v0.3.5-rc2: 20-record
    JSON file with mixed `null` and `""` in INT/FLOAT columns failed
    validation with the above error.
    """
    p = tmp_path / "d.json"
    p.write_text(
        '[{"id": 1, "age": 30,   "score": 0.5},'
        ' {"id": 2, "age": null, "score": 0.6},'
        ' {"id": 3, "age": "",   "score": ""}]'
    )
    result = DataValidator(
        schema={"id": "INT", "age": "INT", "score": "FLOAT"}
    ).validate(str(p))
    assert result.is_valid, f"expected valid; errors={result.errors}"


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

@pytest.mark.parametrize("dtype", ["FLOAT", "DOUBLE", "DECIMAL(10,2)", "NUMERIC(8,3)", "NUMERIC"])
def test_float_family_valid(dtype):
    # NUMERIC is a MySQL alias for DECIMAL; #190 bugbot caught that the DDL
    # and ingestor type-cast layers accepted NUMERIC but the validator's
    # type_validators dict didn't, so a schema using NUMERIC failed preflight
    # with "Unknown data type" even though ingest would proceed.
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
# Non-finite (inf) + realistic messy-data scenarios
# ---------------------------------------------------------------------------

def test_float_infinity_is_rejected():
    # inf survives pd.to_numeric (it is "numeric") but flows through the training
    # scaler unchanged and produces a NaN loss — so the gate must reject it.
    df = pd.DataFrame({"x": [1.0, np.inf, -np.inf]})
    result = DataValidator(schema={"x": "FLOAT"}).validate(df)
    assert not result.is_valid
    assert "non-finite" in result.errors[0]


def test_int_infinity_is_rejected():
    df = pd.DataFrame({"n": [1.0, np.inf, 3.0]})
    result = DataValidator(schema={"n": "INT"}).validate(df)
    assert not result.is_valid
    assert "non-finite" in result.errors[0]


def test_eu_comma_decimal_rejected_with_sample():
    # German/EU exports write "1,5" for 1.5 — non-numeric to pandas. It is
    # correctly rejected, and the sample value shows the user what to fix.
    df = pd.DataFrame({"x": ["1,5", "2,3"]})
    result = DataValidator(schema={"x": "FLOAT"}).validate(df)
    assert not result.is_valid
    assert "1,5" in result.errors[0]


def test_thousands_separator_rejected():
    df = pd.DataFrame({"n": ["1,234", "5,678"]})
    assert not DataValidator(schema={"n": "INT"}).validate(df).is_valid


def test_units_in_numeric_rejected_with_sample():
    df = pd.DataFrame({"x": ["95%", "3kg"]})
    result = DataValidator(schema={"x": "FLOAT"}).validate(df)
    assert not result.is_valid
    assert "95%" in result.errors[0] or "3kg" in result.errors[0]


def test_scientific_notation_is_valid():
    # Mass-spec / proteomics intensities are commonly in scientific notation.
    df = pd.DataFrame({"x": ["1.23E+08", "4.5e7"]})
    assert DataValidator(schema={"x": "FLOAT"}).validate(df).is_valid


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


def test_varchar_with_nulls_is_valid():
    # Regression: a VARCHAR column with genuine missing values was wrongly
    # reported as containing "non-string" values (NaN != NaN), an error the
    # user could never clear by editing data. NULLs are valid for any column.
    df = pd.DataFrame({"s": ["abc", None, "de"]})
    assert DataValidator(schema={"s": "VARCHAR(255)"}).validate(df).is_valid


def test_varchar_all_null_is_valid():
    # An entirely-empty column (e.g. an unmeasured biomarker / analyte in a
    # sparse panel) is a column of NULLs — valid, not N "non-string values".
    df = pd.DataFrame({"s": [None, None, None]})
    assert DataValidator(schema={"s": "VARCHAR(255)"}).validate(df).is_valid


def test_char_with_nulls_is_valid():
    df = pd.DataFrame({"s": ["ab", None, "cd"]})
    assert DataValidator(schema={"s": "CHAR(2)"}).validate(df).is_valid


def test_text_with_nulls_is_valid():
    df = pd.DataFrame({"s": ["any length text", None]})
    assert DataValidator(schema={"s": "TEXT"}).validate(df).is_valid


def test_varchar_still_flags_real_non_strings():
    # NULL tolerance must NOT mask a genuine type error: a real numeric value
    # in a VARCHAR column is still non-string and must be reported.
    df = pd.DataFrame({"s": ["abc", 5, "de"]})
    result = DataValidator(schema={"s": "VARCHAR(255)"}).validate(df)
    assert not result.is_valid
    assert "non-string" in result.errors[0]


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


@pytest.mark.parametrize("dtype", ["DATE", "DATETIME", "TIMESTAMP", "TIME"])
def test_date_family_with_nulls_is_valid(dtype):
    # Regression: a date column with genuine missing values was wrongly
    # reported as "invalid date values" (to_datetime turns NULL into NaT,
    # then isnull() counted it) — an unclearable error. NULLs are valid.
    df = pd.DataFrame({"d": ["2024-01-01", None, "2024-02-02"]})
    assert DataValidator(schema={"d": dtype}).validate(df).is_valid


def test_date_all_null_is_valid():
    df = pd.DataFrame({"d": [None, None, None]})
    assert DataValidator(schema={"d": "DATE"}).validate(df).is_valid


def test_date_still_flags_real_bad_value_with_nulls_present():
    # NULL tolerance must not mask a genuinely un-parseable date.
    df = pd.DataFrame({"d": ["2024-01-01", None, "not-a-date"]})
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


# ---------------------------------------------------------------------------
# Streaming / large-file validation (bounded memory, full coverage)
# ---------------------------------------------------------------------------

def test_validate_csv_streams_and_catches_late_chunk(tmp_path):
    # A bad value PAST the first chunk must still be caught — the validator
    # scans the whole file chunk by chunk (memory-bounded), not just a sample
    # and not by loading the entire file. chunk_size forces multiple chunks.
    p = tmp_path / "big.csv"
    rows = [str(i) for i in range(50)]
    rows[35] = "not-an-int"  # lands in the 4th chunk of 10
    p.write_text("n\n" + "\n".join(rows) + "\n")
    res = DataValidator(schema={"n": "INT"}).validate(str(p), chunk_size=10)
    assert not res.is_valid and "non-numeric" in res.errors[0]


def test_validate_csv_streaming_clean_file_is_valid(tmp_path):
    p = tmp_path / "clean.csv"
    p.write_text("n\n" + "\n".join(str(i) for i in range(50)) + "\n")
    res = DataValidator(schema={"n": "INT"}).validate(str(p), chunk_size=10)
    assert res.is_valid and res.metadata["rows_checked"] == 50


def test_validate_strips_header_whitespace_to_match_ingestor():
    # The ingestor strips header whitespace on every chunk (" age" -> "age");
    # the validator must do the same so it validates the column the ingestor
    # will actually ingest, rather than silently skipping it.
    res = DataValidator(schema={"age": "INT"}).validate(
        pd.DataFrame({" age": [1, "bad"]})
    )
    assert not res.is_valid
