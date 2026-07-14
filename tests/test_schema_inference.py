"""Owned tabular schema-inference rules (#349, RFC-0002 §8.1).

`schema_inference.infer_column_type` is the platform's source of truth for
turning raw column values into SQL types; the Go CLI (`InferSchema`, cli#185)
mirrors it. These tests pin the rule precedence and the leading-zero fix, and
load the shared parity fixture so a change to the contract updates both sides
from one file (backend#1009).
"""

import json
from pathlib import Path

import pandas as pd
import pytest

from tracebloc_ingestor.schema_inference import (
    SAMPLE_CAP,
    canonical_dtype,
    infer_column_type,
    infer_schema,
)


@pytest.mark.parametrize(
    "sql_type,expected",
    [
        ("INT", "int"),
        ("integer", "int"),
        ("BIGINT", "int"),
        ("SMALLINT", "int"),
        ("TINYINT", "int"),
        ("FLOAT", "float"),
        ("DOUBLE", "float"),
        ("DECIMAL(10,2)", "float"),
        ("NUMERIC", "float"),
        ("BOOLEAN", "bool"),
        ("BOOL", "bool"),
        ("VARCHAR(255)", "string"),
        ("CHAR(3)", "string"),
        ("TEXT", "string"),
        ("DATE", "date"),
        ("DATETIME", "datetime"),
        ("TIMESTAMP", "datetime"),
        ("TIME", "time"),
        ("BLOB", "binary"),
    ],
)
def test_canonical_dtype_maps_storage_types(sql_type, expected):
    assert canonical_dtype(sql_type) == expected


def test_canonical_dtype_folds_width_and_size_variants():
    # Width/size differences must not read as a type divergence at combine time.
    assert canonical_dtype("VARCHAR(255)") == canonical_dtype("VARCHAR(100)")
    assert canonical_dtype("INT") == canonical_dtype("BIGINT")


def test_canonical_dtype_unknown_falls_through_lowercased():
    # Honest passthrough rather than a silent coercion to "string".
    assert canonical_dtype("GEOMETRY") == "geometry"

_FIXTURE = Path(__file__).parent / "fixtures" / "schema_inference_parity.json"
_PARITY = json.loads(_FIXTURE.read_text())


# ---------------------------------------------------------------------------
# Parity fixture — the contract the CLI must also satisfy.
# ---------------------------------------------------------------------------

def test_parity_fixture_cap_matches_module():
    # The fixture advertises the sampling cap; it must equal the code's cap so
    # a CLI reading the fixture caps identically.
    assert _PARITY["sample_cap"] == SAMPLE_CAP


@pytest.mark.parametrize(
    "case", _PARITY["cases"], ids=[c["name"] for c in _PARITY["cases"]]
)
def test_parity_cases(case):
    assert infer_column_type(case["values"]) == case["expected"]


# ---------------------------------------------------------------------------
# The #349 fix — leading-zero codes are text, not INT.
# ---------------------------------------------------------------------------

def test_leading_zero_code_is_varchar_not_int():
    # THE bug: "007" must never infer INT (it would corrupt to 7). A single
    # leading-zero token pins the whole column to text.
    assert infer_column_type(["007", "0012"]) == "VARCHAR(4)"
    assert infer_column_type(["123", "007", "456"]) == "VARCHAR(3)"


def test_plain_zero_is_still_int():
    # "0" alone is not a code (length 1, no padding) — a real 0/1/2 column.
    assert infer_column_type(["0", "1", "2"]) == "INT"


def test_signed_leading_zero_is_varchar():
    assert infer_column_type(["+007", "12"]).startswith("VARCHAR")


# ---------------------------------------------------------------------------
# Positive inference — one real column of each type.
# ---------------------------------------------------------------------------

def test_int_column_infers_int():
    assert infer_column_type(["10", "20", "-30"]) == "INT"


def test_large_int_infers_bigint():
    assert infer_column_type(["3000000000", "42"]) == "BIGINT"


def test_float_column_infers_float():
    assert infer_column_type(["3.14", "-0.5", "1e3"]) == "FLOAT"


def test_bool_column_infers_boolean():
    assert infer_column_type(["yes", "no", "YES"]) == "BOOLEAN"
    assert infer_column_type(["true", "false"]) == "BOOLEAN"


def test_date_column_infers_date():
    assert infer_column_type(["2024-01-02", "2024-12-31"]) == "DATE"


def test_datetime_column_infers_datetime():
    assert infer_column_type(["2024-01-02 13:45:00", "2024-01-03 08:00:00"]) == "DATETIME"


def test_text_column_infers_varchar():
    assert infer_column_type(["apple", "banana", "kiwi"]) == "VARCHAR(6)"


# ---------------------------------------------------------------------------
# Negative / precedence — the ambiguous cases resolve deterministically.
# ---------------------------------------------------------------------------

def test_numeric_id_is_int_not_date():
    # An 8-digit id must stay INT — numeric is checked before date so an id
    # column can't be mis-read as a calendar date.
    assert infer_column_type(["20240101", "20240102"]) == "INT"


def test_zero_one_is_int_not_bool():
    # 0/1 is ambiguous; the lossless INT reading wins over BOOL.
    assert infer_column_type(["0", "1", "1", "0"]) == "INT"


def test_infinity_is_text_not_float():
    # A column of "inf" must not be called numeric (it isn't finite).
    assert infer_column_type(["inf", "inf"]).startswith("VARCHAR")


def test_missing_only_column_is_varchar1():
    assert infer_column_type(["", "NA", "null", None]) == "VARCHAR(1)"


def test_missing_values_are_ignored():
    # NA sentinels don't change the inferred type of the present values.
    assert infer_column_type(["1", "NA", "2", ""]) == "INT"


# ---------------------------------------------------------------------------
# Sampling cap — only the first SAMPLE_CAP rows influence the type.
# ---------------------------------------------------------------------------

def test_off_type_token_past_cap_is_ignored():
    # 5,000 clean ints, then a float and a leading-zero code beyond the cap:
    # the column still infers INT because inference stops at the cap.
    values = [str(i) for i in range(SAMPLE_CAP)] + ["3.5", "007"]
    assert infer_column_type(values) == "INT"


def test_within_cap_off_type_token_counts():
    # The same off-type token INSIDE the cap does change the verdict.
    values = [str(i) for i in range(SAMPLE_CAP - 1)] + ["3.5"]
    assert infer_column_type(values) == "FLOAT"


# ---------------------------------------------------------------------------
# infer_schema — DataFrame and mapping forms.
# ---------------------------------------------------------------------------

def test_infer_schema_from_mapping():
    schema = infer_schema({"code": ["007", "012"], "age": ["30", "40"]})
    assert schema == {"code": "VARCHAR(3)", "age": "INT"}


def test_infer_schema_from_dataframe():
    # Read as strings (the input contract) so the leading zeros survive to
    # inference — a pandas-typed frame would already be lossy.
    df = pd.DataFrame({"code": ["007", "012"], "flag": ["yes", "no"]}, dtype=str)
    assert infer_schema(df) == {"code": "VARCHAR(3)", "flag": "BOOLEAN"}


# ---------------------------------------------------------------------------
# ASCII-only numeric matching + charset — Go-CLI parity guardrails.
# ---------------------------------------------------------------------------

def test_unicode_digits_are_text_not_int():
    # `\d`/int() accept Unicode digits, but the Go mirror matches [0-9] only and
    # MySQL INT can't hold the glyphs — so a non-ASCII-digit column is VARCHAR.
    assert infer_column_type(["١٢٣", "٤٥٦"]) == "VARCHAR(3)"


def test_underscore_grouped_numbers_are_text_not_float():
    # Python float("1_000")==1000.0, but Go's strconv.ParseFloat rejects
    # underscores — pre-screening with an ASCII float grammar keeps them text.
    assert infer_column_type(["1_000", "2_000"]) == "VARCHAR(5)"
    assert infer_column_type(["1_000", "2.5"]) == "VARCHAR(5)"


def test_varchar_length_is_char_count_not_bytes():
    # VARCHAR(n) counts characters (MySQL semantics); a multibyte value must not
    # be sized by its UTF-8 byte length (which is what a naive Go len() gives).
    assert infer_column_type(["café", "naïve"]) == "VARCHAR(5)"  # not 6 (bytes)


def test_mixed_timezone_datetimes_do_not_crash():
    # Mixed UTC offsets parse to an object-dtype Series (no .dt accessor); the
    # function must fall back to text, never raise AttributeError.
    got = infer_column_type(
        ["2024-01-01T00:00:00+00:00", "2024-01-01T00:00:00+05:00"]
    )
    assert got.startswith("VARCHAR")
