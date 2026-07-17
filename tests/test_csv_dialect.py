"""Tests for the shared CSV read-dialect passthrough (#371).

The single source of truth every CSV-reading validator uses so it tokenizes
the manifest byte-identically to CSVIngestor — see
``tracebloc_ingestor/utils/csv_dialect.py``.
"""

from __future__ import annotations

from tracebloc_ingestor.utils.csv_dialect import (
    READ_DIALECT_KEYS,
    read_dialect_kwargs,
)


def test_defaults_comma_and_bom_safe_encoding():
    # No options -> comma separator + BOM-stripping utf-8-sig (Excel export).
    kw = read_dialect_kwargs(None)
    assert kw["sep"] == ","
    assert kw["encoding"] == "utf-8-sig"


def test_none_and_empty_are_equivalent():
    assert read_dialect_kwargs({}) == read_dialect_kwargs(None)


def test_forwards_only_dialect_keys():
    # Restructuring keys (usecols/nrows/header/index_col/...) must be dropped —
    # forwarding them would collide with a validator's own read shape.
    kw = read_dialect_kwargs(
        {
            "sep": ";",
            "quotechar": "'",
            "usecols": ["a"],
            "nrows": 5,
            "header": 0,
            "index_col": "id",
        }
    )
    assert kw["sep"] == ";"
    assert kw["quotechar"] == "'"
    assert "usecols" not in kw
    assert "nrows" not in kw
    assert "header" not in kw
    assert "index_col" not in kw


def test_delimiter_alias_suppresses_default_sep():
    # A caller that passes `delimiter` must NOT also get a defaulted `sep`
    # (pandas rejects both at once).
    kw = read_dialect_kwargs({"delimiter": "\t"})
    assert kw["delimiter"] == "\t"
    assert "sep" not in kw


def test_explicit_non_utf8_encoding_preserved():
    assert read_dialect_kwargs({"encoding": "latin-1"})["encoding"] == "latin-1"


def test_dialect_keys_are_tokenizing_only():
    # Guard the whitelist against accidental inclusion of restructuring keys.
    for restructuring in ("usecols", "nrows", "header", "names", "skiprows", "index_col"):
        assert restructuring not in READ_DIALECT_KEYS
