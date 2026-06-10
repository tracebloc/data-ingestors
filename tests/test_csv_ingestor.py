"""Tests for CSVIngestor: read_data chunking, schema type validation, counting."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
from tracebloc_ingestor.utils.constants import TaskCategory


def make_csv_ingestor(schema=None, **overrides):
    db = MagicMock()
    db.create_table.return_value = MagicMock()
    api = MagicMock()
    kwargs = dict(
        database=db,
        api_client=api,
        table_name="tbl",
        schema=schema if schema is not None else {"a": "INT"},
        intent="train",
        category=None,
    )
    kwargs.update(overrides)
    return CSVIngestor(**kwargs)


def test_read_data_yields_records(make_csv):
    path = make_csv({"a": [1, 2], "b": ["x", "y"]})
    ing = make_csv_ingestor(schema={"a": "INT", "b": "VARCHAR(10)"})
    records = list(ing.read_data(str(path)))
    assert len(records) == 2
    assert records[0]["a"] == 1
    assert records[0]["b"] == "x"


def test_read_data_missing_file_raises():
    ing = make_csv_ingestor()
    with pytest.raises(FileNotFoundError):
        list(ing.read_data("/no/such/file.csv"))


def test_read_data_strips_column_whitespace(tmp_path):
    p = tmp_path / "d.csv"
    p.write_text(" a , b \n1,2\n")
    ing = make_csv_ingestor(schema={"a": "INT", "b": "INT"})
    records = list(ing.read_data(str(p)))
    assert "a" in records[0] and "b" in records[0]


def test_read_data_preserves_leading_zeros_with_whitespace_header(tmp_path):
    # Regression (#190 bugbot): the dtype=str pin keyed by clean schema column
    # names is applied by pandas against the RAW header literals — before
    # `chunk.columns.str.strip()`. A header " code " missed the pin, pandas
    # inferred int from "007" -> 7, and the leading zeros were silently lost.
    # The fix probes the raw header and pins both the spaced and clean spellings.
    p = tmp_path / "d.csv"
    p.write_text(" code ,n\n007,1\n042,2\n")
    ing = make_csv_ingestor(schema={"code": "VARCHAR(10)", "n": "INT"})
    records = list(ing.read_data(str(p)))
    assert records[0]["code"] == "007", f"leading zeros lost; got {records[0]['code']!r}"
    assert records[1]["code"] == "042"


def test_read_data_schema_column_missing_raises(make_csv):
    path = make_csv({"a": [1]})
    ing = make_csv_ingestor(schema={"a": "INT", "missing": "INT"})
    with pytest.raises(ValueError, match="Schema columns not present"):
        list(ing.read_data(str(path)))


def test_read_data_unique_id_column_missing_raises(make_csv):
    path = make_csv({"a": [1]})
    ing = make_csv_ingestor(schema={"a": "INT"}, unique_id_column="uid")
    with pytest.raises(ValueError, match="unique_id_column"):
        list(ing.read_data(str(path)))


def test_read_data_empty_file_returns_nothing(tmp_path):
    p = tmp_path / "empty.csv"
    p.write_text("")
    ing = make_csv_ingestor(schema={})
    assert list(ing.read_data(str(p))) == []


def test_validate_csv_type_coercion():
    ing = make_csv_ingestor(schema={"n": "INT", "f": "FLOAT", "b": "BOOLEAN", "d": "DATE"})
    df = pd.DataFrame({
        "n": ["1", "2"],
        "f": ["1.5", "2.5"],
        "b": [True, False],
        "d": ["2024-01-01", "2024-02-02"],
    })
    # Should not raise; coerces in place. A DATE column now becomes plain
    # datetime.date objects (object dtype), NOT datetime64 — so no spurious
    # 00:00:00 time component is appended downstream (DATE vs DATETIME split).
    import datetime
    ing._validate_csv(df)
    assert all(
        isinstance(v, datetime.date) and not isinstance(v, datetime.datetime)
        for v in df["d"]
    )


def test_count_records(make_csv):
    path = make_csv({"a": [1, 2, 3, 4]})
    ing = make_csv_ingestor(schema={"a": "INT"})
    assert ing._count_records(str(path)) == 4


def test_count_records_bad_path_returns_none():
    ing = make_csv_ingestor()
    assert ing._count_records("/no/such.csv") is None


def test_tabular_na_values_applied(tmp_path):
    p = tmp_path / "d.csv"
    p.write_text("a,b\n1,NA\n2,NULL\n")
    ing = make_csv_ingestor(
        schema={"a": "INT", "b": "VARCHAR(10)"},
        category=TaskCategory.TABULAR_CLASSIFICATION,
    )
    records = list(ing.read_data(str(p)))
    # "NA"/"NULL" should be parsed as NaN for tabular categories.
    assert pd.isna(records[0]["b"])
    assert pd.isna(records[1]["b"])


def test_varchar_empty_cells_yield_python_none(tmp_path):
    """Missing cells in a VARCHAR column must yield Python None, NOT float NaN
    or the literal string 'nan'.

    Regression: before the type-coercion branch matched VARCHAR/CHAR (it only
    matched STRING/TEXT), pandas left the column as float64 with NaN for empty
    cells. itertuples emitted those NaN floats, dict() preserved them, and the
    SQL binder stringified them as 'nan' on INSERT — silent data corruption
    of every missing VARCHAR cell. #167 widened NULL-tolerance in the
    validator (so all-null VARCHAR no longer fails validation); without this
    write-side fix the column was happily ingested as a column of 'nan'
    strings instead of SQL NULLs.

    Surfaced by an end-to-end cluster ingestion with an all-empty VARCHAR(50)
    column: MySQL row count = 60, all rows had analyte_y = 'nan' (length 3).
    """
    p = tmp_path / "d.csv"
    # 'analyte_y' is all-empty (the original biomarker scenario from #167);
    # 'notes' mixes a value and an empty.
    p.write_text("a,analyte_y,notes\n1,,hello\n2,,\n3,,world\n")
    ing = make_csv_ingestor(
        schema={"a": "INT", "analyte_y": "VARCHAR(50)", "notes": "VARCHAR(100)"},
        category=TaskCategory.TABULAR_CLASSIFICATION,
    )
    records = list(ing.read_data(str(p)))

    assert len(records) == 3
    # All-empty VARCHAR column: every value must be None, not NaN, not "nan".
    for r in records:
        assert r["analyte_y"] is None, f"expected None, got {r['analyte_y']!r}"
    # Mixed VARCHAR column: present cells preserved; empty -> None.
    assert records[0]["notes"] == "hello"
    assert records[1]["notes"] is None
    assert records[2]["notes"] == "world"


def test_char_empty_cells_yield_python_none(tmp_path):
    """Same regression for CHAR — pre-fix code only matched STRING/TEXT, so
    CHAR(N) columns silently stored 'nan' for empties.
    """
    p = tmp_path / "d.csv"
    p.write_text("a,code\n1,A\n2,\n")
    ing = make_csv_ingestor(
        schema={"a": "INT", "code": "CHAR(1)"},
        category=TaskCategory.TABULAR_CLASSIFICATION,
    )
    records = list(ing.read_data(str(p)))

    assert records[0]["code"] == "A"
    assert records[1]["code"] is None


# ---------------------------------------------------------------------------
# #765 item 3 — date cast policy aligned with numeric (errors="raise")
# ---------------------------------------------------------------------------

def test_date_unparseable_raises_at_cast_not_silent_null(tmp_path):
    """Backend #765 item 3: the date branches were errors="coerce" (silent
    NULL) while the numeric branches were errors="raise" — opposite
    policies for the same bad-input class. Aligning on `raise`: DataValidator
    is the gate (catches bad dates at preflight); the cast trusts what
    passes. A token that reaches the cast un-parseable means a validator
    gap or schema mismatch — surface it loudly instead of silently NULLing.
    """
    p = tmp_path / "d.csv"
    p.write_text("d\n2024-01-01\nnot-a-date\n")
    ing = make_csv_ingestor(schema={"d": "DATE"})
    with pytest.raises(ValueError, match="un-parseable date"):
        list(ing.read_data(str(p)))


def test_datetime_unparseable_raises_at_cast(tmp_path):
    p = tmp_path / "d.csv"
    p.write_text("ts\n2024-01-01 10:00:00\nbroken\n")
    ing = make_csv_ingestor(schema={"ts": "DATETIME"})
    with pytest.raises(ValueError, match="un-parseable date"):
        list(ing.read_data(str(p)))


def test_time_unparseable_raises_at_cast(tmp_path):
    p = tmp_path / "d.csv"
    p.write_text("t\n10:00:00\nnoon\n")
    ing = make_csv_ingestor(schema={"t": "TIME"})
    with pytest.raises(ValueError, match="un-parseable date"):
        list(ing.read_data(str(p)))


def test_date_missing_cells_still_pass_through_as_null(tmp_path):
    """The strict cast must NOT flag legitimate missing values: an empty
    date cell stays as NaT/None. Only PRESENT, un-parseable values raise."""
    p = tmp_path / "d.csv"
    p.write_text("d,n\n2024-01-01,1\n,2\n2024-02-02,3\n")
    ing = make_csv_ingestor(
        schema={"d": "DATETIME", "n": "INT"},
        category=TaskCategory.TABULAR_CLASSIFICATION,
    )
    records = list(ing.read_data(str(p)))
    assert len(records) == 3
    assert records[1]["d"] is None or pd.isna(records[1]["d"])
