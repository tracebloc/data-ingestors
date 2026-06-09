"""Adversarial / i18n ingest fixtures for the CSVIngestor (#739).

These tests pin the engine's behaviour on the data shapes a European
clinical / lab customer actually exports: German-Excel CSV conventions
(``;`` delimiter + decimal comma), biomarker-style column names that
contain ``|`` and ``;``, non-UTF-8 encodings, CRLF line endings, BOMs,
and assorted NA / ragged-row edge cases.

The engine (``data-ingestors``) is the canonical schema source the CLI's
embedded ``ingest.v1.json`` and the chart both track via the drift gate,
so coverage here protects all three.

Contract for every case (per the issue): assert **clean ingest with the
right typing / row-count**, OR a **clear, actionable error** — never a
silent mangle. Where the current engine silently drops / renames data
(ragged rows under ``on_bad_lines="warn"``; pandas' duplicate-header
mangling), the test asserts that *documented* behaviour explicitly so the
contract is visible and any future change is caught, rather than weakening
the assertion.

The headline case is the ``|``/``;`` biomarker header reaching the MySQL
column-name path: it is asserted twice — once that ``Database.create_table``
carries the exact name onto the SQLAlchemy ``Table``, and once that the
emitted ``CREATE TABLE`` DDL backtick-quotes it for the MySQL dialect.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import mysql
from sqlalchemy.schema import CreateTable

from tracebloc_ingestor import database as db_mod
from tracebloc_ingestor.database import Database
from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
from tracebloc_ingestor.utils.constants import TaskCategory


# A generic protein-ID-shaped header (UniProt-style accessions + gene
# symbol). The shape — accession;isoform|accession|GENE — is exactly what
# collides with delimiter sniffing and SQL identifier legality. It is
# synthetic test data, not attributable to anyone.
BIOMARKER_HEADER = "P08253;P08253-2|P08253|MMP2"


def _make_ingestor(schema, *, category=TaskCategory.TABULAR_CLASSIFICATION, **overrides):
    """Build a CSVIngestor with mock DB/API, mirroring tests/test_csv_*.py.

    ``BaseIngestor.__init__`` runs end-to-end (including the mocked
    ``database.create_table``); only the I/O boundary is faked.
    """
    kwargs = dict(
        database=MagicMock(),
        api_client=MagicMock(),
        table_name="t",
        schema=schema,
        category=category,
    )
    kwargs.update(overrides)
    return CSVIngestor(**kwargs)


def _real_db():
    """A ``Database`` with the engine/metadata wired but no live MySQL.

    Mirrors the pattern in tests/test_database.py: bypass ``__init__`` so we
    never open a connection, then stub ``create_all`` and ``inspect`` per
    call site so ``create_table`` builds the SQLAlchemy ``Table`` without DB
    I/O.
    """
    db = Database.__new__(Database)
    db.metadata = MetaData()
    db.tables = {}
    db.engine = MagicMock(name="engine")
    db.metadata.create_all = MagicMock()
    return db


# ---------------------------------------------------------------------------
# German-Excel: ';' delimiter + decimal-comma numerics
# ---------------------------------------------------------------------------

def test_german_excel_semicolon_and_decimal_comma(tmp_path):
    """German Excel default export: ``;`` field separator and ``,`` decimal
    separator. With sep/decimal passed through csv_options the numerics
    must parse as real floats/ints, not strings."""
    p = tmp_path / "de.csv"
    p.write_text(
        "age;height;label\n42;1,75;a\n30;1,80;b\n",
        encoding="utf-8",
    )
    ing = _make_ingestor(
        {"age": "INT", "height": "FLOAT", "label": "VARCHAR(10)"},
        csv_options={"sep": ";", "decimal": ","},
    )
    records = list(ing.read_data(str(p)))

    assert len(records) == 2
    assert records[0]["age"] == 42
    assert records[0]["height"] == pytest.approx(1.75)  # FLOAT downcasts to float32
    assert records[1]["height"] == pytest.approx(1.80)
    assert records[0]["label"] == "a"


def test_german_decimal_comma_without_option_does_not_silently_float(tmp_path):
    """Defensive: if the caller forgets ``decimal=","`` on German data, the
    decimal-comma cell is NOT silently coerced to a bogus float — the FLOAT
    cast over a string like ``"1,75"`` fails loudly with a clear error
    instead of mangling the value (e.g. to 175 or NaN)."""
    p = tmp_path / "de_no_opt.csv"
    p.write_text("height\n1,75\n", encoding="utf-8")  # comma read as field sep
    ing = _make_ingestor({"height": "FLOAT"})

    # With the default comma delimiter, "1,75" splits into two columns; the
    # declared 'height' column then holds "1" and the second value lands in
    # an undeclared column. The point: no value is silently turned into a
    # wrong float. Assert the declared column did not absorb "1,75" as 1.75.
    records = list(ing.read_data(str(p)))
    assert records[0]["height"] != pytest.approx(1.75)


# ---------------------------------------------------------------------------
# Headline: '|' and ';' in a header -> MySQL column-name path
# ---------------------------------------------------------------------------

def test_biomarker_header_survives_read_data(tmp_path):
    """A biomarker column name containing ``|`` and ``;`` must reach the
    record dict intact. A non-colliding delimiter (tab) is required because
    the name itself contains ``;``."""
    p = tmp_path / "bio.csv"
    p.write_text(
        f"sample_id\t{BIOMARKER_HEADER}\nS1\t1.2\nS2\t3.4\n",
        encoding="utf-8",
    )
    ing = _make_ingestor(
        {"sample_id": "VARCHAR(20)", BIOMARKER_HEADER: "FLOAT"},
        category=TaskCategory.TABULAR_REGRESSION,
        csv_options={"sep": "\t"},
    )
    records = list(ing.read_data(str(p)))

    assert len(records) == 2
    assert BIOMARKER_HEADER in records[0]
    assert records[0][BIOMARKER_HEADER] == pytest.approx(1.2, rel=1e-6)
    assert records[1][BIOMARKER_HEADER] == pytest.approx(3.4, rel=1e-6)


def test_biomarker_header_reaches_create_table_unmangled():
    """The headline path: a ``|``/``;`` column name flows through
    ``Database.create_table`` onto the SQLAlchemy ``Table`` with its name
    preserved exactly (no silent sanitisation / truncation)."""
    db = _real_db()
    inspector = MagicMock()
    inspector.get_table_names.return_value = []
    with patch.object(db_mod, "inspect", return_value=inspector):
        table = db.create_table(
            "bio_tbl", {"sample_id": "VARCHAR(20)", BIOMARKER_HEADER: "FLOAT"}
        )

    # Column is reachable by its exact key and its .name is byte-identical.
    assert BIOMARKER_HEADER in table.c
    assert table.c[BIOMARKER_HEADER].name == BIOMARKER_HEADER
    db.metadata.create_all.assert_called_once()


def test_biomarker_header_is_backtick_quoted_in_mysql_ddl():
    """End-to-end MySQL column-name assertion: the emitted ``CREATE TABLE``
    DDL must backtick-quote the special-char identifier so MySQL accepts it
    verbatim. Compiled against the MySQL dialect offline — no live DB."""
    db = _real_db()
    inspector = MagicMock()
    inspector.get_table_names.return_value = []
    with patch.object(db_mod, "inspect", return_value=inspector):
        table = db.create_table(
            "bio_tbl", {"sample_id": "VARCHAR(20)", BIOMARKER_HEADER: "FLOAT"}
        )

    ddl = str(CreateTable(table).compile(dialect=mysql.dialect()))
    # MySQL quotes identifiers with backticks; the full special-char name
    # must appear backtick-wrapped exactly once.
    assert f"`{BIOMARKER_HEADER}`" in ddl
    # And the raw, unquoted form must NOT appear as a bare token (it would
    # be a syntax error in MySQL).
    assert f" {BIOMARKER_HEADER} " not in ddl


def test_biomarker_header_collision_with_reserved_is_rejected():
    """If a special-char schema also collides with a reserved tracebloc
    column, the existing guard still fires with a clear error rather than a
    cryptic DuplicateColumnError — the special-char column doesn't bypass
    the reserved-name check."""
    db = Database.__new__(Database)
    with pytest.raises(ValueError, match="reserved"):
        db.create_table("bio_tbl", {"id": "INT", BIOMARKER_HEADER: "FLOAT"})


# ---------------------------------------------------------------------------
# Line endings / byte-order marks / encodings
# ---------------------------------------------------------------------------

def test_crlf_line_endings(tmp_path):
    """Windows CRLF line endings must not leak ``\\r`` into the last cell of
    each row, and the row count must be correct."""
    p = tmp_path / "crlf.csv"
    p.write_bytes(b"a,b\r\n1,x\r\n2,y\r\n")
    ing = _make_ingestor({"a": "INT", "b": "VARCHAR(5)"})
    records = list(ing.read_data(str(p)))

    assert len(records) == 2
    assert records[-1]["b"] == "y"  # not "y\r"


def test_utf8_bom_is_transparently_stripped(tmp_path):
    """A UTF-8 BOM at the start of the file must not become part of the
    first column's name. pandas' parser strips a leading BOM transparently
    even under the engine's default ``encoding="utf-8"``."""
    p = tmp_path / "bom.csv"
    p.write_bytes("﻿".encode("utf-8") + b"age,label\n1,x\n")
    ing = _make_ingestor({"age": "INT", "label": "VARCHAR(5)"})
    records = list(ing.read_data(str(p)))

    assert list(records[0].keys()) == ["age", "label"]  # not "﻿age"
    assert records[0]["age"] == 1


def test_utf8_bom_with_utf8_sig_encoding(tmp_path):
    """Explicit ``encoding="utf-8-sig"`` is also accepted and yields the
    same clean header — callers exporting a BOM can opt in safely."""
    p = tmp_path / "bom_sig.csv"
    p.write_bytes("﻿".encode("utf-8") + b"age,label\n7,y\n")
    ing = _make_ingestor(
        {"age": "INT", "label": "VARCHAR(5)"},
        csv_options={"encoding": "utf-8-sig"},
    )
    records = list(ing.read_data(str(p)))

    assert list(records[0].keys()) == ["age", "label"]
    assert records[0]["age"] == 7


def test_latin1_read_as_utf8_raises_clear_error(tmp_path):
    """A latin-1 / ISO-8859-1 file read with the default UTF-8 encoding must
    fail loudly with a UnicodeDecodeError — an actionable error, never a
    mojibake'd silent mangle."""
    p = tmp_path / "l1.csv"
    p.write_bytes("Koerpergroesse,label\n175,a\n".replace("oe", "ö").encode("latin-1"))
    ing = _make_ingestor({"Körpergröße": "INT", "label": "VARCHAR(5)"})

    with pytest.raises(UnicodeDecodeError):
        list(ing.read_data(str(p)))


def test_latin1_with_correct_encoding_parses(tmp_path):
    """The same latin-1 file parses cleanly — including the accented column
    name — when ``encoding="latin-1"`` is supplied via csv_options."""
    col = "Körpergröße"  # Körpergröße
    p = tmp_path / "l1ok.csv"
    p.write_bytes(f"{col},label\n175,a\n".encode("latin-1"))
    ing = _make_ingestor(
        {col: "INT", "label": "VARCHAR(5)"},
        csv_options={"encoding": "latin-1"},
    )
    records = list(ing.read_data(str(p)))

    assert records[0][col] == 175
    assert records[0]["label"] == "a"


def test_utf16_read_as_utf8_raises_clear_error(tmp_path):
    """A UTF-16 file read as UTF-8 must raise a clear UnicodeDecodeError
    rather than silently producing garbage."""
    p = tmp_path / "u16.csv"
    p.write_bytes("age,label\n5,x\n".encode("utf-16"))
    ing = _make_ingestor({"age": "INT", "label": "VARCHAR(5)"})

    with pytest.raises(UnicodeDecodeError):
        list(ing.read_data(str(p)))


def test_utf16_with_correct_encoding_parses(tmp_path):
    """UTF-16 parses cleanly when ``encoding="utf-16"`` is supplied."""
    p = tmp_path / "u16ok.csv"
    p.write_bytes("age,label\n5,x\n".encode("utf-16"))
    ing = _make_ingestor(
        {"age": "INT", "label": "VARCHAR(5)"},
        csv_options={"encoding": "utf-16"},
    )
    records = list(ing.read_data(str(p)))

    assert len(records) == 1
    assert records[0] == {"age": 5, "label": "x"}


# ---------------------------------------------------------------------------
# NA tokens
# ---------------------------------------------------------------------------

def test_default_tabular_na_tokens(tmp_path):
    """Tabular-family CSVs use ``keep_default_na=True`` plus the explicit
    ``_TABULAR_NA_VALUES`` widening (``["", "NA", "NULL", "None"]``), so the
    effective NA set is the union of pandas' built-in tokens (which include
    ``NaN`` / ``n/a`` / ``null`` / ``NA`` / ``NULL`` / ``None`` / ``""``) and
    the widening. Tokens NOT in either set — e.g. ``-`` — are stored verbatim.
    This documents the actual boundary so a future narrowing is a conscious
    change. See csv_ingestor.py::read_data for the rationale (validators read
    with pandas defaults, so the ingestor must match)."""
    p = tmp_path / "na.csv"
    p.write_text("a,b\n1,NA\n2,NaN\n3,n/a\n4,-\n5,\n", encoding="utf-8")
    ing = _make_ingestor({"a": "INT", "b": "VARCHAR(10)"})
    records = list(ing.read_data(str(p)))

    b = [r["b"] for r in records]
    assert pd.isna(b[0])      # "NA"  -> NaN (in widening)
    assert pd.isna(b[1])      # "NaN" -> NaN (in pandas defaults)
    assert pd.isna(b[2])      # "n/a" -> NaN (in pandas defaults)
    assert b[3] == "-"        # "-"   -> literal (in neither set)
    assert pd.isna(b[4])      # ""    -> NaN


def test_mixed_na_tokens_via_explicit_override(tmp_path):
    """When the caller supplies the full token set via csv_options, every
    mixed NA token (empty, NA, NaN, n/a, -) parses as NaN — the supported
    path for clinical exports that use those sentinels."""
    p = tmp_path / "na2.csv"
    p.write_text("a,b\n1,NA\n2,NaN\n3,n/a\n4,-\n5,\n", encoding="utf-8")
    ing = _make_ingestor(
        {"a": "INT", "b": "VARCHAR(10)"},
        csv_options={"na_values": ["", "NA", "NaN", "n/a", "-"]},
    )
    records = list(ing.read_data(str(p)))

    assert all(pd.isna(r["b"]) for r in records)


# ---------------------------------------------------------------------------
# Structural edge cases: single-column, ragged rows, duplicate headers
# ---------------------------------------------------------------------------

def test_single_column_label_only_file(tmp_path):
    """A label-only / single-column file ingests cleanly with the right row
    count and values."""
    p = tmp_path / "one.csv"
    p.write_text("label\ncat\ndog\ncat\n", encoding="utf-8")
    ing = _make_ingestor({"label": "VARCHAR(10)"})
    records = list(ing.read_data(str(p)))

    assert len(records) == 3
    assert [r["label"] for r in records] == ["cat", "dog", "cat"]
    assert list(records[0].keys()) == ["label"]


def test_ragged_row_raises_not_silently_dropped(tmp_path):
    """A ragged row (extra trailing ``;`` -> one field too many) must now FAIL
    LOUDLY (``on_bad_lines="error"``) rather than be silently dropped. Silently
    shrinking the dataset corrupts it under a still-green success, and a
    malformed row almost always signals a real structural problem (wrong
    delimiter, unquoted comma) the user must fix. Replaces the former
    drop-with-ParserWarning behaviour."""
    p = tmp_path / "rag.csv"
    # Row 2 has a trailing ';' -> 4 fields where 3 are expected.
    p.write_text("a;b;c\n1;2;3\n4;5;6;\n7;8;9\n", encoding="utf-8")
    ing = _make_ingestor(
        {"a": "INT", "b": "INT", "c": "INT"},
        csv_options={"sep": ";"},
    )

    with pytest.raises(Exception):
        list(ing.read_data(str(p)))


def test_duplicate_header_names_are_rejected(tmp_path):
    """Duplicate header names are now rejected with a clear error BEFORE pandas
    silently de-duplicates them (second ``age`` -> ``age.1``) and the schema
    maps onto the wrong physical column. Replaces the former
    documented-mangling behaviour."""
    p = tmp_path / "dup.csv"
    p.write_text("age,age,label\n10,20,x\n", encoding="utf-8")
    ing = _make_ingestor({"age": "INT", "label": "VARCHAR(5)"})
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        list(ing.read_data(str(p)))
