"""Adversarial ingestion harness — Murphy's-law coverage for raw user data.

WHY THIS FILE EXISTS
--------------------
Users dump *raw, un-preprocessed* data: Excel exports, instrument dumps, hand-
built CSVs, proteomics matrices with ``UniProt|gene`` headers, clinical tables
with ``yes/no`` booleans and European dates. Everything that can go wrong, will.
Four customer-blocking ingestion bugs in a row on one real dataset proved the
point. This harness throws the full zoo of raw-data pathologies at the *real*
ingest code and pins the behaviour.

THE CONTRACT EACH TEST ENFORCES
-------------------------------
For any input, the ingestor must do exactly one of:
  1. INGEST IT CORRECTLY (the value that lands in the DB is faithful), or
  2. FAIL WITH A CLEAR, ACTIONABLE ERROR naming what's wrong.

It must NEVER:
  - crash with a cryptic/non-actionable error,
  - SILENTLY CORRUPT a value (store something subtly wrong), or
  - SILENTLY DROP rows/columns while reporting success.

HOW TO READ THE RESULTS
-----------------------
- A PASSING test = the pipeline already honours the contract for that input.
- An ``xfail`` test = a KNOWN GAP: the pipeline violates the contract today.
  The test asserts the *correct* behaviour, so it currently fails and is marked
  expected-fail (``strict=True``). When the gap is fixed, the test starts
  PASSING, ``strict`` turns that into a SUITE FAILURE, and whoever fixed it is
  forced to delete the marker. That makes the iceberg of gaps an explicit,
  self-updating checklist instead of tribal knowledge.

Every ``xfail`` reason names the failure mode and (where known) the tracking
PR/issue. Run ``pytest tests/test_adversarial_ingestion_harness.py -v`` to see
the full checklist; ``-rxX`` also prints the xfail reasons.

Layers exercised (no live MySQL — engine/DB mocked at the boundary):
  - CSV read + type-cast:  CSVIngestor.read_data -> process_record
  - schema validation:     DataValidator.validate
  - DDL / identifiers:      Database.create_table (+ compiled MySQL DDL)
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import BigInteger, Column, MetaData, String, Table
from sqlalchemy.dialects import mysql
from sqlalchemy.schema import CreateTable

from tracebloc_ingestor import database as db_mod
from tracebloc_ingestor.database import Database
from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
from tracebloc_ingestor.utils.constants import TaskCategory
from tracebloc_ingestor.validators.data_validator import DataValidator

TAB = TaskCategory.TABULAR_CLASSIFICATION


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ingestor(schema, **opts):
    return CSVIngestor(
        database=MagicMock(),
        api_client=MagicMock(),
        table_name="t",
        schema=schema,
        category=opts.pop("category", TAB),
        intent=opts.pop("intent", "train"),
        **opts,
    )


def _ingest(schema, payload, *, tmp_path=None, name="d.csv", **opts):
    """Run the real cast path end to end and return the cleaned feature dicts.

    payload: str or bytes written verbatim as the CSV file (so we can inject
             BOMs, CRLFs, latin-1 bytes, ragged rows — genuinely raw files).
    Returns a list of dicts containing only the schema (feature) columns, with
    the exact values process_record would hand to the SQL binder. Exceptions
    propagate (a test asserts on them).
    """
    base = tmp_path if tmp_path is not None else Path(__import__("tempfile").mkdtemp())
    p = Path(base) / name
    p.write_bytes(payload.encode("utf-8") if isinstance(payload, str) else payload)
    ing = _ingestor(schema, **opts)
    raw = list(ing.read_data(str(p)))
    cleaned = [ing.process_record(r) for r in raw]
    return [{k: c[k] for k in schema if c and k in c} for c in cleaned]


def _real_db():
    """A Database with metadata + a mock engine, no live MySQL. Drives the real
    create_table / DDL-compilation logic (the reserved/length/reflect guards run
    before any I/O)."""
    db = Database.__new__(Database)
    db.metadata = MetaData()
    db.tables = {}
    db.engine = MagicMock(name="engine")
    db.metadata.create_all = MagicMock()
    return db


def _create_table(db, name, schema, existing_columns=None):
    """Invoke real Database.create_table with inspect() mocked. If
    existing_columns is given, simulate a pre-existing reflected table with
    those feature columns."""
    insp = MagicMock()
    insp.get_table_names.return_value = [name] if existing_columns is not None else []
    if existing_columns is not None:
        def fake_reflect(engine, only=None):
            cols = [Column("id", BigInteger, primary_key=True), Column("data_id", String(255))]
            cols += [Column(c, String(255)) for c in existing_columns]
            Table(name, db.metadata, *cols)
        db.metadata.reflect = MagicMock(side_effect=fake_reflect)
    with patch.object(db_mod, "inspect", return_value=insp):
        return db.create_table(name, schema)


def _ddl(table) -> str:
    return str(CreateTable(table).compile(dialect=mysql.dialect()))


# ===========================================================================
# Section A — Encoding & byte-level integrity
# ===========================================================================

def test_utf8_clean_round_trips():
    rows = _ingest({"city": "VARCHAR(40)"}, "city\nMünchen\nZürich\n")
    assert [r["city"] for r in rows] == ["München", "Zürich"]


def test_latin1_read_as_utf8_errors_not_silently():
    """A latin-1 file read as UTF-8 must surface an error, not silently mojibake.
    (The friendly preflight message lives in validate_data; at the read layer a
    decode error is acceptable — what matters is it is NOT silent.)"""
    payload = "city\nM\xfcnchen\n".encode("latin-1")  # 0xFC is invalid UTF-8
    with pytest.raises(Exception):
        _ingest({"city": "VARCHAR(40)"}, payload)


def test_utf8_bom_header_not_mangled():
    """A UTF-8 BOM on the first header must be stripped so the first column name
    still matches the schema — otherwise the first column silently vanishes."""
    payload = b"\xef\xbb\xbfid,city\n1,Berlin\n"
    rows = _ingest({"id": "INT", "city": "VARCHAR(40)"}, payload)
    assert rows and rows[0].get("id") is not None and rows[0].get("city") == "Berlin"


def test_crlf_line_endings_no_trailing_cr():
    rows = _ingest({"city": "VARCHAR(40)"}, "city\r\nBerlin\r\nParis\r\n")
    assert [r["city"] for r in rows] == ["Berlin", "Paris"]


# ===========================================================================
# Section B — CSV structural integrity
# ===========================================================================

def test_quoted_embedded_comma_preserved():
    rows = _ingest({"name": "VARCHAR(40)", "n": "INT"}, 'name,n\n"Smith, John",2\n')
    assert rows[0]["name"] == "Smith, John"


def test_wrong_delimiter_is_not_silent():
    """A semicolon-separated file ingested without sep=';' must NOT silently
    succeed as one garbage column — it should error (schema columns absent)."""
    with pytest.raises(Exception):
        _ingest({"a": "INT", "b": "INT"}, "a;b\n1;2\n3;4\n")


def test_semicolon_delimiter_with_option_works():
    rows = _ingest({"a": "INT", "b": "INT"}, "a;b\n1;2\n", csv_options={"sep": ";"})
    assert rows[0]["a"] == "1" and rows[0]["b"] == "2"


def test_ragged_row_is_a_hard_error_not_silent_drop():
    # REGRESSION GUARD (fixed): a ragged row (wrong field count) used to be
    # silently dropped (on_bad_lines='warn'), shrinking the dataset with no
    # signal and a still-green success. read_data now uses on_bad_lines='error',
    # so a malformed row fails loudly (naming the line) instead of vanishing.
    with pytest.raises(Exception):
        _ingest({"a": "INT", "b": "INT"}, "a,b\n1,2\n3,4,5\n6,7\n")


def test_duplicate_headers_rejected():
    # REGRESSION GUARD (fixed): duplicate header names used to be silently
    # de-duplicated by pandas (x -> x.1) and the second column dropped. read_data
    # now rejects them with a clear error before parsing.
    with pytest.raises(Exception):
        _ingest({"x": "INT"}, "x,x\n1,2\n")


def test_wide_file_column_count_guarded():
    # REGRESSION GUARD (fixed): create_table now fails fast above ~4000 columns
    # (MySQL's hard limit is 4096) with an actionable message, instead of a raw
    # MySQL 1117/1118 deep inside CREATE TABLE.
    db = _real_db()
    schema = {"f%d" % i: "FLOAT" for i in range(5000)}
    with pytest.raises(ValueError, match="column"):
        _create_table(db, "wide", schema)


# ===========================================================================
# Section C — Type casting (the soft underbelly)
# ===========================================================================

def test_bool_true_false_not_stringified():
    # REGRESSION GUARD (fixed): a CSV BOOL column comes back from pandas as
    # numpy.bool_; process_record now recognises it via pd.api.types.is_bool and
    # emits a Python bool, so MySQL writes TINYINT 1/0 instead of rejecting the
    # string 'True'. (base.py process_record.)
    rows = _ingest({"flag": "BOOL"}, "flag\nTrue\nFalse\n")
    assert rows[0]["flag"] in (True, False, 1, 0)
    assert rows[0]["flag"] != "True"


def test_string_booleans_ingest_cleanly():
    # REGRESSION GUARD (fixed): string booleans (yes/no/true/false/t/f/y/n/1/0)
    # that DataValidator accepts used to crash astype('boolean') ('Need to pass
    # bool-like values'). The BOOL branch now maps the accepted forms to a
    # nullable boolean, so validator and ingestor agree. (csv_ingestor._validate_csv.)
    rows = _ingest({"flag": "BOOL"}, "flag\nyes\nno\n")
    assert rows[0]["flag"] in (True, 1) and rows[1]["flag"] in (False, 0)


def test_int_plain_large_value_clean():
    rows = _ingest({"n": "INT"}, "n\n1000000000\n")
    assert rows[0]["n"] == "1000000000"


def test_int_scientific_notation_clean():
    # Pins behaviour: a lone scientific-notation int coerces to the integer
    # value, NOT '1000000000.0'. Guards against a float-stringify regression.
    rows = _ingest({"n": "INT"}, "n\n1e9\n")
    assert rows[0]["n"] == "1000000000"


def test_varchar_leading_zero_codes_preserved():
    # REGRESSION GUARD (fixed): leading-zero codes used to be lost even for
    # VARCHAR columns because pandas inferred the all-digit column as int at read
    # (dtype=None) before the VARCHAR cast. read_data now pins string-family
    # schema columns to dtype=str, so zip/accession/gene codes survive verbatim.
    rows = _ingest({"code": "VARCHAR(10)"}, "code\n007\n0012\n")
    assert [r["code"] for r in rows] == ["007", "0012"]


@pytest.mark.xfail(strict=True, reason=(
    "Same root cause as the VARCHAR case: a code column typed INT loses leading "
    "zeros (007 -> '7') because pandas infers int at read. A raw dumper who types "
    "a zip/accession/gene code as INT silently corrupts it."
))
def test_int_typed_codes_keep_zeros():
    rows = _ingest({"code": "INT"}, "code\n007\n0012\n")
    assert [r["code"] for r in rows] == ["007", "0012"]


def test_float_precision_preserved():
    # REGRESSION GUARD (fixed): FLOAT used to be downcast to float32, so 3.14
    # stringified as '3.140000104904175'. The numeric branch now keeps float64.
    rows = _ingest({"x": "FLOAT"}, "x\n3.14\n")
    assert rows[0]["x"] in ("3.14", 3.14)


def test_date_no_spurious_time_component():
    # REGRESSION GUARD (fixed): a DATE used to gain '... 00:00:00'. The DATE
    # branch now emits a plain date (.dt.date).
    rows = _ingest({"d": "DATE"}, "d\n2026-01-02\n")
    assert rows[0]["d"] == "2026-01-02"


def test_time_no_spurious_date_component():
    # REGRESSION GUARD (fixed): a TIME used to gain today's date. The TIME branch
    # now emits a plain time (.dt.time).
    rows = _ingest({"t": "TIME"}, "t\n14:30:00\n")
    assert rows[0]["t"] == "14:30:00"


def test_double_column_rejects_non_numeric():
    # REGRESSION GUARD (fixed): DOUBLE/DECIMAL had no _validate_csv branch, so
    # junk flowed straight to the DB. The numeric branch now covers them and
    # raises a clear per-column error on non-numeric input.
    with pytest.raises(Exception):
        _ingest({"x": "DOUBLE"}, "x\nabc\n")


def test_decimal_column_supported_and_coerced():
    # REGRESSION GUARD (new): DECIMAL/NUMERIC are now first-class — mapped to
    # SQLAlchemy Numeric in create_table (previously create_table rejected them
    # with "Unsupported MySQL type") and coerced/validated like other numerics.
    db = _real_db()
    table = _create_table(db, "t", {"conc": "DECIMAL(10,2)"})
    assert "conc" in {c.name for c in table.columns}
    rows = _ingest({"conc": "DECIMAL(10,2)"}, "conc\n3.14\n")
    assert rows[0]["conc"] in ("3.14", 3.14)


# ===========================================================================
# Section D — Missing-data semantics (a hardened strength — pin it)
# ===========================================================================

@pytest.mark.parametrize("token", ["", "NA", "NULL", "None"])
def test_tabular_na_tokens_become_none(token):
    rows = _ingest({"name": "VARCHAR(20)", "n": "INT"}, "name,n\n%s,5\n" % token)
    assert rows[0]["name"] is None
    assert rows[0]["n"] == "5"


def test_missing_int_cell_becomes_none():
    # A blank cell in an INT column round-trips to SQL NULL — a hardened strength.
    rows = _ingest({"a": "VARCHAR(5)", "n": "INT"}, "a,n\nx,\ny,7\n")
    assert rows[0]["n"] is None


def test_int_column_missing_cell_does_not_float_promote():
    # REGRESSION GUARD (fixed): a single blank cell used to promote the whole INT
    # column to float64, so 7 round-tripped as '7.0'. The INT branch now casts to
    # nullable Int64, so present integers stay integral and missing -> SQL NULL.
    rows = _ingest({"a": "VARCHAR(5)", "n": "INT"}, "a,n\nx,\ny,7\n")
    assert rows[1]["n"] == "7"


# ===========================================================================
# Section E — Identifiers & DDL (DB-layer guards)
# ===========================================================================

def test_special_char_header_backtick_quoted_in_ddl():
    # Proteomics UniProt|gene headers must be backtick-quoted in CREATE TABLE.
    db = _real_db()
    table = _create_table(db, "panel", {"P08254|MMP3": "FLOAT", "sample_id": "VARCHAR(20)"})
    ddl = _ddl(table)
    assert "`P08254|MMP3`" in ddl


def test_reserved_column_collision_clear_error():
    db = _real_db()
    with pytest.raises(ValueError, match="reserved"):
        _create_table(db, "t", {"id": "INT", "feature_0": "FLOAT"})


def test_overlong_column_name_clear_error():
    db = _real_db()
    long_name = "Protein_" + "X" * 70
    with pytest.raises(ValueError, match="64-character"):
        _create_table(db, "t", {long_name: "FLOAT", "f0": "FLOAT"})


@pytest.mark.xfail(strict=True, reason=(
    "PENDING #185: an existing table whose columns don't match the incoming "
    "schema is silently reflected and reused, then every insert dies with "
    "'Unconsumed column names'. #185 adds a fail-fast guard. Flips to pass when "
    "#185 merges to develop."
))
def test_stale_table_schema_mismatch_clear_error():
    db = _real_db()
    with pytest.raises(ValueError, match="match the dataset schema|stale"):
        _create_table(db, "IBD", {"P02452_COL1A1": "FLOAT"},
                      existing_columns=["P02452|COL1A1"])


# ===========================================================================
# Section F — Schema validation (DataValidator)
# ===========================================================================

def test_validator_flags_non_numeric_in_int():
    res = DataValidator(schema={"n": "INT"}).validate(pd.DataFrame({"n": ["1", "abc"]}))
    assert not res.is_valid and "non-numeric" in res.errors[0]


def test_validator_tolerates_nulls_in_typed_column():
    res = DataValidator(schema={"x": "FLOAT"}).validate(pd.DataFrame({"x": [1.5, None, 2.0]}))
    assert res.is_valid


def test_validator_rejects_infinity():
    res = DataValidator(schema={"x": "FLOAT"}).validate(pd.DataFrame({"x": [1.0, float("inf")]}))
    assert not res.is_valid


def test_validator_scans_beyond_first_1000_rows():
    # REGRESSION GUARD (fixed): DataValidator used to sample only the first 1000
    # rows, so a bad value past row 1000 passed validation and then corrupted or
    # crashed the ingest. It now scans the whole column (default sample_size=None).
    good = ["1"] * 1500
    good[1200] = "not-an-int"  # poison well past the old 1000-row window
    res = DataValidator(schema={"n": "INT"}).validate(pd.DataFrame({"n": good}))
    assert not res.is_valid


# ===========================================================================
# Section G — extended battery: special characters & size (the long tail)
# Each case was probed empirically against the current pipeline before being
# pinned here. "Anything that could go wrong" — headers, values, SQL safety,
# overflow, line endings.
# ===========================================================================

# --- Special characters in HEADERS (read/cast layer) -----------------------

@pytest.mark.parametrize("header", [
    "gene name",        # space
    "conc (mg/L)",      # parentheses + slash
    "feature.1",        # dot
    "温度",             # non-latin unicode
    "col_🔥",           # emoji
    "select",           # SQL reserved word
])
def test_special_char_header_ingests(header):
    rows = _ingest({header: "INT"}, "%s\n5\n" % header)
    assert rows[0][header] == "5"


# --- SQL safety of headers at the DDL layer (the injection-adjacent worry) --

def test_header_with_backtick_is_escaped_in_ddl():
    # A backtick in a header must be doubled (MySQL identifier escaping) so it
    # cannot break out of the quoted identifier.
    db = _real_db()
    table = _create_table(db, "t", {"a`b": "FLOAT"})
    assert "`a``b`" in _ddl(table)


def test_sql_injection_header_is_neutralised_in_ddl():
    # A crafted "injection" header is emitted as ONE backtick-quoted identifier
    # (backtick doubled); the trailing SQL is inert inside it, never an
    # executable statement. A CSV header therefore cannot inject SQL at CREATE
    # TABLE. Locks SQLAlchemy's quoting against a future hand-rolled DDL path
    # that might string-interpolate the name.
    db = _real_db()
    evil = "x`; DROP TABLE y; --"
    ddl = _ddl(_create_table(db, "t", {evil: "FLOAT"}))
    assert "`x``; DROP TABLE y; --`" in ddl


def _capture_upsert_sql(table_name, schema, record):
    """Drive the real insert_batch with a capturing connection; return the
    compiled ON DUPLICATE KEY UPDATE statement (MySQL dialect)."""
    db = _real_db()
    _create_table(db, table_name, schema)
    sqls = []
    conn = MagicMock()

    def _exec(stmt, *a, **k):
        try:
            sqls.append(str(stmt.compile(dialect=mysql.dialect())))
        except Exception:
            sqls.append("")
        res = MagicMock()
        res.fetchall.return_value = []
        res.fetchone.return_value = None
        return res

    conn.execute.side_effect = _exec
    db.engine.connect.return_value.__enter__.return_value = conn
    db.insert_batch(table_name, [record])
    return next((s for s in sqls if "DUPLICATE" in s.upper()), "")


@pytest.mark.xfail(strict=True, reason=(
    "PENDING #184: the upsert ON DUPLICATE KEY UPDATE builds VALUES(col) via a "
    "raw f-string that does NOT backtick-quote the column name, so a special-char "
    "header (P08254|MMP3) breaks the SQL (1064) and is injection-adjacent. #184 "
    "switches to insert_stmt.inserted[col], which quotes. Flips to pass when #184 "
    "merges. (Contrast: CREATE TABLE DDL already escapes — the two tests above.)"
))
def test_upsert_quotes_special_char_column():
    sql = _capture_upsert_sql(
        "t", {"P08254|MMP3": "FLOAT"}, {"data_id": "x", "P08254|MMP3": 1.0}
    )
    assert "VALUES(`P08254|MMP3`)" in sql


# --- Special characters in VALUES (preserved via parameterised binding) ----

@pytest.mark.parametrize("value,payload", [
    ('He said "hi"', 's\n"He said ""hi"""\n'),   # escaped embedded quotes
    ("C:\\new",      "s\nC:\\new\n"),             # backslash (Windows path)
    ("line1\nline2", 's\n"line1\nline2"\n'),      # embedded newline in a quoted field
    ("a;b;c",        "s\na;b;c\n"),               # semicolons (not the delimiter)
    ("🔥hot",        "s\n🔥hot\n"),               # emoji
])
def test_special_char_value_preserved(value, payload):
    rows = _ingest({"s": "VARCHAR(40)"}, payload)
    assert rows[0]["s"] == value


# --- Size / overflow -------------------------------------------------------

def test_bigint_overflow_is_clear_error():
    # A value beyond Int64 range must fail clearly, not wrap or corrupt.
    with pytest.raises(Exception):
        _ingest({"n": "BIGINT"}, "n\n99999999999999999999\n")


def test_float_overflow_is_clear_error():
    with pytest.raises(Exception):
        _ingest({"x": "FLOAT"}, "x\n1e400\n")


def test_value_exceeding_varchar_length_rejected():
    # A cell longer than the declared VARCHAR(n) is caught by DataValidator (now
    # a full-column scan) with an actionable "exceeding max length" error, rather
    # than silently truncating at MySQL write time.
    res = DataValidator(schema={"s": "VARCHAR(10)"}).validate(
        pd.DataFrame({"s": ["x" * 5000]})
    )
    assert not res.is_valid and "max length" in res.errors[0]


def test_mixed_type_column_is_clear_error():
    # A non-numeric mixed into an INT column fails with a clear per-column error,
    # not a silent coerce or a cryptic crash.
    with pytest.raises(Exception):
        _ingest({"n": "INT"}, "n\n1\n2\nabc\n4\n")


# --- Structure: line endings -----------------------------------------------

def test_cr_only_line_endings_parse():
    # Classic-Mac CR-only line endings must parse, not collapse to a single row.
    rows = _ingest({"a": "INT", "b": "INT"}, "a,b\r1,2\r3,4\r")
    assert len(rows) == 2 and rows[1]["a"] == "3"
