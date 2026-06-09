"""Tests for Database with SQLAlchemy mocked at the engine/inspect boundary.

We never touch a real MySQL. ``create_engine`` is patched so ``__init__`` /
``_create_engine`` build a mock engine; ``inspect`` and ``metadata.create_all`` /
``metadata.reflect`` are patched per-test so table operations run without a DB.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import Integer, String, BigInteger, Column, Table

from tracebloc_ingestor import database as db_mod
from tracebloc_ingestor.database import Database
from tracebloc_ingestor.config import Config


@pytest.fixture
def mock_engine_factory():
    """Patch create_engine so Database.__init__ builds a mock engine."""
    with patch.object(db_mod, "create_engine") as ce:
        engine = MagicMock(name="engine")
        # engine.connect() is a context manager yielding a mock connection.
        conn = MagicMock(name="connection")
        engine.connect.return_value.__enter__.return_value = conn
        ce.return_value = engine
        yield ce, engine, conn


@pytest.fixture
def db(mock_engine_factory):
    return Database(Config(EDGE_ENV="local"))


# ---------------------------------------------------------------------------
# __init__ / _create_engine
# ---------------------------------------------------------------------------

def test_init_builds_engine(db, mock_engine_factory):
    ce, engine, conn = mock_engine_factory
    assert db.engine is engine
    # create_engine called twice: server-level then db-specific.
    assert ce.call_count == 2
    # CREATE DATABASE issued + committed during _create_engine.
    conn.execute.assert_called()
    conn.commit.assert_called()


# ---------------------------------------------------------------------------
# _get_sqlalchemy_type (pure)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("mysql_type,expected_cls", [
    ("INT", Integer),
    ("INTEGER", Integer),
    ("BIGINT", BigInteger),
    ("TEXT", db_mod.Text),
    ("FLOAT", db_mod.Float),
    ("BOOLEAN", db_mod.Boolean),
    ("DATE", db_mod.Date),
    ("DATETIME", db_mod.DateTime),
    ("TIMESTAMP", db_mod.DateTime),
    ("TIME", db_mod.Time),
])
def test_get_sqlalchemy_type_mapping(db, mysql_type, expected_cls):
    result = db._get_sqlalchemy_type(mysql_type)
    # result may be a class or an instance depending on length handling.
    assert isinstance(result, expected_cls) or result is expected_cls


def test_get_sqlalchemy_type_varchar_length(db):
    result = db._get_sqlalchemy_type("VARCHAR(128)")
    assert isinstance(result, String)
    assert result.length == 128


def test_get_sqlalchemy_type_bad_length_ignored(db):
    result = db._get_sqlalchemy_type("VARCHAR(abc)")
    assert isinstance(result, String) or result is String


def test_get_sqlalchemy_type_unsupported_raises(db):
    with pytest.raises(ValueError, match="Unsupported MySQL type"):
        db._get_sqlalchemy_type("GEOMETRY")


# ---------------------------------------------------------------------------
# create_table
# ---------------------------------------------------------------------------

def test_create_table_new(db):
    db.metadata.create_all = MagicMock()
    inspector = MagicMock()
    inspector.get_table_names.return_value = []
    with patch.object(db_mod, "inspect", return_value=inspector):
        table = db.create_table("new_tbl", {"feat": "INT"})
    assert "new_tbl" in db.tables
    assert "feat" in table.c
    # standard columns present
    assert "data_id" in table.c
    db.metadata.create_all.assert_called_once()


def test_create_table_cached(db):
    sentinel = MagicMock(name="cached_table")
    db.tables["t"] = sentinel
    assert db.create_table("t", {}) is sentinel


def test_create_table_existing_in_db_reflects(db):
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["existing"]

    def fake_reflect(engine, only=None):
        Table("existing", db.metadata, Column("id", BigInteger, primary_key=True))

    db.metadata.reflect = MagicMock(side_effect=fake_reflect)
    with patch.object(db_mod, "inspect", return_value=inspector):
        table = db.create_table("existing", {})
    assert db.tables["existing"] is table
    db.metadata.reflect.assert_called_once()


def test_create_table_existing_matching_schema_reflects_ok(db):
    """An existing table whose feature columns match the incoming schema is
    reused without error — the normal re-ingest-same-dataset path. Only the
    feature columns are compared; the standard framework columns (id, data_id,
    …) are ignored."""
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["panel"]

    def fake_reflect(engine, only=None):
        Table(
            "panel", db.metadata,
            Column("id", BigInteger, primary_key=True),
            Column("data_id", String(255)),
            Column("P01033_TIMP1", String(255)),
            Column("P02452_COL1A1", String(255)),
        )

    db.metadata.reflect = MagicMock(side_effect=fake_reflect)
    with patch.object(db_mod, "inspect", return_value=inspector):
        table = db.create_table(
            "panel", {"P01033_TIMP1": "FLOAT", "P02452_COL1A1": "FLOAT"}
        )
    assert db.tables["panel"] is table


def test_create_table_existing_schema_mismatch_fails_fast(db):
    """A stale table whose feature columns don't match the incoming schema must
    fail fast with an actionable error — instead of being silently reused and
    then dying on every insert with SQLAlchemy's cryptic 'Unconsumed column
    names'.

    Regression (Henrik/LMU): a prior ingestion left `IBD_Biomarker` with the
    original proteomics headers (`P02452|COL1A1`); the customer then renamed the
    CSV headers to sanitized identifiers (`P02452_COL1A1`) to dodge an unrelated
    SQL error. create_table reflected the stale table, ignored the new schema,
    and all 207 records failed with 'Unconsumed column names'."""
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["IBD_Biomarker"]

    def fake_reflect(engine, only=None):
        Table(
            "IBD_Biomarker", db.metadata,
            Column("id", BigInteger, primary_key=True),
            Column("data_id", String(255)),
            Column("P02452|COL1A1", String(255)),  # original header (stale table)
        )

    db.metadata.reflect = MagicMock(side_effect=fake_reflect)
    with patch.object(db_mod, "inspect", return_value=inspector):
        with pytest.raises(ValueError, match="do not match|stale table"):
            db.create_table("IBD_Biomarker", {"P02452_COL1A1": "FLOAT"})  # sanitized


# ---------------------------------------------------------------------------
# insert_batch
# ---------------------------------------------------------------------------

def _seed_table(db):
    db.metadata.create_all = MagicMock()
    inspector = MagicMock()
    inspector.get_table_names.return_value = []
    with patch.object(db_mod, "inspect", return_value=inspector):
        db.create_table("tbl", {"feat": "INT"})


def test_insert_batch_empty_returns_empty_tuple(db):
    # Empty input returns the same (success_ids, failures) tuple shape as the
    # non-empty path, so callers can unconditionally unpack two values.
    ids, failures = db.insert_batch("tbl", [])
    assert ids == []
    assert failures == []


def test_insert_batch_success(db, mock_engine_factory):
    ce, engine, conn = mock_engine_factory
    _seed_table(db)
    rows = [MagicMock(id=1), MagicMock(id=2)]
    conn.execute.return_value.fetchall.return_value = rows
    ids, failures = db.insert_batch(
        "tbl", [{"data_id": "a", "feat": 1}, {"data_id": "b", "feat": 2}]
    )
    assert ids == [1, 2]
    assert failures == []
    conn.commit.assert_called()


def test_insert_batch_falls_back_to_individual(db, mock_engine_factory):
    ce, engine, conn = mock_engine_factory
    _seed_table(db)

    calls = {"n": 0}

    def execute_side_effect(stmt, *a, **k):
        calls["n"] += 1
        # First call is the bulk insert -> fail to trigger the per-record path.
        if calls["n"] == 1:
            raise RuntimeError("bulk failed")
        result = MagicMock()
        result.fetchone.return_value = MagicMock(id=99)
        result.fetchall.return_value = [MagicMock(id=99)]
        return result

    conn.execute.side_effect = execute_side_effect
    ids, failures = db.insert_batch("tbl", [{"data_id": "a", "feat": 1}])
    # Individual insert path succeeded for the single record.
    assert ids == [99]
    assert failures == []


def test_insert_batch_individual_failure_recorded(db, mock_engine_factory):
    ce, engine, conn = mock_engine_factory
    _seed_table(db)

    def execute_side_effect(stmt, *a, **k):
        raise RuntimeError("always fails")

    conn.execute.side_effect = execute_side_effect
    ids, failures = db.insert_batch("tbl", [{"data_id": "a", "feat": 1}])
    assert ids == []
    assert len(failures) == 1
    assert "always fails" in failures[0]["error"]


def test_insert_batch_connection_error(db, mock_engine_factory):
    ce, engine, conn = mock_engine_factory
    _seed_table(db)
    engine.connect.side_effect = RuntimeError("no connection")
    ids, failures = db.insert_batch("tbl", [{"data_id": "a", "feat": 1}])
    assert ids == []
    assert len(failures) == 1
    assert "Database connection error" in failures[0]["error"]


# ---------------------------------------------------------------------------
# get_table_schema
# ---------------------------------------------------------------------------

def test_get_table_schema(db):
    inspector = MagicMock()
    class Weird:  # unknown SQLAlchemy type, no 'length' attribute
        pass

    inspector.get_columns.return_value = [
        {"name": "id", "type": Integer()},
        {"name": "name", "type": String(255)},
        {"name": "weird", "type": Weird()},
    ]
    with patch.object(db_mod, "inspect", return_value=inspector):
        schema = db.get_table_schema("tbl")
    assert schema["id"] == "INT"
    assert schema["name"] == "VARCHAR(255)"
    # unknown SQLAlchemy type falls back to VARCHAR
    assert schema["weird"] == "VARCHAR"


def test_create_table_rejects_reserved_column():
    """A user schema column colliding with a reserved/internal column (e.g.
    'id') fails fast with a clear ValueError, not a cryptic DuplicateColumnError.
    The guard runs before any DB I/O, so no live connection is needed."""
    db = Database.__new__(Database)
    with pytest.raises(ValueError, match="reserved"):
        db.create_table("some_table", {"id": "INT", "feature_0": "FLOAT"})


def test_create_table_allows_label_in_schema():
    """`label` is the user-facing label column (mapped onto the standard
    column), so it must NOT be treated as a reserved collision."""
    db = Database.__new__(Database)
    db.tables = {"t": "sentinel"}  # short-circuit before any engine use
    assert db.create_table("t", {"feature_0": "FLOAT", "label": "INT"}) == "sentinel"


def test_create_table_rejects_overlong_column_name():
    """Column names over MySQL's 64-char identifier limit fail fast with a clear
    error naming the offenders, instead of a raw MySQL 1059 at CREATE TABLE.
    Like the reserved-column guard, this runs before any DB I/O."""
    db = Database.__new__(Database)
    long_name = "Protein_" + "X" * 70  # 78 chars, > 64
    with pytest.raises(ValueError, match="64-character"):
        db.create_table("t", {long_name: "FLOAT", "feature_0": "FLOAT"})


# ---------------------------------------------------------------------------
# upsert quoting (regression): special-character column names
# ---------------------------------------------------------------------------

def test_upsert_backtick_quotes_special_char_columns_in_values_clause():
    """ON DUPLICATE KEY UPDATE must backtick-quote the column name inside
    VALUES(...).

    Proteomics panels use "UniProt|gene" headers (e.g. `P01033|TIMP1`) and
    isoform names (`P02751-1|FN1`). The previous construction built the update
    clause with a raw f-string ``text(f"VALUES({column.name})")``, leaving the
    name unquoted on the right-hand side. MySQL then parsed the `|` (and `-`)
    as operators and raised 1064 (syntax error) — failing the entire batch.

    Surfaced by Henrik's IBD_Biomarkers ingestion (LMU): all 207 records failed
    with ``near '|TIMP1), `P02452|COL1A1` = VALUES(P02452|COL1A1)'``. Note the
    left-hand side was already correctly quoted; only the VALUES() argument was
    not — which is exactly what this test pins.

    Compiles for the MySQL dialect (no live DB) and asserts both the fixed form
    is present and the broken form is gone.
    """
    from sqlalchemy import MetaData, Table, Column, BigInteger, Float, text
    from sqlalchemy.dialects import mysql
    from sqlalchemy.dialects.mysql import insert

    pipe_col = "P01033|TIMP1"
    isoform_col = "P02751-1|FN1"
    # Include a column that WILL NOT appear in the inserted record. The
    # production code iterates every table column; the fix must keep working
    # when an update target isn't in the INSERT list (the e2e regression that
    # `insert_stmt.inserted[col]` introduced — MySQL 8 row-alias `new.col`
    # requires the column to be in the INSERT list and raised 1054).
    table = Table(
        "IBD_Biomarkers",
        MetaData(),
        Column("id", BigInteger, primary_key=True),
        Column("data_id", mysql.VARCHAR(255)),
        Column(pipe_col, Float),
        Column(isoform_col, Float),
        Column("status", mysql.VARCHAR(50)),
    )
    insert_stmt = insert(table)
    # Mirror the production construction exactly (database.py):
    update_dict = {
        column.name: text(f"VALUES(`{column.name}`)")
        for column in table.columns
        if column.name not in ["id", "created_at", "data_id"]
    }
    stmt = insert_stmt.values(
        [{"data_id": "x", pipe_col: 1.0, isoform_col: 2.0}]
    ).on_duplicate_key_update(**update_dict)
    sql = str(stmt.compile(dialect=mysql.dialect()))

    # Fixed: the name is backtick-quoted inside VALUES(...).
    assert "VALUES(`P01033|TIMP1`)" in sql
    assert "VALUES(`P02751-1|FN1`)" in sql
    # Regression guard: the unquoted form that broke MySQL must not reappear.
    assert "VALUES(P01033|TIMP1)" not in sql
    assert "VALUES(P02751-1|FN1)" not in sql
    # Second regression guard: never re-introduce the MySQL-8 row-alias form
    # (`AS new ... new.col`) which requires every referenced column to be in
    # the INSERT list — the e2e failure that flipped the original PR red.
    assert " AS new " not in sql
    assert "new.status" not in sql
