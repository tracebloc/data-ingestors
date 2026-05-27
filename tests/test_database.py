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


# ---------------------------------------------------------------------------
# insert_batch
# ---------------------------------------------------------------------------

def _seed_table(db):
    db.metadata.create_all = MagicMock()
    inspector = MagicMock()
    inspector.get_table_names.return_value = []
    with patch.object(db_mod, "inspect", return_value=inspector):
        db.create_table("tbl", {"feat": "INT"})


def test_insert_batch_empty_returns_dict(db):
    result = db.insert_batch("tbl", [])
    assert result == {"success_ids": [], "failures": []}


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
