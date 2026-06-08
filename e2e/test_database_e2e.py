"""End-to-end DATABASE behaviour against a real MySQL.

The unit suite (``tests/test_database.py``) mocks the SQLAlchemy engine, so no
SQL is ever executed despite 100% line coverage: ``CREATE TABLE``, the
``ON DUPLICATE KEY UPDATE`` upsert, the bulk-insert -> per-row fallback, type
mapping and charset round-tripping are never actually run. These tests run them
for real, closing the gap between "every line executed" and "the DB behaves".

Skipped unless a MySQL is reachable (see ``conftest.py``); CI runs it with a
MySQL service in ``.github/workflows/e2e.yml``.
"""
import os
import uuid

import mysql.connector
import pytest

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.database import Database


def _query(sql):
    conn = mysql.connector.connect(
        host=os.environ["MYSQL_HOST"], port=int(os.environ["MYSQL_PORT"]),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@pytest.fixture
def db():
    return Database(Config())


@pytest.fixture
def table(db):
    """A uniquely-named table, dropped on teardown."""
    name = "e2e_db_" + uuid.uuid4().hex[:8]
    yield name
    _query(f"DROP TABLE IF EXISTS `{name}`")


def _rec(data_id, **cols):
    return {"data_id": data_id, **cols}


def test_create_table_and_insert_roundtrip(db, table):
    """CREATE TABLE + INSERT actually run, and values come back intact."""
    db.create_table(table, {"feature": "FLOAT"})
    ids, failures = db.insert_batch(table, [_rec("a", feature=1.5), _rec("b", feature=2.5)])
    assert failures == []
    assert _query(f"SELECT COUNT(*) FROM `{table}`")[0][0] == 2
    vals = sorted(r[0] for r in _query(f"SELECT feature FROM `{table}`"))
    assert vals == [1.5, 2.5]


def test_upsert_dedupes_on_data_id(db, table):
    """Re-ingesting the same data_id UPDATEs the row (ON DUPLICATE KEY UPDATE),
    it does not create a duplicate — the core idempotency guarantee."""
    db.create_table(table, {"feature": "INT"})
    db.insert_batch(table, [_rec("dup", feature=1)])
    db.insert_batch(table, [_rec("dup", feature=99)])
    assert _query(f"SELECT COUNT(*) FROM `{table}`")[0][0] == 1
    assert _query(f"SELECT feature FROM `{table}` WHERE data_id='dup'")[0][0] == 99


def test_partial_batch_falls_back_without_duplicating_good_rows(db, table):
    """data_id is NOT NULL UNIQUE; a NULL row aborts the bulk INSERT, so the
    per-row fallback must still insert the good rows exactly once (no dupes)."""
    db.create_table(table, {"feature": "INT"})
    batch = [_rec("ok1", feature=1), _rec(None, feature=2), _rec("ok2", feature=3)]
    ids, failures = db.insert_batch(table, batch)
    assert _query(f"SELECT COUNT(*) FROM `{table}`")[0][0] == 2  # ok1 + ok2, once each
    assert len(failures) == 1                                    # the NULL-data_id row


def test_non_ascii_data_roundtrip(db, table):
    """Non-ASCII values (German umlauts) survive the real INSERT/SELECT."""
    db.create_table(table, {"name": "VARCHAR(64)"})
    db.insert_batch(table, [_rec("u", name="Größe-Meßwert")])
    assert _query(f"SELECT name FROM `{table}` WHERE data_id='u'")[0][0] == "Größe-Meßwert"
