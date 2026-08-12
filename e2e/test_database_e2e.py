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


def test_get_table_schema_reports_real_mysql_types(db, table):
    """The schema sent to the backend must carry the REAL column types.

    Reflection against a live MySQL returns dialect type classes (INTEGER,
    FLOAT, DATETIME, ...). A mapping keyed by generic SQLAlchemy class names
    (Integer, Float, ...) matched none of them, so every non-VARCHAR column
    was reported to the backend as VARCHAR. The mocked unit test couldn't
    catch that — it fed generic types into a fake inspector. This is the
    test shape that does: declared type in, real CREATE TABLE, real
    reflection out."""
    db.create_table(table, {
        "f_int": "INT",
        "f_float": "FLOAT",
        "f_dec": "DECIMAL(10,2)",
        "f_bool": "BOOLEAN",
        "f_dt": "DATETIME",
        "f_name": "VARCHAR(64)",
    })
    schema = db.get_table_schema(table)
    assert schema["f_int"] == "INT"
    assert schema["f_float"] == "FLOAT"
    assert schema["f_dec"] == "DECIMAL(10,2)"
    assert schema["f_bool"] == "BOOLEAN"  # MySQL stores BOOL as TINYINT(1)
    assert schema["f_dt"] == "DATETIME"
    assert schema["f_name"] == "VARCHAR(64)"
    # The framework's standard columns get their real types too.
    assert schema["id"] == "BIGINT"
    assert schema["created_at"] == "DATETIME"
    assert schema["annotation"] == "TEXT"


def test_content_hash_retry_reclaims_rows_instead_of_duplicating(db, table):
    """#225 end-to-end against real MySQL: a retried run inserting the SAME
    content under deterministic ids re-claims the prior attempt's rows via
    the data_id UNIQUE upsert — row count stays constant and ownership moves
    to the retry's ingestor_id (whose scoped label counts then match)."""
    from tracebloc_ingestor.ingestors.record_processor import RecordProcessor

    db.create_table(table, {"feature": "FLOAT"})
    salt = db.get_or_create_table_salt(table)
    assert db.get_or_create_table_salt(table) == salt  # stable across calls

    def run(ingestor_id):
        rp = RecordProcessor(
            schema={"feature": "FLOAT"},
            intent="train",
            label_column="target",
            annotation_column=None,
            unique_id_column=None,
            ingestor_id=ingestor_id,
            data_id_strategy="content_hash",
            table_salt=salt,
        )
        records = []
        for i, label in [(1, "cat"), (2, "dog")]:
            rec = rp.process({"feature": float(i), "target": label})
            rec["ingestor_id"] = ingestor_id
            records.append(rec)
        db.insert_batch(table, records)

    run("attempt-1")               # first attempt: rows land
    assert _query(f"SELECT COUNT(*) FROM `{table}`")[0][0] == 2

    run("attempt-2-retry")         # k8s Job retry: SAME content, new run id
    assert _query(f"SELECT COUNT(*) FROM `{table}`")[0][0] == 2  # no duplication
    owners = {r[0] for r in _query(f"SELECT ingestor_id FROM `{table}`")}
    assert owners == {"attempt-2-retry"}  # re-claimed by the retry
    # the retry's scoped label counts see the full dataset
    assert db.get_label_counts(table, "attempt-2-retry") == {"cat": 1, "dog": 1}


def test_table_salts_are_per_table(db, table):
    """Identical content in different tables must be unlinkable."""
    other = table + "_b"
    try:
        s1 = db.get_or_create_table_salt(table)
        s2 = db.get_or_create_table_salt(other)
        assert s1 != s2
        assert len(s1) == 64
    finally:
        _query(f"DROP TABLE IF EXISTS `{other}`")


def test_delete_by_ingestor_id_removes_only_that_run(db, table):
    """#227 compensating delete: removes exactly one run's rows, leaves the
    other run's rows untouched, and reports the deleted count."""
    db.create_table(table, {"feature": "FLOAT"})
    db.insert_batch(
        table,
        [
            _rec("a", feature=1.0, ingestor_id="run-failed"),
            _rec("b", feature=2.0, ingestor_id="run-failed"),
            _rec("c", feature=3.0, ingestor_id="run-registered"),
        ],
    )
    assert _query(f"SELECT COUNT(*) FROM `{table}`")[0][0] == 3

    deleted = db.delete_by_ingestor_id(table, "run-failed")

    assert deleted == 2
    rows = _query(f"SELECT data_id, ingestor_id FROM `{table}`")
    assert rows == [("c", "run-registered")]
    # idempotent: a second delete is a harmless no-op
    assert db.delete_by_ingestor_id(table, "run-failed") == 0


def test_blob_columns_roundtrip_from_string_cells(db, table):
    """BLOB/LONGBLOB columns accept str cells (CSV/JSON deliver strings) —
    encoded to bytes at insert (Bugbot on #330: the blob example could never
    ingest; SQLAlchemy raised StatementError(TypeError) on every row)."""
    db.create_table(table, {"payload": "LONGBLOB", "thumb": "BLOB"})
    ids, failures = db.insert_batch(
        table,
        [_rec("a", payload="JVBERi0xLjQ=", thumb="iVBOR")],
    )
    assert failures == []
    rows = _query(f"SELECT payload, thumb FROM `{table}`")
    assert rows[0][0] == b"JVBERi0xLjQ=" and rows[0][1] == b"iVBOR"


# ── Run journal: orphan-row reconciliation (backend#1028 item 2) ─────────────
# A hard kill (OOMKilled / SIGKILL) bypasses the #227 compensating delete, so
# the dead run's rows survive while its dataset was never registered — and the
# Job retry then duplicates them. The run journal + reclaim pass below is the
# fix; these run it against a real MySQL (real DDL, INSERT IGNORE, JOIN).


def _run_id(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def test_hard_killed_run_rows_reclaimed_on_retry(db, table):
    """The bug scenario end-to-end: attempt 1 journals its start, inserts
    rows, and dies hard (no registration, no compensating delete). The
    retry's reconcile pass removes exactly those rows, so its own insert
    (fresh uuid data_ids) converges to N rows instead of 2N."""
    dead, retry = _run_id("dead"), _run_id("retry")
    db.create_table(table, {"feature": "FLOAT"})

    # Attempt 1 — the same calls the engine makes, cut off mid-ingest:
    db.record_ingest_started(table, dead)
    db.insert_batch(
        table,
        [
            _rec("a1", feature=1.0, ingestor_id=dead),
            _rec("a2", feature=2.0, ingestor_id=dead),
        ],
    )
    # -- hard kill here: no send_ingest_summary, no #227 delete --
    assert _query(f"SELECT COUNT(*) FROM `{table}`")[0][0] == 2

    # Attempt 2 (the Job retry), in engine order:
    reclaimed = db.reclaim_dead_run_rows(table, retry)
    assert reclaimed == {dead: 2}
    db.record_ingest_started(table, retry)
    db.insert_batch(
        table,
        [
            _rec("b1", feature=1.0, ingestor_id=retry),
            _rec("b2", feature=2.0, ingestor_id=retry),
        ],
    )
    db.mark_ingest_registered(table, retry)

    assert _query(f"SELECT COUNT(*) FROM `{table}`")[0][0] == 2  # converged
    owners = {r[0] for r in _query(f"SELECT DISTINCT ingestor_id FROM `{table}`")}
    assert owners == {retry}
    assert db.get_label_counts(table, retry) == {"": 2}


def test_reclaim_never_touches_registered_or_legacy_rows(db, table):
    """Safety contract: rows of a REGISTERED run (journaled start + mark) and
    legacy rows that predate the journal entirely (no entry at all — every
    pre-fix dataset, registered or not) must both survive the reclaim pass
    untouched."""
    registered, legacy = _run_id("registered"), _run_id("legacy")
    db.create_table(table, {"feature": "FLOAT"})

    db.record_ingest_started(table, registered)
    db.insert_batch(table, [_rec("r1", feature=1.0, ingestor_id=registered)])
    db.mark_ingest_registered(table, registered)

    # Legacy run: rows only, no journal entry (ingested before the fix).
    db.insert_batch(table, [_rec("l1", feature=2.0, ingestor_id=legacy)])

    assert db.reclaim_dead_run_rows(table, _run_id("current")) == {}
    assert _query(f"SELECT COUNT(*) FROM `{table}`")[0][0] == 2


def test_reclaim_is_idempotent_and_excludes_current_run(db, table):
    """Running the reclaim pass twice is safe (second pass finds nothing),
    and the current run's own journaled-but-unregistered rows are never
    touched — so the pass may re-run at any point of a live ingest."""
    dead, current = _run_id("dead"), _run_id("current")
    db.create_table(table, {"feature": "FLOAT"})

    db.record_ingest_started(table, dead)
    db.insert_batch(table, [_rec("d1", feature=1.0, ingestor_id=dead)])

    db.record_ingest_started(table, current)
    db.insert_batch(table, [_rec("c1", feature=2.0, ingestor_id=current)])

    assert db.reclaim_dead_run_rows(table, current) == {dead: 1}
    assert db.reclaim_dead_run_rows(table, current) == {}  # idempotent
    rows = _query(f"SELECT data_id, ingestor_id FROM `{table}`")
    assert rows == [("c1", current)]
