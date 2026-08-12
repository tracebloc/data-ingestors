"""Tests for Database with SQLAlchemy mocked at the engine/inspect boundary.

We never touch a real MySQL. ``create_engine`` is patched so ``__init__`` /
``_create_engine`` build a mock engine; ``inspect`` and ``metadata.create_all`` /
``metadata.reflect`` are patched per-test so table operations run without a DB.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    Numeric,
    String,
    Table,
)
from sqlalchemy.dialects import mysql

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
    # DB_USER/DB_PASSWORD are required (backend#1528 removed the edgeuser
    # fallback); the engine is mocked so the values are arbitrary.
    return Database(Config(EDGE_ENV="local", DB_USER="tb_ingest", DB_PASSWORD="pw"))


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


@pytest.mark.parametrize(
    "mysql_type,expected_cls",
    [
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
    ],
)
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


def test_get_sqlalchemy_type_typo_suggests_correction(db):
    """A close-typo unknown type must surface a 'Did you mean X?' hint.

    A new user typing ``BIGINTEGER`` (mixing up MySQL's ``BIGINT`` with
    Python's ``int`` keyword) used to get a bare ``Unsupported MySQL
    type: BIGINTEGER`` — correct but unhelpful. With the suggestion
    layer the error guides them toward a related supported type and
    lists the full vocabulary alongside, turning a 'wait, what's the
    right spelling?' round-trip into a zero-thought fix. Surfaced by
    adversarial new-user testing N3 (parent #261).

    ``BIGINTEGER``'s nearest by Levenshtein distance is ``INTEGER``
    (d=3 — drop the BIG prefix); ``BIGINT`` is d=4 (drop the EGER
    suffix). Both are valid supported types, INTEGER wins by a single
    edit. The full supported-types list (which the error also prints)
    surfaces ``BIGINT`` for the user who actually wanted the 64-bit
    range.
    """
    with pytest.raises(ValueError, match="Did you mean 'INTEGER'") as excinfo:
        db._get_sqlalchemy_type("BIGINTEGER")
    # The full supported-types listing must also be present so the user
    # discovers BIGINT (the better fit by intent) even though the
    # suggestion is INTEGER by edit distance.
    assert "Supported types" in str(excinfo.value)
    assert "BIGINT" in str(excinfo.value)


def test_get_sqlalchemy_type_typo_no_suggestion_for_distant_input(db):
    """A type name that's NOT close to any supported entry must NOT
    misleadingly suggest one — ``GEOMETRY`` is a real MySQL type but
    semantically unrelated to anything we map; suggesting ``DATETIME``
    for it would be worse than no suggestion."""
    with pytest.raises(ValueError, match="Unsupported MySQL type") as excinfo:
        db._get_sqlalchemy_type("GEOMETRY")
    assert "Did you mean" not in str(excinfo.value)


@pytest.mark.parametrize(
    "typo,suggestion",
    [
        ("INTGER", "INTEGER"),
        ("NUMRIC", "NUMERIC"),
        ("BOLEAN", "BOOLEAN"),
        ("VARCAHR", "VARCHAR"),
    ],
)
def test_get_sqlalchemy_type_typo_suggestions_cover_common_mistakes(
    db, typo, suggestion
):
    """Several real-world typos a new user might make. Each is within
    edit distance 2 of the suggested correction and far enough from
    other candidates that the suggestion is unambiguous."""
    with pytest.raises(ValueError, match=f"Did you mean '{suggestion}'"):
        db._get_sqlalchemy_type(typo)


def test_get_sqlalchemy_type_decimal_precision_scale(db):
    # Regression (#190 bugbot): DECIMAL(10,2) used to fail int("10,2") and
    # fall back to a bare Numeric() — declared precision and scale silently
    # dropped, MySQL then used its default and clipped values.
    result = db._get_sqlalchemy_type("DECIMAL(10,2)")
    assert isinstance(result, db_mod.Numeric)
    assert result.precision == 10
    assert result.scale == 2


def test_get_sqlalchemy_type_numeric_precision_scale(db):
    # NUMERIC is an alias for DECIMAL; same precision/scale handling.
    result = db._get_sqlalchemy_type("NUMERIC(8, 3)")
    assert isinstance(result, db_mod.Numeric)
    assert result.precision == 8
    assert result.scale == 3


def test_get_sqlalchemy_type_char_with_length(db):
    """Regression: CHAR(N) was absent from `type_mapping`. The DataValidator
    accepts CHAR(N) (see `_validate_char` in data_validator.py), so a user
    could declare e.g. `code: CHAR(1)` in their schema, pass preflight, then
    hit `ValueError: Unsupported MySQL type: CHAR(1)` at table creation —
    the validator and DDL layers had divergent vocabularies. CHAR is a
    valid MySQL type (fixed-width, padded), distinct from VARCHAR but same
    SQLAlchemy semantics; add it to the mapping so the two layers agree.

    Surfaced by an adversarial 'all-types' tabular schema run against
    v0.3.10-rc1: a CSV declaring `code: CHAR(1)` failed at table creation
    even though every column type the schema lists is documented as
    supported.
    """
    result = db._get_sqlalchemy_type("CHAR(1)")
    assert isinstance(result, db_mod.CHAR)
    assert result.length == 1


def test_get_sqlalchemy_type_char_bare(db):
    """Bare ``CHAR`` (no length) is still valid SQL — MySQL defaults to
    CHAR(1). Don't raise; map to the SQLAlchemy class so the column gets
    created with the dialect default."""
    result = db._get_sqlalchemy_type("CHAR")
    # May be the class or an instance — either is acceptable as long as
    # the dialect picks up the right default at DDL time.
    assert isinstance(result, db_mod.CHAR) or result is db_mod.CHAR


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
            "panel",
            db.metadata,
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
            "IBD_Biomarker",
            db.metadata,
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
    # #226: the raw driver message can embed cell values — the stored
    # error carries the exception class, never the message.
    assert "RuntimeError" in failures[0]["error"]
    assert "always fails" not in failures[0]["error"]


def test_insert_batch_connection_error(db, mock_engine_factory):
    """A failure of ``engine.connect()`` itself (different from a per-
    statement error inside the connection) is caught at the outer ``try``
    and reported as a 'Database connection error' failure. Tenacity's
    retry covers transient per-statement errors; the connect-itself
    path is separate (the engine has its own pool / pre-ping retry).
    """
    ce, engine, conn = mock_engine_factory
    _seed_table(db)
    engine.connect.side_effect = RuntimeError("no connection")
    ids, failures = db.insert_batch("tbl", [{"data_id": "a", "feat": 1}])
    assert ids == []
    # #219 bugbot: restore the assertions that pin the outer-connect
    # error path (one failure entry, message mentions the connection).
    assert len(failures) == 1
    assert "Database connection error" in failures[0]["error"]


# ---------------------------------------------------------------------------
# Transient DB retry via tenacity — backend/#772 P2
# ---------------------------------------------------------------------------


def test_insert_batch_retries_on_transient_operational_error(db, mock_engine_factory):
    """A transient MySQL hiccup (server-gone-away, lost connection,
    deadlock) used to fail every in-flight batch permanently. tenacity
    now retries up to 3 attempts with exponential backoff; the second
    attempt succeeds in this test."""
    from sqlalchemy.exc import OperationalError

    ce, engine, conn = mock_engine_factory
    _seed_table(db)

    calls = {"n": 0}

    def execute_side_effect(stmt, *a, **k):
        calls["n"] += 1
        # First execute (the bulk insert) raises a transient error once,
        # then succeeds on attempt #2. Subsequent execute calls (the
        # SELECT for ids) succeed.
        if calls["n"] == 1:
            raise OperationalError(
                "INSERT …", {}, Exception("MySQL server has gone away")
            )
        result = MagicMock()
        result.fetchall.return_value = [MagicMock(id=42)]
        return result

    conn.execute.side_effect = execute_side_effect
    ids, failures = db.insert_batch("tbl", [{"data_id": "a", "feat": 1}])
    # Retry kicked in: the bulk insert was attempted twice (fail, succeed),
    # then the id SELECT ran once.
    assert calls["n"] >= 2
    assert ids == [42]
    assert failures == []


def test_insert_batch_does_not_retry_permanent_error_falls_to_per_row(
    db, mock_engine_factory
):
    """Permanent errors (IntegrityError, DataError, …) must NOT be
    retried — they reflect bad data, not a transient blip. They fall
    straight through to the existing per-row fallback path so the
    offending record can be identified."""
    from sqlalchemy.exc import IntegrityError

    ce, engine, conn = mock_engine_factory
    _seed_table(db)

    calls = {"bulk": 0, "row": 0}

    def execute_side_effect(stmt, *a, **k):
        # The very first execute is the bulk insert: raise a permanent
        # error. Subsequent executes (per-row inserts + SELECTs) succeed.
        compiled = str(stmt)
        if "INSERT" in compiled.upper() and calls["bulk"] == 0:
            calls["bulk"] += 1
            raise IntegrityError("INSERT …", {}, Exception("dup key"))
        calls["row"] += 1
        result = MagicMock()
        result.fetchone.return_value = MagicMock(id=7)
        return result

    conn.execute.side_effect = execute_side_effect
    ids, failures = db.insert_batch("tbl", [{"data_id": "a", "feat": 1}])
    # Bulk insert tried once (no retry on permanent error), then the
    # per-record path took over and succeeded for this single row.
    assert calls["bulk"] == 1, "permanent error must not be retried"
    assert ids == [7]


def test_insert_batch_gives_up_after_max_retries(db, mock_engine_factory):
    """If a transient error persists, retry caps at 3 attempts and falls
    to the per-row path (which will see the same error and record the
    failure)."""
    from sqlalchemy.exc import OperationalError

    ce, engine, conn = mock_engine_factory
    _seed_table(db)

    err = OperationalError("INSERT …", {}, Exception("MySQL server has gone away"))

    def execute_side_effect(stmt, *a, **k):
        raise err

    conn.execute.side_effect = execute_side_effect
    ids, failures = db.insert_batch("tbl", [{"data_id": "a", "feat": 1}])
    # All paths fail; the record lands in failures with the underlying error.
    assert ids == []
    assert len(failures) == 1
    # #226: class + errno instead of the raw driver message.
    assert "OperationalError" in failures[0]["error"]
    assert "gone away" not in failures[0]["error"]


def test_insert_batch_rolls_back_between_transient_retries(db, mock_engine_factory):
    """#219 bugbot: SQLAlchemy puts the connection into pending-rollback
    state after a failed statement. Without an intervening rollback(),
    the next attempt raises PendingRollbackError — a non-transient class
    tenacity wouldn't retry — cutting the retries short. The helper must
    rollback BETWEEN attempts so the connection's transactional state
    is reset before the retry runs.
    """
    from sqlalchemy.exc import OperationalError

    ce, engine, conn = mock_engine_factory
    _seed_table(db)

    events = []
    rollback_orig = conn.rollback

    def track_rollback(*a, **k):
        events.append("rollback")
        return rollback_orig(*a, **k)

    conn.rollback.side_effect = track_rollback

    def execute_side_effect(stmt, *a, **k):
        events.append("execute")
        # Fail the bulk insert twice; the third attempt succeeds.
        n = sum(1 for e in events if e == "execute")
        if n <= 2:
            raise OperationalError(
                "INSERT …", {}, Exception("MySQL server has gone away")
            )
        result = MagicMock()
        result.fetchall.return_value = [MagicMock(id=99)]
        return result

    conn.execute.side_effect = execute_side_effect
    ids, failures = db.insert_batch("tbl", [{"data_id": "a", "feat": 1}])
    assert ids == [99]
    # Each transient failure inside _execute_with_retry triggers a rollback
    # before re-raising for the next retry. Two failures -> at least two
    # rollback calls (plus the outer commit-or-rollback path's own).
    rollback_count = sum(1 for e in events if e == "rollback")
    assert (
        rollback_count >= 2
    ), f"expected at least 2 rollbacks between retries; events={events}"


# ---------------------------------------------------------------------------
# get_table_schema
# ---------------------------------------------------------------------------


def test_get_table_schema_reflected_dialect_types(db):
    """Regression: ``inspector.get_columns()`` against a real MySQL returns
    DIALECT type classes (INTEGER, FLOAT, DATETIME, ...), not the generic
    SQLAlchemy ones (Integer, Float, ...). The old mapping was keyed by the
    generic class names, so every non-VARCHAR column fell through to the
    VARCHAR default — the backend was told INT/FLOAT/DATETIME columns were
    VARCHAR. (VARCHAR columns only came out right because the fallback
    happened to be VARCHAR.)"""
    inspector = MagicMock()
    inspector.get_columns.return_value = [
        {"name": "id", "type": mysql.BIGINT(display_width=20)},
        {"name": "f_int", "type": mysql.INTEGER()},
        {"name": "f_float", "type": mysql.FLOAT()},
        {"name": "f_double", "type": mysql.DOUBLE()},
        {"name": "f_dec", "type": mysql.DECIMAL(precision=10, scale=2)},
        {"name": "f_bool", "type": mysql.TINYINT(display_width=1)},  # BOOL
        {"name": "f_tiny", "type": mysql.TINYINT(display_width=4)},
        {"name": "f_vc", "type": mysql.VARCHAR(length=255)},
        {"name": "f_text", "type": mysql.TEXT()},
        {"name": "f_date", "type": mysql.DATE()},
        {"name": "f_dt", "type": mysql.DATETIME()},
        {"name": "f_ts", "type": mysql.TIMESTAMP()},
        {"name": "f_time", "type": mysql.TIME()},
        {"name": "f_blob", "type": mysql.LONGBLOB()},
    ]
    with patch.object(db_mod, "inspect", return_value=inspector):
        schema = db.get_table_schema("tbl")
    assert schema == {
        "id": "BIGINT",
        "f_int": "INT",
        "f_float": "FLOAT",
        "f_double": "DOUBLE",
        "f_dec": "DECIMAL(10,2)",
        "f_bool": "BOOLEAN",  # MySQL reflects BOOL as TINYINT(1)
        "f_tiny": "TINYINT",
        "f_vc": "VARCHAR(255)",
        "f_text": "TEXT",
        "f_date": "DATE",
        "f_dt": "DATETIME",
        "f_ts": "TIMESTAMP",
        "f_time": "TIME",
        "f_blob": "LONGBLOB",
    }


def test_get_table_schema_generic_types(db):
    """Generic (in-process) SQLAlchemy types map to the same MySQL vocabulary
    as their reflected dialect counterparts."""
    inspector = MagicMock()

    class Weird:  # unknown SQLAlchemy type, no 'length' attribute
        pass

    inspector.get_columns.return_value = [
        {"name": "id", "type": Integer()},
        {"name": "big", "type": BigInteger()},
        {"name": "name", "type": String(255)},
        {"name": "bare_str", "type": String()},  # no length declared
        {"name": "ratio", "type": Float()},
        {"name": "price", "type": Numeric(8, 3)},
        {"name": "flag", "type": Boolean()},
        {"name": "seen", "type": DateTime()},
        {"name": "weird", "type": Weird()},
    ]
    with patch.object(db_mod, "inspect", return_value=inspector):
        schema = db.get_table_schema("tbl")
    assert schema["id"] == "INT"
    assert schema["big"] == "BIGINT"
    assert schema["name"] == "VARCHAR(255)"
    assert schema["bare_str"] == "VARCHAR"  # no "VARCHAR(None)"
    assert schema["ratio"] == "FLOAT"
    assert schema["price"] == "DECIMAL(8,3)"
    assert schema["flag"] == "BOOLEAN"
    assert schema["seen"] == "DATETIME"
    # Unknown types pass through as their (upper-cased) class name instead of
    # being mislabelled VARCHAR.
    assert schema["weird"] == "WEIRD"


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


def test_upsert_doubles_embedded_backticks_in_column_name():
    """Identifier-escape regression (#190 bugbot, re-applied).

    A column name containing a literal backtick must be escaped by doubling
    it (` -> ``) — MySQL's identifier-escape rule, same one CREATE TABLE DDL
    already follows. Without that, an embedded backtick closes the quoted
    identifier early and the rest of the name leaks into the SQL as literal
    tokens — the statement is either rejected or silently altered.

    Bugbot flagged that the upsert RHS wraps in backticks but does NOT
    double embedded ones; this test pins the fix. (The original was authored
    in #191 but dropped by GitHub's squash-merge — re-applying.)
    """
    from sqlalchemy import MetaData, Table, Column, BigInteger, Float, text
    from sqlalchemy.dialects import mysql
    from sqlalchemy.dialects.mysql import insert

    weird = "ev`il"
    table = Table(
        "t",
        MetaData(),
        Column("id", BigInteger, primary_key=True),
        Column("data_id", mysql.VARCHAR(255)),
        Column(weird, Float),
    )
    insert_stmt = insert(table)
    update_dict = {
        column.name: text(f"VALUES(`{column.name.replace('`', '``')}`)")
        for column in table.columns
        if column.name not in ["id", "created_at", "data_id"]
    }
    stmt = insert_stmt.values([{"data_id": "x", weird: 1.0}]).on_duplicate_key_update(
        **update_dict
    )
    sql = str(stmt.compile(dialect=mysql.dialect()))

    # Escaped form ` -> `` is present.
    assert "VALUES(`ev``il`)" in sql
    # Unescaped form would close the identifier early — must not appear.
    assert "VALUES(`ev`il`)" not in sql


# ---------------------------------------------------------------------------
# get_samples / get_label_counts: SQL NULL label normalisation
# ---------------------------------------------------------------------------


def test_get_samples_null_label_normalised_to_empty_string(db, mock_engine_factory):
    """SQL NULL labels in sample rows must be normalised to '' so the JSON
    payload matches get_label_counts, which already maps NULL → '' (the same
    convention the old per-row API always sent when no label was present)."""
    _, _, conn = mock_engine_factory
    conn.execute.return_value.fetchall.return_value = [
        ("id-1", None),  # self-supervised / no label column → SQL NULL
        ("id-2", "cat"),
    ]
    result = db.get_samples("tbl", "ing-uuid")
    assert result == [
        {"data_id": "id-1", "label": ""},
        {"data_id": "id-2", "label": "cat"},
    ]


def test_get_label_counts_null_label_normalised_to_empty_string(
    db, mock_engine_factory
):
    """SQL NULL and '' both map to '' so they merge into a single count."""
    _, _, conn = mock_engine_factory
    # get_label_counts is the materialised form of iter_label_counts, which
    # fetches in chunks (#488) rather than calling fetchall().
    conn.execute.return_value.partitions.return_value = [
        [(None, 3), ("", 2)],
        [("cat", 5)],
    ]
    result = db.get_label_counts("tbl", "ing-uuid")
    assert result == {"": 5, "cat": 5}


# ---------------------------------------------------------------------------
# Run journal: orphan-row reconciliation (backend#1028 item 2)
# ---------------------------------------------------------------------------


def _executed_sql(conn):
    """The raw SQL text of every statement executed on the mock connection."""
    return [str(call.args[0]) for call in conn.execute.call_args_list]


def test_record_ingest_started_journals_idempotently(db, mock_engine_factory):
    """Journaling a run's start must lazily create the journal table (with the
    task column) and use INSERT IGNORE (idempotent re-entry), then commit —
    mirroring the salt store's lazy-creation pattern (#225)."""
    _, _, conn = mock_engine_factory
    db.record_ingest_started("tbl", "run-a")
    sql = _executed_sql(conn)
    create = next(
        s for s in sql if "CREATE TABLE IF NOT EXISTS `tracebloc_ingest_runs`" in s
    )
    assert "task VARCHAR(64)" in create
    assert any("INSERT IGNORE INTO `tracebloc_ingest_runs`" in s for s in sql)
    conn.commit.assert_called()


def test_record_ingest_started_persists_task(db, mock_engine_factory):
    """The run's task (ingest category) is written into the journal so a
    cluster-local reader can report it without a backend round-trip."""
    _, _, conn = mock_engine_factory
    db.record_ingest_started("tbl", "run-a", "image_classification")
    insert = next(
        c.args[0]
        for c in conn.execute.call_args_list
        if "INSERT IGNORE INTO `tracebloc_ingest_runs`" in str(c.args[0])
    )
    assert "task" in str(insert)
    assert insert.compile().params == {
        "ingestor_id": "run-a",
        "table_name": "tbl",
        "task": "image_classification",
    }


def test_record_ingest_started_task_defaults_null(db, mock_engine_factory):
    """An older caller that omits the task still journals the run (task NULL),
    so registration/reclaim keep working across the rollout."""
    _, _, conn = mock_engine_factory
    db.record_ingest_started("tbl", "run-a")
    insert = next(
        c.args[0]
        for c in conn.execute.call_args_list
        if "INSERT IGNORE INTO `tracebloc_ingest_runs`" in str(c.args[0])
    )
    assert insert.compile().params["task"] is None


def test_ensure_runs_table_adds_task_column_when_missing(db, mock_engine_factory):
    """A runs table created before the task column existed is migrated in
    place: information_schema reports it absent, so an ALTER adds it."""
    _, _, conn = mock_engine_factory
    conn.execute.return_value.scalar.return_value = 0  # column absent
    db.record_ingest_started("tbl", "run-a", "tabular_classification")
    sql = _executed_sql(conn)
    assert any(
        "ALTER TABLE `tracebloc_ingest_runs`" in s and "ADD COLUMN task" in s
        for s in sql
    )


def test_ensure_runs_table_skips_alter_when_task_column_present(
    db, mock_engine_factory
):
    """When the column already exists, no ALTER runs (the common steady-state
    path — a bare check, no DDL)."""
    _, _, conn = mock_engine_factory
    conn.execute.return_value.scalar.return_value = 1  # column present
    db.record_ingest_started("tbl", "run-a", "tabular_classification")
    assert not any("ADD COLUMN task" in s for s in _executed_sql(conn))


def test_ensure_runs_table_swallows_lost_alter_race(db, mock_engine_factory):
    """A concurrent ingestor can add the column between our absence check and
    our own ALTER, so the ALTER fails duplicate-column. The handler must roll
    back the (now pending-rollback) connection BEFORE re-checking — else the
    re-check hits PendingRollbackError (#219) — then swallow the race once the
    column is confirmed present."""
    from sqlalchemy.exc import DBAPIError

    _, _, conn = mock_engine_factory
    events = []
    conn.rollback.side_effect = lambda *a, **k: events.append("rollback")
    altered = {"attempted": False}

    def execute_side_effect(stmt, *a, **k):
        sql = str(stmt)
        events.append(sql)
        if "ADD COLUMN task" in sql:
            altered["attempted"] = True
            raise DBAPIError(sql, {}, Exception("Duplicate column name 'task'"))
        result = MagicMock()
        if "information_schema.columns" in sql:
            # Absent on the pre-ALTER check; present on the re-check (the
            # racing ingestor's ALTER has landed). Our ALTER attempt is the
            # ordering marker between the two.
            result.scalar.return_value = 1 if altered["attempted"] else 0
        return result

    conn.execute.side_effect = execute_side_effect
    # Must NOT raise: the lost race is swallowed once the column is present.
    db.record_ingest_started("tbl", "run-a", "tabular_classification")

    # A rollback ran between the failed ALTER and the re-check.
    alter_idx = next(i for i, e in enumerate(events) if "ADD COLUMN task" in e)
    recheck_idx = next(
        i
        for i, e in enumerate(events)
        if i > alter_idx and "information_schema.columns" in e
    )
    assert "rollback" in events[alter_idx:recheck_idx]


def test_ensure_runs_table_reraises_non_race_alter_failure(db, mock_engine_factory):
    """When the ALTER fails and the column is STILL absent afterwards (a genuine
    DDL failure, not a lost race), the error propagates rather than being
    silently swallowed."""
    from sqlalchemy.exc import DBAPIError

    _, _, conn = mock_engine_factory
    conn.rollback.side_effect = lambda *a, **k: None

    def execute_side_effect(stmt, *a, **k):
        sql = str(stmt)
        if "ADD COLUMN task" in sql:
            raise DBAPIError(sql, {}, Exception("some other DDL error"))
        result = MagicMock()
        if "information_schema.columns" in sql:
            result.scalar.return_value = 0  # still absent -> not a race
        return result

    conn.execute.side_effect = execute_side_effect
    with pytest.raises(DBAPIError):
        db.record_ingest_started("tbl", "run-a", "tabular_classification")
    conn.rollback.assert_called()


def test_mark_ingest_registered_flips_journal_flag(db, mock_engine_factory):
    """Marking registration must UPDATE the run's journal row to
    registered=1, scoped to this (ingestor_id, table_name)."""
    _, _, conn = mock_engine_factory
    db.mark_ingest_registered("tbl", "run-a")
    update = next(s for s in _executed_sql(conn) if "UPDATE" in s)
    assert "registered = 1" in update
    assert "ingestor_id = :ingestor_id" in update
    stmt = next(
        c.args[0] for c in conn.execute.call_args_list if "UPDATE" in str(c.args[0])
    )
    assert stmt.compile().params == {"ingestor_id": "run-a", "table_name": "tbl"}
    conn.commit.assert_called()


def test_mark_ingest_unregistered_resets_journal_flag(db, mock_engine_factory):
    """The failure-path undo must UPDATE the run's journal row back to
    registered=0 (and clear registered_at), scoped to this
    (ingestor_id, table_name) — so a send failure after the pre-send flip
    leaves the rows reclaimable (backend#1028, bugbot High)."""
    _, _, conn = mock_engine_factory
    db.mark_ingest_unregistered("tbl", "run-a")
    update = next(s for s in _executed_sql(conn) if "UPDATE" in s)
    assert "registered = 0" in update
    assert "registered_at = NULL" in update
    assert "ingestor_id = :ingestor_id" in update
    stmt = next(
        c.args[0] for c in conn.execute.call_args_list if "UPDATE" in str(c.args[0])
    )
    assert stmt.compile().params == {"ingestor_id": "run-a", "table_name": "tbl"}
    conn.commit.assert_called()


def test_reclaim_dead_run_rows_deletes_each_dead_run(db, mock_engine_factory):
    """The reconcile pass deletes rows per dead run via the #227
    delete_by_ingestor_id (so logging/retry behavior is shared) and reports
    ``{dead_id: deleted_count}``."""
    _, _, conn = mock_engine_factory
    conn.execute.return_value.fetchall.return_value = [("dead-a",), ("dead-b",)]
    with patch.object(db, "delete_by_ingestor_id", side_effect=[3, 2]) as delete:
        result = db.reclaim_dead_run_rows("tbl", "current-run")
    assert result == {"dead-a": 3, "dead-b": 2}
    delete.assert_any_call("tbl", "dead-a")
    delete.assert_any_call("tbl", "dead-b")


def test_reclaim_query_targets_only_unregistered_non_current_runs(
    db, mock_engine_factory
):
    """The orphan query must (i) join the journal — rows that predate it
    (legacy ingests) never match, (ii) require registered = 0, and (iii)
    exclude the current run, so re-running the pass mid-ingest is safe."""
    _, _, conn = mock_engine_factory
    conn.execute.return_value.fetchall.return_value = []
    db.reclaim_dead_run_rows("tbl", "current-run")
    select = next(s for s in _executed_sql(conn) if "SELECT DISTINCT" in s)
    assert "JOIN `tracebloc_ingest_runs`" in select
    assert "registered = 0" in select
    assert "!= :current_ingestor_id" in select
    stmt = next(
        c.args[0]
        for c in conn.execute.call_args_list
        if "SELECT DISTINCT" in str(c.args[0])
    )
    assert stmt.compile().params == {
        "table_name": "tbl",
        "current_ingestor_id": "current-run",
    }


def test_reclaim_dead_run_rows_noop_when_nothing_dead(db, mock_engine_factory):
    """No journaled-started-unregistered ids owning rows → no deletes, empty
    result (idempotency: the second pass after a reclaim hits this path)."""
    _, _, conn = mock_engine_factory
    conn.execute.return_value.fetchall.return_value = []
    with patch.object(db, "delete_by_ingestor_id") as delete:
        assert db.reclaim_dead_run_rows("tbl", "current-run") == {}
    delete.assert_not_called()


@pytest.mark.parametrize("reserved", ["tracebloc_ingest_meta", "tracebloc_ingest_runs"])
def test_create_table_rejects_reserved_bookkeeping_tables(reserved):
    """The salt store (#225) and the run journal (backend#1028) share the
    cluster MySQL with dataset tables — a dataset must not be able to claim
    (and corrupt) either name. The guard runs before any DB I/O."""
    db = Database.__new__(Database)
    with pytest.raises(ValueError, match="reserved"):
        db.create_table(reserved, {"feature_0": "FLOAT"})


# ---------------------------------------------------------------------------
# Run journal: enumeration for the metadata backfill sweep
# ---------------------------------------------------------------------------


def test_list_registered_runs_returns_registered_only(db, mock_engine_factory):
    """The backfill sweep enumerates only REGISTERED runs, one row each, mapped
    to {ingestor_id, table_name, task}. A NULL task (pre-task-column run) is
    preserved as None."""
    _, _, conn = mock_engine_factory
    conn.execute.return_value.fetchall.return_value = [
        ("run-a", "ta", "tabular_classification"),
        ("run-b", "tb", None),
    ]
    result = db.list_registered_runs()
    assert result == [
        {"ingestor_id": "run-a", "table_name": "ta", "task": "tabular_classification"},
        {"ingestor_id": "run-b", "table_name": "tb", "task": None},
    ]
    select = next(
        s for s in _executed_sql(conn) if "SELECT ingestor_id, table_name, task" in s
    )
    assert "WHERE registered = 1" in select


def test_list_dataset_ingestor_ids_scans_tables_excluding_framework(
    db, mock_engine_factory
):
    """Discovers dataset tables via information_schema (tables carrying an
    ingestor_id column), skips the framework tables (run journal + salt store),
    and returns each table's distinct non-null ingestor_id."""
    _, _, conn = mock_engine_factory

    def _result(rows):
        r = MagicMock()
        r.fetchall.return_value = rows
        return r

    # 1st execute: information_schema table list (includes a framework table).
    # then one execute per NON-framework table, in returned order.
    conn.execute.side_effect = [
        _result([("ds_one",), ("tracebloc_ingest_runs",), ("ds_two",)]),
        _result([("i1",), ("i2",)]),  # ds_one distinct ids
        _result([("i3",)]),  # ds_two distinct ids
    ]

    result = db.list_dataset_ingestor_ids()
    assert result == [
        {"ingestor_id": "i1", "table_name": "ds_one"},
        {"ingestor_id": "i2", "table_name": "ds_one"},
        {"ingestor_id": "i3", "table_name": "ds_two"},
    ]

    executed = _executed_sql(conn)
    # Discovery query hit information_schema for the ingestor_id column.
    assert any(
        "information_schema.columns" in s and "ingestor_id" in s for s in executed
    )
    # The framework run-journal table was skipped — never SELECTed as a dataset.
    assert not any("`tracebloc_ingest_runs`" in s for s in executed)
    # Per-dataset-table id scans excluded null/empty ids.
    assert any("FROM `ds_one`" in s and "IS NOT NULL" in s for s in executed)


def test_list_dataset_ingestor_ids_skips_a_table_that_errors(db, mock_engine_factory):
    """A single unreadable table (scan timeout / dropped mid-sweep / perms) is
    skipped and discovery continues — the sweep's 'one bad table can't abort the
    rollout' guarantee must hold at enumeration time too. (bugbot)"""
    _, _, conn = mock_engine_factory

    def _result(rows):
        r = MagicMock()
        r.fetchall.return_value = rows
        return r

    conn.execute.side_effect = [
        _result([("ds_one",), ("ds_bad",), ("ds_two",)]),  # information_schema
        _result([("i1",)]),  # ds_one
        RuntimeError("scan timeout"),  # ds_bad — per-table query fails
        _result([("i2",)]),  # ds_two still scanned
    ]

    result = db.list_dataset_ingestor_ids()
    assert result == [
        {"ingestor_id": "i1", "table_name": "ds_one"},
        {"ingestor_id": "i2", "table_name": "ds_two"},
    ]


# ---------------------------------------------------------------------------
# iter_label_counts: chunked fetch of the outbound label counts (#488)
# ---------------------------------------------------------------------------


def test_iter_label_counts_yields_pairs_in_chunks(db, mock_engine_factory):
    """Fetched via partitions() rather than fetchall(), so a raw continuous
    target's ungrouped counts are never all in memory at once."""
    _, _, conn = mock_engine_factory
    conn.execute.return_value.partitions.return_value = [
        [("a", 1), ("b", 2)],
        [("c", 3)],
    ]
    assert list(db.iter_label_counts("tbl", "ing-uuid")) == [
        ("a", 1),
        ("b", 2),
        ("c", 3),
    ]
    conn.execute.return_value.partitions.assert_called_once_with(
        db.LABEL_COUNT_FETCH_SIZE
    )


def test_iter_label_counts_normalises_null_label_to_empty_string(
    db, mock_engine_factory
):
    """Same NULL handling as get_samples, so the send boundary maps both to the
    missing-label bucket."""
    _, _, conn = mock_engine_factory
    conn.execute.return_value.partitions.return_value = [[(None, 4), ("cat", 1)]]
    assert list(db.iter_label_counts("tbl", "ing-uuid")) == [("", 4), ("cat", 1)]


def test_iter_label_counts_is_lazy(db, mock_engine_factory):
    """Nothing is fetched until the caller iterates — the generator holds the
    connection only while it is being consumed."""
    _, _, conn = mock_engine_factory
    conn.execute.return_value.partitions.return_value = [[("a", 1)]]
    # Relative to whatever the Database fixture already ran on this connection.
    before = conn.execute.call_count
    gen = db.iter_label_counts("tbl", "ing-uuid")
    assert conn.execute.call_count == before
    next(gen)
    assert conn.execute.call_count == before + 1
