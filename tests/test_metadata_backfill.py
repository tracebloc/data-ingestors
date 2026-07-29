"""Tests for metadata_backfill — recomputing a dataset's {schema, meta_data}
payload from an already-ingested table via SQL, without re-ingesting rows.

Two tiers:
* unit — the SQL-shaping helpers (_numeric_feature_stats / _categorical_counts)
  against a hand-built SQLAlchemy Table + a fake connection, so the exclusion /
  coercion / cardinality logic is covered with no database;
* integration — build_dataset_metadata end-to-end over a real in-memory SQLite
  table, asserting it reuses the ingestor's shapes (enriched schema with
  role:"target", numeric feature_stats, categorical vocab).
"""

from __future__ import annotations

from collections import Counter
from decimal import Decimal

from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    text,
)

from tracebloc_ingestor import metadata_backfill as mb
from tracebloc_ingestor.config import Config


# ---- fakes ---------------------------------------------------------------


class _FakeResult:
    def __init__(self, one=None, all_=None):
        self._one = one
        self._all = all_

    def one(self):
        return self._one

    def all(self):
        return self._all


class _FakeConn:
    """Returns canned aggregate results in execute() call order."""

    def __init__(self, results):
        self._results = list(results)

    def execute(self, _stmt):
        return self._results.pop(0)


def _table(cols):
    md = MetaData()
    type_map = {"int": Integer, "float": Float, "str": String}
    return Table("t", md, *(Column(n, type_map[t]) for n, t in cols.items()))


# ---- unit: _numeric_feature_stats ----------------------------------------


def test_numeric_stats_excludes_framework_and_nonnumeric_keeps_regression_label():
    # schema order drives the query order: id(framework→skip), label(regression
    # target→keep), age(int feature), feat(float feature), city(string→skip).
    schema = {
        "id": "BIGINT",
        "label": "FLOAT",
        "age": "INT",
        "feat": "FLOAT",
        "city": "VARCHAR(255)",
    }
    table = _table({"id": "int", "label": "float", "age": "int", "feat": "float"})
    conn = _FakeConn(
        [
            _FakeResult(one=(3, Decimal("60"), Decimal("1400"), 10.0, 30.0)),  # label
            _FakeResult(one=(3, Decimal("60"), Decimal("1400"), 18, 42)),  # age (INT)
            _FakeResult(one=(3, 6.0, 14.0, 1.0, 3.0)),  # feat
        ]
    )
    stats = mb._numeric_feature_stats(conn, table, schema, "tabular_regression")

    assert set(stats) == {"label", "age", "feat"}  # id + city excluded
    # sum/sum_sq always float; INT column keeps integer min/max, float stays float.
    assert stats["label"] == {
        "count": 3,
        "sum": 60.0,
        "sum_sq": 1400.0,
        "min": 10.0,
        "max": 30.0,
    }
    assert stats["age"]["min"] == 18 and isinstance(stats["age"]["min"], int)
    assert stats["feat"]["min"] == 1.0 and isinstance(stats["feat"]["min"], float)
    assert isinstance(stats["age"]["sum"], float)  # Decimal coerced


def test_numeric_stats_excludes_label_for_classification():
    # For a classification task the `label` column is the class label, not a
    # numeric feature — it must NOT contribute feature_stats even if numeric.
    schema = {"label": "INT", "feat": "FLOAT"}
    table = _table({"label": "int", "feat": "float"})
    conn = _FakeConn([_FakeResult(one=(2, 3.0, 5.0, 1.0, 2.0))])  # only feat queried
    stats = mb._numeric_feature_stats(conn, table, schema, "tabular_classification")
    assert set(stats) == {"feat"}


def test_numeric_stats_omits_all_null_column():
    # An all-null column has count 0 → no stats (min/max undefined), matching the
    # live ingest's "empty column omitted".
    schema = {"feat": "FLOAT"}
    table = _table({"feat": "float"})
    conn = _FakeConn([_FakeResult(one=(0, None, None, None, None))])
    assert mb._numeric_feature_stats(conn, table, schema, "tabular_classification") == {}


# ---- unit: _categorical_counts -------------------------------------------


def test_categorical_counts_excludes_label_and_framework_builds_counter():
    schema = {
        "data_id": "VARCHAR(255)",  # framework → skip
        "label": "VARCHAR(255)",  # target/class → skip
        "city": "VARCHAR(255)",  # feature → keep
        "age": "INT",  # non-string → skip
    }
    table = _table({"data_id": "str", "label": "str", "city": "str", "age": "int"})
    conn = _FakeConn([_FakeResult(all_=[("N", 2), ("S", 1)])])  # only city queried
    counts = mb._categorical_counts(conn, table, schema)
    assert set(counts) == {"city"}
    assert counts["city"] == Counter({"N": 2, "S": 1})


def test_categorical_counts_drops_over_cardinality_cap(monkeypatch):
    monkeypatch.setattr(mb, "_MAX_CATEGORICAL_CARDINALITY", 1)
    schema = {"city": "VARCHAR(255)"}
    table = _table({"city": "str"})
    conn = _FakeConn([_FakeResult(all_=[("N", 2), ("S", 1)])])  # 2 distinct > cap 1
    assert mb._categorical_counts(conn, table, schema) == {}


# ---- integration: build_dataset_metadata over real SQLite ----------------


class _FakeDB:
    def __init__(self, engine, schema):
        self.engine = engine
        self._schema = schema
        self.config = Config(EMIT_ENRICHED_SCHEMA=True, CATEGORICAL_MIN_COUNT=1)

    def get_table_schema(self, _name):
        return dict(self._schema)


def _sqlite_with_rows():
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.execute(
            text("CREATE TABLE t (id INTEGER, label REAL, feat REAL, city TEXT)")
        )
        for i, (lab, f, city) in enumerate(
            [(10.0, 1.0, "N"), (20.0, 2.0, "S"), (30.0, 3.0, "N")]
        ):
            c.execute(
                text("INSERT INTO t (id, label, feat, city) VALUES (:i,:l,:f,:c)"),
                {"i": i, "l": lab, "f": f, "c": city},
            )
    return engine


def test_build_dataset_metadata_regression_end_to_end():
    engine = _sqlite_with_rows()
    schema = {
        "id": "BIGINT",
        "label": "FLOAT",
        "feat": "FLOAT",
        "city": "VARCHAR(255)",
    }
    db = _FakeDB(engine, schema)

    out = mb.build_dataset_metadata(
        db, "t", category="tabular_regression", label_column="target"
    )

    # Enriched schema: label carries role:"target"; id/framework still present as
    # a plain column (parity with the live _schema_payload over get_table_schema).
    assert out["schema"]["label"]["role"] == "target"
    assert out["schema"]["label"]["dtype"] == "float"

    fs = out["meta_data"]["attributes"]["feature_stats"]
    # Regression target's stats land under "label"; id (framework) is excluded.
    assert fs["label"] == {
        "count": 3,
        "sum": 60.0,
        "sum_sq": 10.0**2 + 20.0**2 + 30.0**2,
        "min": 10.0,
        "max": 30.0,
    }
    assert fs["feat"]["count"] == 3 and fs["feat"]["sum"] == 6.0
    assert "id" not in fs
    # Categorical vocab (union, sorted) for the string feature.
    assert fs["city"] == {"categories": ["N", "S"]}


def test_build_dataset_metadata_classification_excludes_label_from_stats():
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.execute(text("CREATE TABLE c (id INTEGER, label TEXT, feat REAL)"))
        for i, (lab, f) in enumerate([("cat", 1.0), ("dog", 2.0)]):
            c.execute(
                text("INSERT INTO c (id, label, feat) VALUES (:i,:l,:f)"),
                {"i": i, "l": lab, "f": f},
            )
    schema = {"id": "BIGINT", "label": "VARCHAR(255)", "feat": "FLOAT"}
    db = _FakeDB(engine, schema)

    out = mb.build_dataset_metadata(
        db, "c", category="tabular_classification", label_column="label"
    )
    fs = out["meta_data"]["attributes"]["feature_stats"]
    assert "label" not in fs  # class label is not a feature
    assert set(fs) == {"feat"}
    assert out["schema"]["label"]["role"] == "target"


def test_build_dataset_metadata_manifest_category_emits_no_alignment_stats():
    # Same gate as the live cast pass (#385, bugbot High on #383): for a
    # manifest-style category the table's cells are bookkeeping, not features
    # — a keypoint Visibility TEXT column must not ship as vocab. The SQL
    # scans are skipped entirely and the payload matches a fresh ingest.
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.execute(text("CREATE TABLE t (id INTEGER, width REAL, Visibility TEXT)"))
        c.execute(
            text("INSERT INTO t (id, width, Visibility) VALUES (1, 640.0, '[1,1]')")
        )
    schema = {"id": "BIGINT", "width": "FLOAT", "Visibility": "TEXT"}
    db = _FakeDB(engine, schema)

    out = mb.build_dataset_metadata(db, "t", category="keypoint_detection")

    # Enriched schema still ships; no alignment stats are fabricated.
    assert out["schema"]["width"]["dtype"] == "float"
    assert out["meta_data"] == {}
