"""Content-compared real-ingest e2e (backend#1009, Tier-1).

The offline parity harness (Go goldens) proves the *plan*; nothing proved the
*stored* content. This test closes that gap: for a small shared fixture set it
runs the REAL engine into REAL MySQL and asserts, per fixture, that the STORED
table matches the content DERIVED FROM THE SOURCE CSV in-test (pandas) — never
a committed golden, so it stays honest if a template changes.

Per fixture it pins four dimensions of the stored table:

  (a) resolved label column values per row — the multiset of the standard
      ``label`` column equals the multiset of the source label column;
  (b) the EXACT class -> count map (per-value, not a bucket-sum) — this is the
      dimension that catches the #340 class (a case-/whitespace-mismatched
      label column that passes preflight then reads all-NULL: the stored map
      collapses to ``{None: N}`` instead of the real per-class counts);
  (c) the row count equals the source row count;
  (d) per-column dtypes from ``information_schema.columns`` — the ``label``
      column is VARCHAR and every schema-declared feature column carries the
      MySQL type its declared schema type maps to.

Real-MySQL-gated like the rest of ``e2e/`` (conftest skips collection when no
MySQL is reachable), so the default unit ``pytest`` run is unaffected.
"""

import os
from collections import Counter
from pathlib import Path

import mysql.connector
import pandas as pd
import pytest
import yaml

from tracebloc_ingestor.cli import run

REPO = Path(__file__).resolve().parents[1]
T = REPO / "templates"


def _cfg(**kw):
    base = {"apiVersion": "tracebloc.io/v1", "kind": "IngestConfig", "intent": "train"}
    base.update(kw)
    return base


# One entry per fixture. Reuses the bundled templates/ sample data; each config
# is the correct config for that bundled dataset (matched to the existing e2e
# suite). Both are classification (passthrough label policy) so the stored
# `label` column carries the raw source label value verbatim — the precondition
# for the per-value class->count comparison in (b).
CASES = [
    dict(
        id="image_classification",
        cfg=_cfg(
            table="cc_img",
            category="image_classification",
            csv=str(T / "image_classification/data/labels_file_sample.csv"),
            images=str(T / "image_classification/data/images"),
            label="label",
            spec={"file_options": {"extension": ".jpeg", "target_size": [256, 256]}},
            # #350: this fixture is 6 unique (filename,label) rows repeated 96×.
            # The content-vs-source comparison below assumes 1:1 storage, so pin
            # uuid; under the content_hash default these rows dedup to 6 (working
            # as designed). content_hash dedup/retry is covered by
            # test_database_e2e.py. The tabular case below keeps the default
            # (unique rows ⇒ content_hash stores 1:1 too), exercising the full
            # run.main() stack with content_hash.
            data_id={"strategy": "uuid"},
        ),
    ),
    dict(
        id="tabular_classification",
        cfg=_cfg(
            table="cc_tabclf",
            category="tabular_classification",
            csv=str(
                T
                / "tabular_classification/tabular_classification_sample_in_csv_format.csv"
            ),
            schema={
                "feature_00": "FLOAT",
                "feature_01": "FLOAT",
                "feature_02": "FLOAT",
                "label": "INT",
            },
            label="label",
        ),
    ),
]


# Declared schema type (length-stripped, upper-cased) -> the DATA_TYPE MySQL
# reports in information_schema.columns. Mirrors the generic-SQLAlchemy mapping
# in database._get_sqlalchemy_type (INT/TINYINT/... -> Integer -> `int`, etc.).
SCHEMA_TO_INFORMATION_SCHEMA = {
    "VARCHAR": "varchar",
    "CHAR": "char",
    "TEXT": "text",
    "INT": "int",
    "INTEGER": "int",
    # TINYINT/SMALLINT/MEDIUMINT all map to SQLAlchemy Integer in
    # database._get_sqlalchemy_type, which MySQL renders as `int` — so
    # information_schema reports `int`, not the narrower declared width.
    "TINYINT": "int",
    "SMALLINT": "int",
    "MEDIUMINT": "int",
    "BIGINT": "bigint",
    "FLOAT": "float",
    "DOUBLE": "double",
}


def _connect():
    return mysql.connector.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ["MYSQL_PORT"]),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
    )


def _drop(table):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    conn.commit()
    cur.close()
    conn.close()


def _stored_labels(table):
    """The stored ``label`` column, one entry per row. Kept in raw form: a NULL
    stays None (so an all-NULL #340 regression reads {None: N}) and any real
    value is normalized to str to compare against the VARCHAR round-trip."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(f"SELECT `label` FROM `{table}`")
    labels = [None if v is None else str(v) for (v,) in cur.fetchall()]
    cur.close()
    conn.close()
    return labels


def _column_types(table):
    """{column_name: information_schema DATA_TYPE (lower-case)} for the table."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (os.environ["DB_NAME"], table),
    )
    types = {name: dtype.lower() for name, dtype in cur.fetchall()}
    cur.close()
    conn.close()
    return types


@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
def test_stored_content_matches_source_csv(case, tmp_path, monkeypatch, request):
    cfg = case["cfg"]
    table = cfg["table"]
    _drop(table)  # deterministic on re-run
    # Drop again on exit so a passing run leaves the DB clean and a re-run
    # (or a later fixture) never trips over a leftover table.
    request.addfinalizer(lambda: _drop(table))

    config_path = tmp_path / "ingest.yaml"
    config_path.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("INGEST_CONFIG", str(config_path))

    rc = run.main()
    assert rc == 0, f"{case['id']}: ingest exited {rc}"

    # Expected content is DERIVED FROM THE SOURCE CSV in-test — no golden.
    source = pd.read_csv(cfg["csv"])
    label = cfg.get("label")
    label_col = label.get("column") if isinstance(label, dict) else label
    # The VARCHAR label column stores str(value); normalize the source the same
    # way so an int class id (0/1) compares equal to its stored "0"/"1".
    expected_labels = [str(v) for v in source[label_col].tolist()]

    stored_labels = _stored_labels(table)

    # ── (c) row count == source rows ─────────────────────────────────────
    assert len(stored_labels) == len(source), (
        f"{case['id']}: {len(stored_labels)} rows stored, "
        f"{len(source)} rows in source CSV"
    )

    # ── (a) resolved label column values per row (order-independent) ──────
    assert sorted(stored_labels, key=lambda x: (x is None, x)) == sorted(
        expected_labels
    ), (
        f"{case['id']}: stored label values differ from source "
        f"(stored sample: {stored_labels[:10]}, source sample: {expected_labels[:10]})"
    )

    # ── (b) EXACT class -> count map (per-value, catches the #340 class) ──
    assert Counter(stored_labels) == Counter(expected_labels), (
        f"{case['id']}: stored class->count map {dict(Counter(stored_labels))} "
        f"!= source {dict(Counter(expected_labels))}"
    )

    # ── (d) per-column dtypes from information_schema.columns ─────────────
    col_types = _column_types(table)
    # The standard label column is always VARCHAR regardless of the declared
    # schema type for the label (which is dropped and mapped to `label`).
    assert col_types.get("label") == "varchar", (
        f"{case['id']}: label column DATA_TYPE is {col_types.get('label')!r}, "
        f"expected 'varchar'"
    )
    # Every schema-declared feature column carries the type its declared schema
    # type maps to. Expected DATA_TYPE is derived from the config, not hardcoded.
    schema = cfg.get("schema", {})
    for col, declared in schema.items():
        if col == label_col:
            continue  # label maps to the standard `label` column, checked above
        base_type = declared.split("(", 1)[0].strip().upper()
        expected_dtype = SCHEMA_TO_INFORMATION_SCHEMA.get(base_type)
        assert expected_dtype is not None, (
            f"{case['id']}: test needs a DATA_TYPE mapping for declared "
            f"schema type {declared!r}"
        )
        assert col_types.get(col) == expected_dtype, (
            f"{case['id']}: column {col!r} DATA_TYPE is {col_types.get(col)!r}, "
            f"expected {expected_dtype!r} (declared {declared!r})"
        )
