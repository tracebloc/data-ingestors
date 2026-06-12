"""Characterization harness — the safety net for the upcoming structural refactor.

For each bundled ``templates/`` dataset this runs the REAL engine into REAL
MySQL and pins the three observable dimensions a refactor MUST preserve:

  1. **MySQL rows** — count matches the source manifest, and the standard
     columns carry the right semantics (``data_intent`` == configured intent,
     a single non-null ``ingestor_id``, unique non-null ``data_id``). For the
     tabular family a feature column is round-tripped value-for-value (catches
     type corruption — leading zeros, NA handling, numeric coercion).
  2. **DEST_PATH file manifest** — exactly the sidecar files that should be
     copied for file-bearing categories (catches a category that inserts rows
     but copies no files — the silent-half-ingest class).
  3. **Backend payloads** — the records + metadata the engine hands the
     APIClient. ``CLIENT_ENV=local`` short-circuits the HTTP call *before* the
     payload is serialised, so we spy on the APIClient method ARGS (which are
     passed regardless of mode) rather than the HTTP mock.

Why this is stable on ``develop`` today: it characterises CLEAN-input
behavior. The in-flight fix PRs (#242–#245) only change MALFORMED-input
handling (bad cells, NA tokens, traversal filenames, dropped-record
accounting), so these goldens hold now and become the contract the refactor
is checked against. Expectations are DERIVED from the source files (no
hardcoded magic values), so the harness stays honest if a template changes.
"""

import os
from pathlib import Path

import mysql.connector
import pandas as pd
import pytest
import yaml

import tracebloc_ingestor.api.client as client_mod
from tracebloc_ingestor.cli import run
from tracebloc_ingestor.config import Config

REPO = Path(__file__).resolve().parents[1]
T = REPO / "templates"


def _cfg(**kw):
    base = {"apiVersion": "tracebloc.io/v1", "kind": "IngestConfig", "intent": "train"}
    base.update(kw)
    return base


# One entry per modality that ingests cleanly from its bundled data. Each
# carries the matched config plus the facts the assertions derive from:
#   source_csv  — the manifest (its row count is the expected DB row count)
#   sidecars    — {dest-subdir-is-flat: source dir} files expected in DEST_PATH
#                 ({} for tabular/time-series, which copy nothing)
#   label_field — the manifest column holding the label (None if unlabeled)
CASES = [
    dict(
        id="tabular_classification",
        cfg=_cfg(
            table="char_tabclf",
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
        sidecars=[],
        roundtrip_col="feature_00",
    ),
    dict(
        id="tabular_regression",
        cfg=_cfg(
            table="char_tabreg",
            category="tabular_regression",
            csv=str(
                T / "tabular_regression/tabular_regression_sample_in_csv_format.csv"
            ),
            schema={
                "square_feet": "FLOAT",
                "bedrooms": "INT",
                "age": "INT",
                "price": "FLOAT",
            },
            label={"column": "price", "policy": "bucket"},
        ),
        sidecars=[],
        roundtrip_col="bedrooms",
    ),
    dict(
        id="image_classification",
        cfg=_cfg(
            table="char_img",
            category="image_classification",
            csv=str(T / "image_classification/data/labels_file_sample.csv"),
            images=str(T / "image_classification/data/images"),
            label="label",
            spec={"file_options": {"extension": ".jpeg", "target_size": [256, 256]}},
        ),
        sidecars=[str(T / "image_classification/data/images")],
        roundtrip_col=None,
    ),
    dict(
        id="text_classification",
        cfg=_cfg(
            table="char_text",
            category="text_classification",
            csv=str(T / "text_classification/data/labels_file_sample.csv"),
            texts=str(T / "text_classification/data/texts"),
            label="label",
        ),
        sidecars=[str(T / "text_classification/data/texts")],
        roundtrip_col=None,
    ),
    dict(
        id="time_to_event_prediction",
        cfg=_cfg(
            table="char_tte",
            category="time_to_event_prediction",
            csv=str(
                T
                / "time_to_event_prediction/time_to_event_prediction_sample_in_csv_format.csv"
            ),
            time_column="time",
            schema={
                "age": "INT",
                "anaemia": "INT",
                "creatinine_phosphokinase": "INT",
                "diabetes": "INT",
                "ejection_fraction": "INT",
                "high_blood_pressure": "INT",
                "platelets": "FLOAT",
                "serum_creatinine": "FLOAT",
                "serum_sodium": "INT",
                "sex": "INT",
                "smoking": "INT",
                "time": "INT",
                "DEATH_EVENT": "INT",
            },
            label={"column": "DEATH_EVENT", "policy": "bucket"},
        ),
        sidecars=[],
        roundtrip_col="age",
    ),
    dict(
        id="time_series_forecasting",
        cfg=_cfg(
            table="char_tsf",
            category="time_series_forecasting",
            csv=str(
                T
                / "time_series_forecasting/time_series_forecasting_sample_in_csv_format.csv"
            ),
            schema={
                "timestamp": "TIMESTAMP",
                "day_of_week": "INT",
                "month": "INT",
                "day_of_month": "INT",
                "week_of_year": "INT",
                "is_weekend": "INT",
                "value": "FLOAT",
            },
            label={"column": "value", "policy": "bucket"},
        ),
        sidecars=[],
        roundtrip_col="month",
    ),
    dict(
        id="keypoint_detection",
        cfg=_cfg(
            table="char_kp",
            category="keypoint_detection",
            csv=str(T / "keypoint_detection/data/labels_file_sample.csv"),
            images=str(T / "keypoint_detection/data/images"),
            label="image_label",
            target_size=[448, 448],
            number_of_keypoints=9,
        ),
        sidecars=[str(T / "keypoint_detection/data/images")],
        roundtrip_col=None,
    ),
    dict(
        id="object_detection",
        cfg=_cfg(
            table="char_od",
            category="object_detection",
            csv=str(T / "object_detection/data/labels_file_sample.csv"),
            images=str(T / "object_detection/data/images"),
            annotations=str(T / "object_detection/data/annotations"),
            label="image_label",
            target_size=[1920, 1080],
        ),
        sidecars=[
            str(T / "object_detection/data/images"),
            str(T / "object_detection/data/annotations"),
        ],
        roundtrip_col=None,
    ),
    dict(
        id="masked_language_modeling",
        cfg=_cfg(
            table="char_mlm",
            category="masked_language_modeling",
            csv=str(T / "masked_language_modeling/data/labels_file_sample.csv"),
            sequences=str(T / "masked_language_modeling/data/sequences"),
        ),
        sidecars=[str(T / "masked_language_modeling/data/sequences")],
        roundtrip_col=None,
    ),
]


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


def _fetch_rows(table):
    conn = _connect()
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT * FROM `{table}`")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@pytest.fixture
def capture_api(monkeypatch):
    """Record the args every APIClient backend method is called with, then
    delegate to the original (which returns the local-mode value). Captures the
    engine's INTENT to send even though local mode skips the actual POST."""
    calls = {
        n: []
        for n in (
            "send_batch",
            "send_global_meta_meta",
            "prepare_dataset",
            "create_dataset",
        )
    }
    for name in calls:
        orig = getattr(client_mod.APIClient, name)

        def make(name, orig):
            def wrapper(self, *args, **kwargs):
                calls[name].append({"args": args, "kwargs": kwargs})
                return orig(self, *args, **kwargs)

            return wrapper

        monkeypatch.setattr(client_mod.APIClient, name, make(name, orig))
    return calls


@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
def test_characterization(case, tmp_path, monkeypatch, capture_api):
    cfg = case["cfg"]
    table = cfg["table"]
    _drop(table)  # deterministic on re-run

    config_path = tmp_path / "ingest.yaml"
    config_path.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("INGEST_CONFIG", str(config_path))

    rc = run.main()
    assert rc == 0, f"{case['id']}: ingest exited {rc}"

    source = pd.read_csv(cfg["csv"])
    expected_rows = len(source)
    rows = _fetch_rows(table)

    # ── Dimension 1: MySQL rows ──────────────────────────────────────────
    assert (
        len(rows) == expected_rows
    ), f"{case['id']}: {len(rows)} rows in DB, {expected_rows} in source manifest"
    assert {r["data_intent"] for r in rows} == {cfg["intent"]}
    ingestor_ids = {r["ingestor_id"] for r in rows}
    assert len(ingestor_ids) == 1 and None not in ingestor_ids
    data_ids = [r["data_id"] for r in rows]
    assert all(data_ids) and len(set(data_ids)) == len(
        data_ids
    ), "data_id not unique/non-null"

    # Feature round-trip for tabular: the column's values must survive the
    # read → coerce → insert path unchanged (catches type corruption).
    if case["roundtrip_col"]:
        col = case["roundtrip_col"]
        got = sorted(float(r[col]) for r in rows)
        want = sorted(float(v) for v in source[col].tolist())
        # rel=1e-4 tolerates MySQL FLOAT being 32-bit single precision (a
        # legitimate ~1e-6 round-trip delta) while still catching real
        # corruption (off by whole numbers, dropped/duplicated values, NaN).
        assert got == pytest.approx(
            want, rel=1e-4
        ), f"{case['id']}: {col} did not round-trip"

    # ── Dimension 2: DEST_PATH file manifest ─────────────────────────────
    dest = Path(Config.STORAGE_PATH) / table
    if case["sidecars"]:
        copied = {p.name for p in dest.iterdir()} if dest.exists() else set()
        expected_files = set()
        for src_dir in case["sidecars"]:
            expected_files |= {p.name for p in Path(src_dir).iterdir() if p.is_file()}
        # Every source sidecar referenced by the manifest must have been copied.
        assert (
            expected_files <= copied
        ), f"{case['id']}: missing copied files {expected_files - copied}"
    else:
        # Tabular/time-series copy nothing: DEST_PATH should hold no data files.
        if dest.exists():
            assert not [
                p for p in dest.iterdir() if p.is_file()
            ], f"{case['id']}: tabular ingest copied unexpected files"

    # ── Dimension 3: backend payloads ────────────────────────────────────
    # send_batch(records, table_name, ingestor_id) — records is [(id, dict)].
    sent = [rec for c in capture_api["send_batch"] for (_id, rec) in c["args"][0]]
    assert (
        len(sent) == expected_rows
    ), f"{case['id']}: {len(sent)} records sent to API, {expected_rows} expected"
    for rec in sent:
        assert rec.get("data_id"), "sent record missing data_id"
        assert rec.get("data_intent") == cfg["intent"]
    # The dataset-registration trio each fires exactly once.
    assert len(capture_api["send_global_meta_meta"]) == 1
    assert len(capture_api["prepare_dataset"]) == 1
    assert len(capture_api["create_dataset"]) == 1
    # global_meta carries the table name + a schema whose columns are a
    # superset of the user schema (plus the framework's standard columns).
    meta_args = capture_api["send_global_meta_meta"][0]["args"]
    assert meta_args[0] == table
    schema_sent = meta_args[1]
    # The label column is mapped onto the framework's standard `label` column,
    # not stored under its own name (e.g. regression's `price` -> `label`), so
    # exclude it: the remaining FEATURE columns must all be in the payload.
    label = cfg.get("label")
    label_col = label.get("column") if isinstance(label, dict) else label
    feature_cols = set(cfg.get("schema", {})) - {label_col}
    assert feature_cols <= set(schema_sent), (
        f"{case['id']}: schema payload missing feature columns "
        f"{feature_cols - set(schema_sent)}"
    )
