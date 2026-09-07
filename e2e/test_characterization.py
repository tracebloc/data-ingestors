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
import xml.etree.ElementTree as ET
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
            # #350: this fixture is 6 unique (filename,label) rows repeated 96×
            # (576 rows). Under the new content_hash default those collapse to 6
            # stored rows (content-level dedup, working as designed). This
            # harness pins the row-per-source-row golden, so opt into uuid here;
            # content_hash dedup/retry is covered by test_database_e2e.py.
            data_id={"strategy": "uuid"},
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
        id="sentence_pair_classification",
        cfg=_cfg(
            table="char_spc",
            category="sentence_pair_classification",
            csv=str(T / "sentence_pair_classification/data/labels_file_sample.csv"),
            texts=str(T / "sentence_pair_classification/data/texts"),
            label="label",
        ),
        sidecars=[str(T / "sentence_pair_classification/data/texts")],
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
            # No csv / label: object_detection is enumerated from
            # annotations/*.xml, one record per image (backend#1006).
            images=str(T / "object_detection/data/images"),
            annotations=str(T / "object_detection/data/annotations"),
            target_size=[1920, 1080],
            # No uuid pin any more. It existed because the manifest repeated 10
            # unique rows to 128 and content_hash would have deduped them to 10
            # (#350). Per-image enumeration ingests one row per annotation file,
            # so there are no duplicates to collapse and this exercises the
            # shipped default (content_hash) instead of a test-only strategy.
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
    dict(
        id="causal_language_modeling",
        cfg=_cfg(
            table="char_clm",
            category="causal_language_modeling",
            csv=str(T / "causal_language_modeling/data/labels_file_sample.csv"),
            texts=str(T / "causal_language_modeling/data/texts"),
        ),
        sidecars=[str(T / "causal_language_modeling/data/texts")],
        roundtrip_col=None,
    ),
    dict(
        id="seq2seq",
        cfg=_cfg(
            table="char_s2s",
            category="seq2seq",
            csv=str(T / "seq2seq/data/labels_file_sample.csv"),
            texts=str(T / "seq2seq/data/texts"),
        ),
        sidecars=[str(T / "seq2seq/data/texts")],
        roundtrip_col=None,
    ),
    dict(
        id="embeddings",
        cfg=_cfg(
            table="char_emb",
            category="embeddings",
            csv=str(T / "embeddings/data/labels_file_sample.csv"),
            texts=str(T / "embeddings/data/texts"),
        ),
        sidecars=[str(T / "embeddings/data/texts")],
        roundtrip_col=None,
    ),
    # semantic_segmentation: the one file-bearing modality the harness never
    # characterized (audit gap). mask_id is DECLARED in schema so it's stored
    # (the training client SELECTs it to locate masks — backend#816); the masks
    # dir is a sidecar so the manifest assertion pins that the per-row mask
    # files land in DEST_PATH (the real semseg invariant, related to #136).
    dict(
        id="semantic_segmentation",
        cfg=_cfg(
            table="char_seg",
            category="semantic_segmentation",
            csv=str(T / "semantic_segmentation/semantic_data/labels_file_sample.csv"),
            images=str(T / "semantic_segmentation/semantic_data/images"),
            masks=str(T / "semantic_segmentation/semantic_data/masks"),
            label="image_label",
            schema={"mask_id": "VARCHAR(255)"},
        ),
        sidecars=[
            str(T / "semantic_segmentation/semantic_data/images"),
            str(T / "semantic_segmentation/semantic_data/masks"),
        ],
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
    """Record the kwargs send_ingest_summary is called with, then delegate to
    the original (which returns the local-mode value without making an HTTP
    call). Captures the engine's intent to send the summary payload."""
    calls = {"send_ingest_summary": []}
    orig = client_mod.APIClient.send_ingest_summary

    def wrapper(self, *args, **kwargs):
        calls["send_ingest_summary"].append({"args": args, "kwargs": kwargs})
        return orig(self, *args, **kwargs)

    monkeypatch.setattr(client_mod.APIClient, "send_ingest_summary", wrapper)
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

    # Expected DB rows. Manifest-driven categories ingest one row per manifest
    # line; object_detection has no manifest since backend#1006 and ingests one
    # row per ANNOTATION FILE (i.e. per image), so the source of truth for its
    # row count is annotations/*.xml.
    if "csv" in cfg:
        # `source` also feeds the tabular feature round-trip below; only the
        # manifest-driven categories have one (and only they set roundtrip_col).
        source = pd.read_csv(cfg["csv"])
        expected_rows = len(source)
        expected_label_total = expected_rows
    else:
        source = None
        annotations = sorted(Path(cfg["annotations"]).glob("*.xml"))
        expected_rows = len(annotations)
        # The deliberate SECOND unit (backend#1006): `labels` counts BOXES
        # while the row count is IMAGES, so these two numbers differ on purpose
        # here — asserting both is what pins that contract end to end.
        expected_label_total = sum(
            len(ET.parse(a).getroot().findall(".//object")) for a in annotations
        )
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
    # send_ingest_summary fires exactly once, carrying the full ingest summary.
    assert len(capture_api["send_ingest_summary"]) == 1, (
        f"{case['id']}: send_ingest_summary called "
        f"{len(capture_api['send_ingest_summary'])} times"
    )
    summary_kw = capture_api["send_ingest_summary"][0]["kwargs"]
    # Table name must be passed through.
    assert summary_kw.get("table_name") == table, (
        f"{case['id']}: table_name in summary payload is "
        f"{summary_kw.get('table_name')!r}, expected {table!r}"
    )
    # Intent must match the configured intent.
    assert summary_kw.get("data_intent") == cfg["intent"], (
        f"{case['id']}: data_intent {summary_kw.get('data_intent')!r} != "
        f"{cfg['intent']!r}"
    )
    # Label counts. For manifest-driven categories the labels PARTITION the
    # rows, so their sum is the row count. object_detection does not partition:
    # one row is one IMAGE carrying a whole class multiset, so its labels sum to
    # BOXES while `record_count` carries the image count (backend#1006, the same
    # split token_classification already has). Assert whichever unit applies,
    # and for object_detection assert the image count too — the two-unit
    # contract is only pinned if BOTH halves are checked.
    total_labelled = sum(summary_kw.get("labels", {}).values())
    assert total_labelled == expected_label_total, (
        f"{case['id']}: labels total {total_labelled} != "
        f"{expected_label_total} expected"
    )
    if "csv" not in cfg:
        assert summary_kw.get("record_count") == expected_rows, (
            f"{case['id']}: record_count {summary_kw.get('record_count')!r} != "
            f"{expected_rows} images (labels sum to {total_labelled} boxes)"
        )
    # Schema: the user-declared feature columns must all be present in the
    # schema payload sent to the backend. The label column maps to the
    # framework's standard `label` column and is excluded from the check.
    label = cfg.get("label")
    label_col = label.get("column") if isinstance(label, dict) else label
    feature_cols = set(cfg.get("schema", {})) - {label_col}
    schema_sent = summary_kw.get("schema", {})
    assert feature_cols <= set(schema_sent), (
        f"{case['id']}: schema payload missing feature columns "
        f"{feature_cols - set(schema_sent)}"
    )
