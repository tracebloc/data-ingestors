"""End-to-end ingestion equivalence: every modality's bundled template ingests.

For each modality we build an ``ingest.yaml`` matched to the bundled
``templates/`` dataset, run the real engine into MySQL, and assert it succeeds
with rows. Modalities with known engine/template gaps are ``xfail``'d against
their tracking ticket — when the fix lands the test XPASSes and the xfail can be
removed.

The matched configs here are deliberately the *correct* configs for the bundled
data — several differ from the shipped ``examples/yaml`` (which don't match the
templates) and that mismatch is exactly what this suite guards against (#134).
"""

import os
from pathlib import Path

import mysql.connector
import pytest
import yaml

from tracebloc_ingestor.cli import run

REPO = Path(__file__).resolve().parents[1]
T = REPO / "templates"


def _cfg(**kw):
    base = {"apiVersion": "tracebloc.io/v1", "kind": "IngestConfig", "intent": "train"}
    base.update(kw)
    return base


CASES = [
    pytest.param(
        _cfg(
            table="e2e_text",
            category="text_classification",
            csv=str(T / "text_classification/data/labels_file_sample.csv"),
            texts=str(T / "text_classification/data/texts"),
            label="label",
        ),
        id="text_classification",
    ),
    # sentence_pair_classification: SUPERVISED text classification (label in the
    # CSV, like text_classification) but each .txt is a tab-separated
    # text_a<TAB>text_b pair. The structural SentencePairValidator gates
    # malformed files; alignment is the data-derived text profile (#805).
    pytest.param(
        _cfg(
            table="e2e_spc",
            category="sentence_pair_classification",
            csv=str(T / "sentence_pair_classification/data/labels_file_sample.csv"),
            texts=str(T / "sentence_pair_classification/data/texts"),
            label="label",
        ),
        id="sentence_pair_classification",
    ),
    # token_classification was uncovered by any e2e test (audit gap). Same
    # on-disk layout as text classification (one .txt per sample under texts/);
    # the BIO tags travel in the `label` column. The template CSV quotes the
    # extension as '.txt', exercising that parse path too.
    pytest.param(
        _cfg(
            table="e2e_tokclf",
            category="token_classification",
            csv=str(T / "token_classification/data/labels_file_sample.csv"),
            texts=str(T / "token_classification/data/texts"),
            label="label",
        ),
        id="token_classification",
    ),
    pytest.param(
        _cfg(
            table="e2e_tte",
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
        id="time_to_event_prediction",
    ),
    pytest.param(
        _cfg(
            table="e2e_tabclf",
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
        id="tabular_classification",
    ),
    pytest.param(
        _cfg(
            table="e2e_tabreg",
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
        id="tabular_regression",
    ),
    pytest.param(
        _cfg(
            table="e2e_img",
            category="image_classification",
            csv=str(T / "image_classification/data/labels_file_sample.csv"),
            images=str(T / "image_classification/data/images"),
            label="label",
            spec={"file_options": {"extension": ".jpeg", "target_size": [256, 256]}},
        ),
        id="image_classification",
    ),
    pytest.param(
        _cfg(
            table="e2e_tsf",
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
        id="time_series_forecasting",
    ),
    # time_series_classification: sequence-grouped (one label per
    # sequence_id; fixed sequence_id/timestamp names — backend#1054). The
    # bundled sample is 6 ICU stays × 3–7 hourly timesteps; the label counts
    # the summary sends are per SEQUENCE (COUNT(DISTINCT sequence_id)), and
    # a composite (sequence_id, timestamp) index is created —
    # test_tsc_sequence_semantics below pins both against the real MySQL.
    pytest.param(
        _cfg(
            table="e2e_tsc",
            category="time_series_classification",
            csv=str(
                T
                / "time_series_classification/time_series_classification_sample_in_csv_format.csv"
            ),
            schema={
                "sequence_id": "VARCHAR(64)",
                "timestamp": "TIMESTAMP",
                "heart_rate": "FLOAT",
                "resp_rate": "FLOAT",
                "temperature": "FLOAT",
                "spo2": "FLOAT",
                "lactate": "FLOAT",
                "label": "INT",
            },
            label="label",
        ),
        id="time_series_classification",
    ),
    pytest.param(
        _cfg(
            table="e2e_kp",
            category="keypoint_detection",
            csv=str(T / "keypoint_detection/data/labels_file_sample.csv"),
            images=str(T / "keypoint_detection/data/images"),
            label="image_label",
            target_size=[448, 448],
            number_of_keypoints=9,
        ),
        id="keypoint_detection",
    ),
    # object_detection: now ingests after relaxing the PascalVOC `difficult`
    # validator (#135a); target_size matched to the bundled VisDrone image.
    pytest.param(
        _cfg(
            table="e2e_od",
            category="object_detection",
            # No csv / label: object_detection is enumerated from
            # annotations/*.xml, one record per image (backend#1006).
            images=str(T / "object_detection/data/images"),
            annotations=str(T / "object_detection/data/annotations"),
            target_size=[1920, 1080],
        ),
        id="object_detection",
    ),
    # masked_language_modeling: now ingests after adding the template's
    # tokenizer.json (#137).
    pytest.param(
        _cfg(
            table="e2e_mlm",
            category="masked_language_modeling",
            csv=str(T / "masked_language_modeling/data/labels_file_sample.csv"),
            sequences=str(T / "masked_language_modeling/data/sequences"),
        ),
        id="masked_language_modeling",
    ),
    # causal_language_modeling: self-supervised raw text in texts/ (plain text
    # or prompt<TAB>completion). No tokenizer.json at ingest — alignment is the
    # data-derived text profile (#805).
    pytest.param(
        _cfg(
            table="e2e_clm",
            category="causal_language_modeling",
            csv=str(T / "causal_language_modeling/data/labels_file_sample.csv"),
            texts=str(T / "causal_language_modeling/data/texts"),
        ),
        id="causal_language_modeling",
    ),
    # seq2seq: self-supervised raw text in texts/ (a source<TAB>target pair —
    # same on-disk shape as causal LM). No tokenizer.json at ingest — alignment
    # is the data-derived text profile (#805).
    pytest.param(
        _cfg(
            table="e2e_s2s",
            category="seq2seq",
            csv=str(T / "seq2seq/data/labels_file_sample.csv"),
            texts=str(T / "seq2seq/data/texts"),
        ),
        id="seq2seq",
    ),
    # embeddings: self-supervised contrastive raw text in texts/ (an
    # anchor<TAB>positive pair or anchor<TAB>positive<TAB>negative triplet). The
    # structural ContrastivePairsValidator gates malformed files; alignment is
    # the data-derived text profile (#805), no tokenizer.json at ingest.
    pytest.param(
        _cfg(
            table="e2e_emb",
            category="embeddings",
            csv=str(T / "embeddings/data/labels_file_sample.csv"),
            texts=str(T / "embeddings/data/texts"),
        ),
        id="embeddings",
    ),
    # semantic_segmentation: masks now wire through the declarative path (the P5
    # mask_id work — the transfer resolves the mask via the schema-declared
    # mask_id), so #136's xfail is removed. The full contract (masks land in
    # DEST_PATH + mask_id stored for the client) is pinned in
    # test_characterization.py::test_characterization[semantic_segmentation].
    pytest.param(
        _cfg(
            table="e2e_seg",
            category="semantic_segmentation",
            csv=str(T / "semantic_segmentation/semantic_data/labels_file_sample.csv"),
            images=str(T / "semantic_segmentation/semantic_data/images"),
            masks=str(T / "semantic_segmentation/semantic_data/masks"),
            label="image_label",
            schema={"mask_id": "VARCHAR(255)"},
        ),
        id="semantic_segmentation",
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


def _rows(table):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM `{table}`")
    n = cur.fetchone()[0]
    cur.close()
    conn.close()
    return n


@pytest.mark.parametrize("cfg", CASES)
def test_modality_ingests_its_template(cfg, tmp_path, monkeypatch):
    table = cfg["table"]
    _drop(table)  # clean slate so the row assertion is deterministic on re-runs
    config_path = tmp_path / "ingest.yaml"
    config_path.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("INGEST_CONFIG", str(config_path))

    rc = run.main()
    assert rc == 0, f"ingest exited {rc} for {cfg['category']}"
    assert _rows(table) > 0, f"no rows ingested for {cfg['category']}"


def test_tsc_sequence_semantics(tmp_path, monkeypatch):
    """backend#1054 WS1 done-contract, against the real MySQL: a 3-patient
    toy CSV (T=5/3/7, 2 classes) ingests to 15 ROWS while the label counts
    the summary is built from are per SEQUENCE ({"0": 2, "1": 1}), and the
    composite (sequence_id, timestamp) index exists on the table."""
    table = "e2e_tsc_toy"
    _drop(table)

    rows = ["sequence_id,timestamp,heart_rate,label"]
    for pid, T, label in (("p1", 5, "1"), ("p2", 3, "0"), ("p3", 7, "0")):
        rows += [
            f"{pid},2024-01-01 {8 + t:02d}:00:00,{70 + t}.0,{label}" for t in range(T)
        ]
    csv_path = tmp_path / "toy.csv"
    csv_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    cfg = _cfg(
        table=table,
        category="time_series_classification",
        csv=str(csv_path),
        schema={
            "sequence_id": "VARCHAR(64)",
            "timestamp": "TIMESTAMP",
            "heart_rate": "FLOAT",
            "label": "INT",
        },
        label="label",
    )
    config_path = tmp_path / "ingest.yaml"
    config_path.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("INGEST_CONFIG", str(config_path))

    rc = run.main()
    assert rc == 0, f"ingest exited {rc}"

    # 15 rows stored (row unit) …
    assert _rows(table) == 15

    conn = _connect()
    cur = conn.cursor()
    # … but label counts are SEQUENCE-unit (Decision-3/T2): the same query
    # get_label_sequence_counts runs for the summary payload.
    cur.execute(
        f"SELECT label, COUNT(DISTINCT sequence_id) FROM `{table}` GROUP BY label"
    )
    counts = {str(label): int(cnt) for label, cnt in cur.fetchall()}
    assert counts == {"0": 2, "1": 1}
    # Composite (sequence_id, timestamp) secondary index exists.
    cur.execute(f"SHOW INDEX FROM `{table}`")
    by_index = {}
    for row in cur.fetchall():
        # row: (Table, Non_unique, Key_name, Seq_in_index, Column_name, ...)
        by_index.setdefault(row[2], []).append((row[3], row[4]))
    composite = [
        sorted(cols) for name, cols in by_index.items() if name.startswith("ix_")
    ]
    assert [
        (1, "sequence_id"),
        (2, "timestamp"),
    ] in composite, (
        f"composite (sequence_id, timestamp) index missing; indexes: {by_index}"
    )
    cur.close()
    conn.close()


def test_hard_killed_prior_run_reclaimed_by_retry(tmp_path, monkeypatch):
    """backend#1028 item 2, engine-level: a prior attempt that died HARD
    (OOMKilled / SIGKILL — journaled as started, rows inserted, never
    registered, #227 compensating delete bypassed) must not leak duplicates
    into the retry. The real engine, re-ingesting the same CSV into the same
    table, reclaims the dead run's rows on start and converges to the CSV's
    row count."""
    import uuid as _uuid

    from tracebloc_ingestor.config import Config
    from tracebloc_ingestor.database import Database

    table = "e2e_orphan_reclaim"
    _drop(table)

    # Simulate the hard-killed attempt with the same calls the engine makes
    # (create table → journal start → insert a batch), then "die": no
    # registration, no cleanup. Feature columns must match the retry's
    # cleaned schema or the stale-table guard would trip first.
    dead = "dead-" + _uuid.uuid4().hex[:8]
    db = Database(Config())
    db.create_table(
        table,
        {"feature_00": "FLOAT", "feature_01": "FLOAT", "feature_02": "FLOAT"},
    )
    db.record_ingest_started(table, dead)
    db.insert_batch(
        table,
        [
            {
                "data_id": f"orphan-{i}",
                "ingestor_id": dead,
                "data_intent": "train",
                "label": "0",
                "feature_00": float(i),
                "feature_01": float(i),
                "feature_02": float(i),
            }
            for i in range(3)
        ],
    )
    assert _rows(table) == 3

    # The retry: run the real engine on the bundled tabular template (8 data
    # rows) into the same table.
    cfg = _cfg(
        table=table,
        category="tabular_classification",
        csv=str(
            T / "tabular_classification/tabular_classification_sample_in_csv_format.csv"
        ),
        schema={
            "feature_00": "FLOAT",
            "feature_01": "FLOAT",
            "feature_02": "FLOAT",
            "label": "INT",
        },
        label="label",
    )
    config_path = tmp_path / "ingest.yaml"
    config_path.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("INGEST_CONFIG", str(config_path))

    rc = run.main()
    assert rc == 0, f"retry ingest exited {rc}"

    # Converged: the 8 CSV rows, not 8 + 3 — the dead run's rows are gone.
    assert _rows(table) == 8
    conn = _connect()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE ingestor_id = %s", (dead,))
    assert cur.fetchone()[0] == 0
    cur.close()
    conn.close()
    # …and the retry registered (mock backend), so its rows are protected: a
    # THIRD attempt's reclaim pass finds nothing to remove.
    probe = "probe-" + _uuid.uuid4().hex[:8]
    assert db.reclaim_dead_run_rows(table, probe) == {}
    assert _rows(table) == 8
