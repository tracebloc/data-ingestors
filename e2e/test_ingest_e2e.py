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
            csv=str(T / "object_detection/data/labels_file_sample.csv"),
            images=str(T / "object_detection/data/images"),
            annotations=str(T / "object_detection/data/annotations"),
            label="image_label",
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
