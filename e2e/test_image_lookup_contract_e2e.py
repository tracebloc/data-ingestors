"""backend#1706 — the ingestor→trainer IMAGE-lookup contract, end to end.

Sibling of ``test_semseg_client_contract_e2e.py``, which pins the same kind of
boundary for ``mask_id``. Here the subject is the image itself, and the bug it
guards against already shipped: tracebloc-engine#615, where keypoint and
semantic-segmentation training crashed on the first batch of every
CLI-ingested dataset because the trainer resolved images by ``data_id`` while
this repo names the file after the manifest ``filename``.

Why an e2e and not just ``tests/test_ingest_storage_contract.py``: that suite
captures the rows as they are handed to ``Database.insert_batch``. The trainer
does not read those dicts — it reads a MySQL table. Everything between (the
generated DDL, column types, NULL handling, VARCHAR width) sits in the gap, and
the gap is where a filename gets truncated or a column silently isn't created.
So this ingests through the real YAML/CLI route into real MySQL and then
resolves every SELECTed row against the real files on disk, using the
**trainer's own rule** transcribed below.

The trainer's rule (tracebloc-engine ``core/utils/general.py::resolve_image_path``,
used by all four CV dataset readers since #615), per row::

    for column in ("filename", "data_id"):        # filename FIRST
        cell = row[column]                        # skip absent / NULL / blank
        if isfile(dir/cell):        -> resolve    # value carrying its extension
        stem = cell minus a RECOGNISED image suffix
        if isfile(dir/stem + probed extension) -> resolve
    else: FileNotFoundError                       # crashes the training batch

Coverage: every file-bearing CV category under the DEFAULT ``data_id``
strategy — the shape `tracebloc dataset push` produces and the one #615 broke —
plus one case under the ``column`` alias the keypoint / semseg Python templates
set, which is the coincidence that hid the bug.

Skipped by conftest's ``collect_ignore_glob`` unless a MySQL is reachable.
"""

import os
from pathlib import Path

import mysql.connector
import pytest
import yaml

from tracebloc_ingestor.cli import run
from tracebloc_ingestor.config import Config
from tracebloc_ingestor.storage_contract import (
    EXTENSION_COLUMN,
    IMAGE_NAME_COLUMN,
    IMAGE_NAME_COLUMNS,
    RECOGNISED_IMAGE_SUFFIXES,
    ROW_ID_COLUMN,
    strip_image_extension,
)

REPO = Path(__file__).resolve().parents[1]
T = REPO / "templates"


def _cfg(**kw):
    base = {"apiVersion": "tracebloc.io/v1", "kind": "IngestConfig", "intent": "train"}
    base.update(kw)
    return base


CASES = [
    pytest.param(
        _cfg(
            table="e2e_lookup_img",
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
            table="e2e_lookup_od",
            category="object_detection",
            csv=str(T / "object_detection/data/labels_file_sample.csv"),
            images=str(T / "object_detection/data/images"),
            annotations=str(T / "object_detection/data/annotations"),
            label="image_label",
            target_size=[1920, 1080],
        ),
        id="object_detection",
    ),
    pytest.param(
        _cfg(
            table="e2e_lookup_kp",
            category="keypoint_detection",
            csv=str(T / "keypoint_detection/data/labels_file_sample.csv"),
            images=str(T / "keypoint_detection/data/images"),
            label="image_label",
            target_size=[448, 448],
            number_of_keypoints=9,
        ),
        id="keypoint_detection",
    ),
    pytest.param(
        _cfg(
            table="e2e_lookup_seg",
            category="semantic_segmentation",
            csv=str(T / "semantic_segmentation/semantic_data/labels_file_sample.csv"),
            images=str(T / "semantic_segmentation/semantic_data/images"),
            masks=str(T / "semantic_segmentation/semantic_data/masks"),
            label="image_label",
            schema={"mask_id": "VARCHAR(255)"},
        ),
        id="semantic_segmentation",
    ),
    # The template alias: data_id == filename. The trainer must resolve this
    # shape too — legacy datasets are full of it — so it stays covered.
    pytest.param(
        _cfg(
            table="e2e_lookup_kp_alias",
            category="keypoint_detection",
            csv=str(T / "keypoint_detection/data/labels_file_sample.csv"),
            images=str(T / "keypoint_detection/data/images"),
            label="image_label",
            target_size=[448, 448],
            number_of_keypoints=9,
            data_id={"strategy": "column", "column": "filename"},
        ),
        id="keypoint_detection-data_id_alias",
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
    conn.cursor().execute(f"DROP TABLE IF EXISTS `{table}`")
    conn.commit()
    conn.close()


def _rows(table):
    conn = _connect()
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT * FROM `{table}`")
    rows = cur.fetchall()
    conn.close()
    return rows


def _is_blank(value) -> bool:
    """A NULL / blank metadata cell — the trainer skips these and falls through
    to the next candidate column rather than resolving the string ``"None"``."""
    return value is None or not str(value).strip()


def _trainer_resolves(data_dir: Path, row: dict):
    """Transcription of tracebloc-engine's ``resolve_image_path``.

    Returns the resolved path, or ``None`` — which is the trainer raising
    ``FileNotFoundError`` and killing the training batch.
    """
    for column in IMAGE_NAME_COLUMNS:
        if column not in row or _is_blank(row[column]):
            continue
        cell = str(row[column]).strip()
        if (data_dir / cell).is_file():
            return data_dir / cell
        stem = strip_image_extension(cell)
        for suffix in RECOGNISED_IMAGE_SUFFIXES:
            for cased in (suffix, suffix.upper()):
                if (data_dir / f"{stem}{cased}").is_file():
                    return data_dir / f"{stem}{cased}"
    return None


def _ingest(cfg, tmp_path, monkeypatch) -> Path:
    """Run the real YAML/CLI ingest; return the dataset directory on disk."""
    config_path = tmp_path / "ingest.yaml"
    config_path.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("INGEST_CONFIG", str(config_path))
    rc = run.main()
    assert rc == 0, f"{cfg['table']}: ingest exited {rc}"
    return Path(Config.STORAGE_PATH) / cfg["table"]


@pytest.mark.parametrize("cfg", CASES)
def test_every_ingested_row_resolves_to_its_image(cfg, tmp_path, monkeypatch):
    """The assertion #615 needed: every row SELECTed from the ingested table
    resolves to a real file under the trainer's own rule."""
    table = cfg["table"]
    _drop(table)
    dest = _ingest(cfg, tmp_path, monkeypatch)

    rows = _rows(table)
    assert rows, f"{table}: ingest stored no rows"

    for row in rows:
        assert not _is_blank(row.get(IMAGE_NAME_COLUMN)), (
            f"{table}: NULL/blank {IMAGE_NAME_COLUMN} on "
            f"{ROW_ID_COLUMN}={row.get(ROW_ID_COLUMN)!r} — the trainer has "
            f"nothing to resolve the image by"
        )
        resolved = _trainer_resolves(dest, row)
        assert resolved is not None, (
            f"{table}: no image file for row "
            f"{ {c: row.get(c) for c in IMAGE_NAME_COLUMNS} } under {dest} — "
            f"the trainer's dataset reader would raise FileNotFoundError on "
            f"the first training batch (tracebloc-engine#615). Directory holds "
            f"{sorted(p.name for p in dest.iterdir())[:10]}"
        )


@pytest.mark.parametrize(
    "cfg", [c for c in CASES if "alias" not in c.id], ids=lambda c: c["table"]
)
def test_a_data_id_keyed_reader_would_still_fail(cfg, tmp_path, monkeypatch):
    """Negative control — without it the test above could pass vacuously.

    Under the default strategy ``data_id`` names no file, so the pre-#615
    reader (``data_id`` only) resolves nothing. If this ever starts failing,
    something has re-aliased ``data_id`` to the on-disk name and the suite
    above stopped proving anything.
    """
    table = cfg["table"]
    _drop(table)
    dest = _ingest(cfg, tmp_path, monkeypatch)

    rows = _rows(table)
    assert rows, f"{table}: ingest stored no rows"
    for row in rows:
        data_id_only = {ROW_ID_COLUMN: row[ROW_ID_COLUMN]}
        assert _trainer_resolves(dest, data_id_only) is None, (
            f"{table}: data_id {row[ROW_ID_COLUMN]!r} resolves to a file on "
            f"disk. That coincidence is what hid tracebloc-engine#615 — the "
            f"contract is that only {IMAGE_NAME_COLUMN} names the file."
        )


@pytest.mark.parametrize("cfg", CASES)
def test_stored_filename_survives_the_mysql_round_trip(cfg, tmp_path, monkeypatch):
    """``filename`` + ``extension`` come back out of MySQL naming the file the
    ingest wrote — no truncation, no NULL, no dropped column.

    This is the half the unit contract test cannot see: it captures the row
    *before* the insert, and the trainer reads it *after*.
    """
    table = cfg["table"]
    _drop(table)
    dest = _ingest(cfg, tmp_path, monkeypatch)

    rows = _rows(table)
    assert rows, f"{table}: ingest stored no rows"
    assert EXTENSION_COLUMN in rows[0], (
        f"{table}: no {EXTENSION_COLUMN} column in the ingested table; "
        f"columns = {sorted(rows[0])}"
    )

    on_disk = {p.name for p in dest.iterdir() if p.is_file()}
    for row in rows:
        stem = str(row[IMAGE_NAME_COLUMN])
        extension = row[EXTENSION_COLUMN] or ""
        assert f"{stem}{extension}" in on_disk or stem in on_disk, (
            f"{table}: stored ({IMAGE_NAME_COLUMN}={stem!r}, "
            f"{EXTENSION_COLUMN}={extension!r}) names no file under {dest}"
        )
