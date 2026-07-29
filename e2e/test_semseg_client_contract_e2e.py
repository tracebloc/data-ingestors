"""backend#816 — the semseg ingestor→client mask_id contract, end-to-end.

Ingests a real ``semantic_segmentation`` dataset into a real MySQL (the e2e
harness DB) and asserts the ingestor produces EXACTLY what the training client
resolves masks from — checked against the CLIENT'S OWN derivation rule. This is
the boundary the develop regression slipped through: the ingestor's own e2e only
asserted "sidecar files copied", and the client's tests mock the metadata
DataFrame, so nothing proved the ingested table + files satisfy the client.

The client (tracebloc-client/core/datasets/segmentation_dataset_pytorch.py,
``__getitem__``) does, per row::

    mask_id   = str(row["mask_id"]).strip()
    mask_name = mask_id.split(".")[0] if "." in mask_id else mask_id
    detected  = find_image_extn(self.path, mask_name)   # .jpg/.jpeg/.png, any case
    if not detected: raise FileNotFoundError(...)        # NO naming fallback

Two halves, matching di#358's REQUIRED-contract decision:

- DECLARED case (the template path): the manifest declares + populates
  ``mask_id``, so the ingest succeeds and the stored table satisfies the client
  exactly — (1) ``row["mask_id"]`` is populated (never NULL), and (2) a mask
  file exists at ``DEST/<mask_name><ext>`` (what ``find_image_extn`` resolves).
- MISSING-COLUMN case: ``mask_id`` is now REQUIRED — a manifest lacking the
  column is REJECTED at preflight by ``MaskIdColumnValidator`` (no silent schema
  mutation), so the ingest FAILS (rc != 0) and creates no table.

Skipped by conftest's ``collect_ignore_glob`` unless a MySQL is reachable.
"""

import os
from pathlib import Path

import mysql.connector
import pandas as pd
import yaml

from tracebloc_ingestor.cli import run
from tracebloc_ingestor.config import Config

REPO = Path(__file__).resolve().parents[1]
SEG = REPO / "templates" / "semantic_segmentation" / "semantic_data"

# The extensions + any-case matching the client's find_image_extn tries.
_IMG_EXTS = (".jpg", ".jpeg", ".png")


def _cfg(**kw):
    base = {"apiVersion": "tracebloc.io/v1", "kind": "IngestConfig", "intent": "train"}
    base.update(kw)
    return base


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


def _table_exists(table) -> bool:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE %s", (table,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists


def _client_would_resolve_mask(data_dir: Path, mask_id: str) -> bool:
    """Mirror the client's resolution: mask_name = mask_id sans extension, then
    look for mask_name + a known image extension (any case) in data_dir.
    Returns True iff a file exists — i.e. the client would NOT FileNotFoundError.
    """
    mask_name = mask_id.split(".")[0] if "." in mask_id else mask_id
    for ext in _IMG_EXTS:
        for cand in (ext, ext.upper()):
            if (data_dir / f"{mask_name}{cand}").exists():
                return True
    return False


def test_semseg_declared_ingest_satisfies_client_mask_contract(tmp_path, monkeypatch):
    # DECLARED case: the template manifest declares + populates mask_id, so the
    # ingest succeeds and the stored table satisfies the client's derivation.
    table = "e2e_seg_declared"
    _drop(table)  # deterministic on re-run
    cfg = _cfg(
        table=table,
        category="semantic_segmentation",
        csv=str(SEG / "labels_file_sample.csv"),
        images=str(SEG / "images"),
        masks=str(SEG / "masks"),
        label="image_label",
        schema={"mask_id": "VARCHAR(255)"},
    )
    config_path = tmp_path / "ingest.yaml"
    config_path.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("INGEST_CONFIG", str(config_path))

    rc = run.main()
    assert rc == 0, f"semseg ingest ({table}) exited {rc}"

    rows = _rows(table)
    src = pd.read_csv(cfg["csv"])
    assert len(rows) == len(src), f"{table}: {len(rows)} rows, expected {len(src)}"

    # (1) The ingested table MUST carry a populated mask_id column (declared in
    # the manifest, backend#816). The client SELECTs it; a NULL/absent value
    # makes it derive a garbage filename + crash.
    assert all("mask_id" in r for r in rows), (
        f"{table}: ingested rows have no mask_id column (client SELECTs it); "
        f"columns = {list(rows[0].keys()) if rows else 'no rows'}"
    )
    src_mask_ids = set(src["mask_id"].astype(str))
    dest = Path(Config.STORAGE_PATH) / table
    for r in rows:
        mask_id = r["mask_id"]
        assert mask_id, (
            f"{table}: NULL/empty mask_id on data_id={r.get('data_id')!r} — the "
            f"client would derive a garbage mask filename and raise FileNotFoundError"
        )
        assert str(mask_id) in src_mask_ids, (
            f"{table}: stored mask_id {mask_id!r} not one of the source values "
            f"{src_mask_ids}"
        )
        # (2) The mask file is at exactly the path the client derives. This is
        # the cross-repo assertion: it fails if the ingestor stored mask_id but
        # didn't copy the mask, or copied it under a name the client can't find.
        assert _client_would_resolve_mask(dest, str(mask_id)), (
            f"{table}: no mask file for mask_id={mask_id!r} at the client-derived "
            f"path under {dest} — PyTorchSegmentationDataset would raise "
            f"FileNotFoundError at train time"
        )


def test_semseg_missing_mask_id_column_is_rejected(tmp_path, monkeypatch):
    # MISSING-COLUMN case: mask_id is REQUIRED (di#358 — no silent schema
    # mutation). The template CSV always ships a mask_id header, so build a copy
    # WITHOUT that column to exercise the rejection. MaskIdColumnValidator must
    # fail this at preflight → the ingest exits non-zero and creates no table.
    table = "e2e_seg_no_mask_id"
    _drop(table)  # deterministic on re-run

    src = pd.read_csv(SEG / "labels_file_sample.csv")
    assert "mask_id" in src.columns, "template CSV should ship a mask_id header"
    no_mask = src.drop(columns=["mask_id"])
    csv_no_mask = tmp_path / "labels_no_mask_id.csv"
    no_mask.to_csv(csv_no_mask, index=False)

    cfg = _cfg(
        table=table,
        category="semantic_segmentation",
        csv=str(csv_no_mask),
        images=str(SEG / "images"),
        masks=str(SEG / "masks"),
        label="image_label",
        schema={"mask_id": "VARCHAR(255)"},
    )
    config_path = tmp_path / "ingest.yaml"
    config_path.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("INGEST_CONFIG", str(config_path))

    rc = run.main()
    assert rc != 0, (
        f"semseg ingest with a manifest lacking mask_id should be REJECTED at "
        f"preflight (mask_id is required, backend#816), but exited {rc}"
    )
    # Rejection happens before the destination table is created — no orphan table.
    assert not _table_exists(
        table
    ), f"{table}: a validator-rejected ingest must not create a table"


def test_semseg_undeclared_mask_id_is_rejected(tmp_path, monkeypatch):
    # ORIGINAL #816 SHAPE: the manifest CSV HAS mask_id (populated), but the
    # config omits `schema`, so mask_id is never a declared -> stored column.
    # RecordProcessor would silently drop it (the develop regression). The
    # declaration half of MaskIdColumnValidator must reject this at preflight —
    # a CSV-only check would wave it straight through to a broken table.
    table = "e2e_seg_undeclared_mask_id"
    _drop(table)  # deterministic on re-run

    src = pd.read_csv(SEG / "labels_file_sample.csv")
    assert "mask_id" in src.columns, "template CSV should ship a populated mask_id"

    cfg = _cfg(
        table=table,
        category="semantic_segmentation",
        csv=str(SEG / "labels_file_sample.csv"),  # HAS mask_id
        images=str(SEG / "images"),
        masks=str(SEG / "masks"),
        label="image_label",
        # NB: no `schema` key -> mask_id is not declared, so it would be dropped.
    )
    config_path = tmp_path / "ingest.yaml"
    config_path.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("INGEST_CONFIG", str(config_path))

    rc = run.main()
    assert rc != 0, (
        f"semseg ingest that doesn't DECLARE mask_id in the schema should be "
        f"REJECTED at preflight (mask_id would be dropped -> client crash, "
        f"backend#816), but exited {rc}"
    )
    assert not _table_exists(
        table
    ), f"{table}: a validator-rejected ingest must not create a table"
