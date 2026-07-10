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

So the contract pinned here is exactly: (1) ``row["mask_id"]`` is populated
(never NULL), and (2) a mask file exists at ``DEST/<mask_name><ext>`` — i.e.
precisely what ``find_image_extn(data_dir, mask_name)`` resolves. Both the
DECLARED-schema (template) and SCHEMA-LESS (di#358 auto-add) cases must satisfy
it identically.

Skipped by conftest's ``collect_ignore_glob`` unless a MySQL is reachable.
"""

import os
from pathlib import Path

import mysql.connector
import pandas as pd
import pytest
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


@pytest.mark.parametrize(
    "table,schema",
    [
        ("e2e_seg_declared", {"mask_id": "VARCHAR(255)"}),  # template path
        ("e2e_seg_schemaless", None),  # schema OMITTED → di#358 auto-add
    ],
    ids=["mask_id_declared", "mask_id_schema_less"],
)
def test_semseg_ingest_satisfies_client_mask_contract(
    table, schema, tmp_path, monkeypatch
):
    _drop(table)  # deterministic on re-run
    cfg = _cfg(
        table=table,
        category="semantic_segmentation",
        csv=str(SEG / "labels_file_sample.csv"),
        images=str(SEG / "images"),
        masks=str(SEG / "masks"),
        label="image_label",
    )
    # "schema-less" = omit the schema field entirely. That's valid (schema is
    # not a required config key) whereas an empty {} is rejected by the config
    # schema's minProperties=1. The ingestor then auto-adds the masks sidecar's
    # mask_id link column (di#358) — which is exactly what this case verifies.
    if schema is not None:
        cfg["schema"] = schema
    config_path = tmp_path / "ingest.yaml"
    config_path.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("INGEST_CONFIG", str(config_path))

    rc = run.main()
    assert rc == 0, f"semseg ingest ({table}) exited {rc}"

    rows = _rows(table)
    src = pd.read_csv(cfg["csv"])
    assert len(rows) == len(src), f"{table}: {len(rows)} rows, expected {len(src)}"

    # (1) The ingested table MUST carry a populated mask_id column — declared,
    # or auto-added for the schema-less case (backend#816 / di#358). The client
    # SELECTs it; a NULL/absent value makes it derive a garbage filename + crash.
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
