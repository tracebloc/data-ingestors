"""Tests for ``file_transfer.reclaim_source`` (#346).

The ingestor COPIES staged sidecars from ``SRC_PATH`` into the final table
dir, so without a reclaim every file-bearing ingest left two copies on the
shared PVC (~2x disk). ``reclaim_source`` removes the staging tree after a
verified, clean load — but only when it can prove the delete won't take the
freshly-written table (or another tenant's data) with it. These pin both the
happy path and every SAFETY guard.
"""

from __future__ import annotations

import os

import pytest

from tracebloc_ingestor import file_transfer
from tracebloc_ingestor.config import Config


def _cfg(src, storage, table="tbl"):
    """Config pointing SRC_PATH at ``src`` and DEST at ``storage/<table>``.

    STORAGE_PATH is a class-level constant (not env-driven), so shadow it on
    the instance — ``DEST_PATH`` reads ``self.STORAGE_PATH``.
    """
    cfg = Config(SRC_PATH=str(src), TABLE_NAME=table)
    cfg.STORAGE_PATH = str(storage)
    return cfg


def _seed(root, subdir, name="cat.jpg", content=b"data"):
    d = os.path.join(str(root), subdir)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, name), "wb") as fh:
        fh.write(content)


# ---------------------------------------------------------------------------
# happy path — the real staging layout (SharedRoot/.tracebloc-staging/<table>)
# ---------------------------------------------------------------------------


def test_reclaims_staging_dir_after_verified_load(tmp_path):
    storage = tmp_path / "shared"
    src = storage / ".tracebloc-staging" / "tbl"  # isolated staging copy
    dest = storage / "tbl"  # the table dir the ingest just wrote
    src.mkdir(parents=True)
    dest.mkdir(parents=True)
    _seed(src, "images")
    (dest / "cat.jpg").write_bytes(b"data")  # the copied-in table file

    cfg = _cfg(src, storage)
    assert file_transfer.reclaim_source(cfg) is True
    assert not src.exists()  # staging copy reclaimed
    assert dest.exists() and (dest / "cat.jpg").exists()  # table untouched


# ---------------------------------------------------------------------------
# guards — every path that must NOT delete
# ---------------------------------------------------------------------------


def test_skips_when_src_path_unset(tmp_path):
    cfg = Config(SRC_PATH="", TABLE_NAME="tbl")
    cfg.STORAGE_PATH = str(tmp_path)
    assert file_transfer.reclaim_source(cfg) is False


def test_skips_when_src_missing(tmp_path):
    storage = tmp_path / "shared"
    storage.mkdir()
    cfg = _cfg(tmp_path / "ghost-staging", storage)
    assert file_transfer.reclaim_source(cfg) is False


def test_refuses_when_src_equals_dest(tmp_path):
    storage = tmp_path / "shared"
    dest = storage / "tbl"
    dest.mkdir(parents=True)
    _seed(dest, "images")
    # SRC_PATH set to the table dir itself.
    cfg = _cfg(dest, storage)
    assert file_transfer.reclaim_source(cfg) is False
    assert dest.exists()


def test_refuses_when_dest_lives_inside_src(tmp_path):
    # Helm-direct layout: data staged straight under the storage root, so
    # SRC (a parent) CONTAINS the table dir. Deleting SRC would take the
    # table (and siblings) with it — must skip.
    storage = tmp_path / "data" / "shared"
    storage.mkdir(parents=True)
    src = tmp_path / "data"  # parent of storage -> contains DEST
    dest = storage / "tbl"
    dest.mkdir()
    _seed(dest, "images")
    cfg = _cfg(src, storage)
    assert file_transfer.reclaim_source(cfg) is False
    assert src.exists() and dest.exists()


def test_refuses_when_src_lives_inside_dest(tmp_path):
    # Degenerate: SRC nested under the table dir itself. Skip rather than
    # delete a subtree of the freshly-written table.
    storage = tmp_path / "shared"
    dest = storage / "tbl"
    src = dest / "substaging"
    src.mkdir(parents=True)
    _seed(src, "images")
    cfg = _cfg(src, storage)
    assert file_transfer.reclaim_source(cfg) is False
    assert src.exists()


def test_refuses_when_src_is_storage_root(tmp_path):
    storage = tmp_path / "shared"
    storage.mkdir()
    cfg = _cfg(storage, storage)  # SRC == STORAGE_PATH (shared-PVC root)
    assert file_transfer.reclaim_source(cfg) is False
    assert storage.exists()


def test_best_effort_swallows_rmtree_error(tmp_path, monkeypatch):
    # A filesystem error while removing the staging tree must never fail an
    # already-successful load: log + return False, don't raise.
    storage = tmp_path / "shared"
    src = storage / ".tracebloc-staging" / "tbl"
    src.mkdir(parents=True)
    _seed(src, "images")

    def boom(*_a, **_k):
        raise OSError("read-only filesystem")

    monkeypatch.setattr(file_transfer.shutil, "rmtree", boom)
    cfg = _cfg(src, storage)
    assert file_transfer.reclaim_source(cfg) is False
    assert src.exists()  # untouched because the delete failed


def test_default_cfg_falls_back_to_module_config(tmp_path, monkeypatch):
    # Called with no cfg -> uses the module-global ``config`` (env-driven),
    # mirroring the other file_transfer helpers.
    storage = tmp_path / "shared"
    src = storage / ".tracebloc-staging" / "tbl"
    src.mkdir(parents=True)
    _seed(src, "images")
    monkeypatch.setenv("SRC_PATH", str(src))
    monkeypatch.setenv("TABLE_NAME", "tbl")
    monkeypatch.setattr(file_transfer.config, "STORAGE_PATH", str(storage))
    assert file_transfer.reclaim_source() is True
    assert not src.exists()
