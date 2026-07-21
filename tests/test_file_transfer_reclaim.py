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


# ---------------------------------------------------------------------------
# guards added for the Bugbot review (PR #381): sibling user dir, symlink
# escape, and a root SRC_PATH — none of which the original blocklist caught.
# ---------------------------------------------------------------------------


def test_refuses_sibling_user_dataset_dir(tmp_path):
    # Helm layout: _resolve_config sets SRC_PATH to the parent of images:, e.g.
    # /data/shared/cats-dogs — a SIBLING of the table dir, and the user's OWN
    # data. It is not an isolated .tracebloc-staging dir, so reclaim must leave
    # it alone (the original guards deleted it on a clean load — the #381 bug).
    storage = tmp_path / "shared"
    src = storage / "cats-dogs"  # user's mounted dataset dir
    dest = storage / "cats_dogs_train"  # the table dir (sibling)
    src.mkdir(parents=True)
    dest.mkdir(parents=True)
    _seed(src, "images")
    cfg = _cfg(src, storage, table="cats_dogs_train")
    assert file_transfer.reclaim_source(cfg) is False
    assert src.exists() and (src / "images" / "cat.jpg").exists()


def test_refuses_symlinked_src_resolving_outside_staging(tmp_path):
    # A SRC_PATH whose path looks like a staging dir but is a SYMLINK to the
    # user's data must not be reclaimed: guards resolve symlinks (realpath) so
    # the real target is judged, not the innocent-looking link path.
    storage = tmp_path / "shared"
    user_data = storage / "cats-dogs"
    user_data.mkdir(parents=True)
    _seed(user_data, "images")
    staging = storage / ".tracebloc-staging"
    staging.mkdir(parents=True)
    link = staging / "tbl"  # path is under .tracebloc-staging …
    os.symlink(user_data, link)  # … but resolves to the user's data
    cfg = _cfg(link, storage)
    assert file_transfer.reclaim_source(cfg) is False
    assert user_data.exists() and (user_data / "images" / "cat.jpg").exists()


def test_refuses_root_src(tmp_path):
    # A truthy SRC_PATH of "/" must never reach rmtree: it is outside the
    # staging subtree, so the opt-in gate skips it (a string-prefix guard
    # mishandled the "//" root case — the #381 medium finding).
    storage = tmp_path / "shared"
    storage.mkdir()
    cfg = _cfg("/", storage)
    assert file_transfer.reclaim_source(cfg) is False


def test_reclaims_through_symlinked_storage_mount(tmp_path):
    # Realistic PVC: STORAGE_PATH is a symlink to the real mount. A legit
    # staging dir under it must STILL be reclaimed (realpath resolves both
    # sides consistently) — the symlink handling must not over-skip.
    real_storage = tmp_path / "real-shared"
    real_src = real_storage / ".tracebloc-staging" / "tbl"
    real_src.mkdir(parents=True)
    _seed(real_src, "images")
    storage_link = tmp_path / "shared"  # symlink -> real-shared
    os.symlink(real_storage, storage_link)
    cfg = _cfg(storage_link / ".tracebloc-staging" / "tbl", storage_link)
    assert file_transfer.reclaim_source(cfg) is True
    assert not real_src.exists()


# ---------------------------------------------------------------------------
# guard added for the 2nd Bugbot round (PR #381): reclaim must be bound to
# THIS table's staging dir, not just "somewhere under .tracebloc-staging".
# ---------------------------------------------------------------------------


def test_refuses_another_tables_staging_dir(tmp_path):
    # SRC_PATH points at a DIFFERENT table's staging dir under the same
    # .tracebloc-staging tree. Deleting it would take a sibling dataset's
    # staging with it — must skip (gate binds to cfg.TABLE_NAME).
    storage = tmp_path / "shared"
    other = storage / ".tracebloc-staging" / "other_table"
    other.mkdir(parents=True)
    _seed(other, "images")
    cfg = _cfg(other, storage, table="tbl")  # our table is 'tbl', not 'other_table'
    assert file_transfer.reclaim_source(cfg) is False
    assert other.exists() and (other / "images" / "cat.jpg").exists()


def test_refuses_symlink_into_another_tables_staging(tmp_path):
    # Our per-table path is a symlink that realpaths to ANOTHER table's staging
    # under the same tree. The literal-path equality (src resolves elsewhere)
    # skips it — the sibling's data survives.
    storage = tmp_path / "shared"
    other = storage / ".tracebloc-staging" / "other_table"
    other.mkdir(parents=True)
    _seed(other, "images")
    link = storage / ".tracebloc-staging" / "tbl"
    os.symlink(other, link)
    cfg = _cfg(link, storage, table="tbl")
    assert file_transfer.reclaim_source(cfg) is False
    assert other.exists() and (other / "images" / "cat.jpg").exists()


def test_skips_when_table_name_unset(tmp_path):
    # A well-formed staging dir but no TABLE_NAME to bind it to → skip (we can't
    # prove which table this staging belongs to).
    storage = tmp_path / "shared"
    src = storage / ".tracebloc-staging" / "tbl"
    src.mkdir(parents=True)
    _seed(src, "images")
    cfg = _cfg(src, storage, table="")
    assert file_transfer.reclaim_source(cfg) is False
    assert src.exists()


# ---------------------------------------------------------------------------
# best-effort contract (3rd Bugbot round, PR #381): reclaim runs AFTER the load
# is durable + registered, so NO error in it — guards, realpath, or logging,
# not just rmtree — may propagate and turn a green ingest red.
# ---------------------------------------------------------------------------


def test_internal_error_never_fails_the_ingest(tmp_path, monkeypatch):
    # An unexpected raise in a guard (here _is_within) must be swallowed, not
    # propagated — the delete never happens and the load stays green.
    storage = tmp_path / "shared"
    src = storage / ".tracebloc-staging" / "tbl"
    src.mkdir(parents=True)
    _seed(src, "images")

    def boom(*_a, **_k):
        raise RuntimeError("unexpected guard failure")

    monkeypatch.setattr(file_transfer, "_is_within", boom)
    cfg = _cfg(src, storage)
    assert file_transfer.reclaim_source(cfg) is False  # must NOT raise
    assert src.exists()  # never reached rmtree


def test_logging_failure_never_fails_the_ingest(tmp_path, monkeypatch):
    # Even a broken logger inside reclaim must not propagate (Bugbot flagged
    # logging as a raise path). A skip-path log that throws is swallowed.
    storage = tmp_path / "shared"
    other = storage / ".tracebloc-staging" / "other"
    other.mkdir(parents=True)
    _seed(other, "images")

    def boom(*_a, **_k):
        raise RuntimeError("logger down")

    monkeypatch.setattr(file_transfer.logger, "info", boom)
    cfg = _cfg(other, storage, table="tbl")  # mismatch → gate logs .info → boom
    assert file_transfer.reclaim_source(cfg) is False  # must NOT raise
    assert other.exists()
