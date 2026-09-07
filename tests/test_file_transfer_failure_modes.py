"""file_transfer failure-mode tests: the retry, permission, and atomicity paths.

The happy-path + missing-source tests live in ``test_file_transfer_transfers.py``
and ``test_file_transfer_summary.py``. Those copy files that succeed on the first
try, so the tenacity retry on ``_copy_file_with_retry`` (RETRY_MAX_ATTEMPTS, retry
on OSError) never fires, the permission-denied path never runs, and the "atomic
skip leaves no orphan" guarantee is only asserted as ``rec is None``. These cover
exactly those gaps.
"""

from __future__ import annotations

import os

import pytest
import tenacity

from tracebloc_ingestor import file_transfer
from tracebloc_ingestor.utils.constants import TaskCategory, RETRY_MAX_ATTEMPTS


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    """Point file_transfer's module-level Config at tmp src + storage dirs.
    SRC_PATH is env-backed; DEST_PATH derives from STORAGE_PATH / TABLE_NAME."""
    src = tmp_path / "src"
    storage = tmp_path / "storage"
    src.mkdir()
    storage.mkdir()
    monkeypatch.setenv("SRC_PATH", str(src))
    monkeypatch.setenv("TABLE_NAME", "tbl")
    monkeypatch.setattr(file_transfer.config, "STORAGE_PATH", str(storage))
    return src, storage / "tbl"


@pytest.fixture
def no_retry_wait(monkeypatch):
    """Neutralise the exponential backoff so retry tests run instantly."""
    monkeypatch.setattr(
        file_transfer._copy_file_with_retry.retry, "wait", tenacity.wait_none()
    )


def _seed(src, subdir, name, content=b"data"):
    d = src / subdir
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_bytes(content)
    return d / name


def _raise_oserror(*_a, **_k):
    raise OSError("disk error")


# --- retry behaviour (tenacity on _copy_file_with_retry) --------------------


def test_copy_retries_transient_error_then_succeeds(dirs, no_retry_wait, monkeypatch):
    src, dest = dirs
    s = _seed(src, "images", "a.jpg", b"payload")
    dest.mkdir(parents=True, exist_ok=True)
    target = dest / "a.jpg"

    real_copy = file_transfer.shutil.copy
    calls = {"n": 0}

    def flaky(src_, dst_):
        calls["n"] += 1
        if calls["n"] < RETRY_MAX_ATTEMPTS:  # fail until the final attempt
            raise OSError("transient")
        return real_copy(src_, dst_)

    monkeypatch.setattr(file_transfer.shutil, "copy", flaky)
    file_transfer._copy_file_with_retry(str(s), str(target))
    assert calls["n"] == RETRY_MAX_ATTEMPTS
    assert target.read_bytes() == b"payload"


def test_copy_reraises_after_exhausting_retries(dirs, no_retry_wait, monkeypatch):
    src, dest = dirs
    s = _seed(src, "images", "a.jpg")
    dest.mkdir(parents=True, exist_ok=True)
    calls = {"n": 0}

    def always_fail(*_a):
        calls["n"] += 1
        raise OSError("persistent")

    monkeypatch.setattr(file_transfer.shutil, "copy", always_fail)
    with pytest.raises(OSError):  # reraise=True once the attempt cap is hit
        file_transfer._copy_file_with_retry(str(s), str(dest / "a.jpg"))
    assert calls["n"] == RETRY_MAX_ATTEMPTS


def test_image_transfer_wraps_persistent_copy_error(dirs, no_retry_wait, monkeypatch):
    src, _ = dirs
    _seed(src, "images", "cat.jpg")
    monkeypatch.setattr(file_transfer.shutil, "copy", _raise_oserror)
    with pytest.raises(ValueError, match="Error processing"):
        file_transfer.image_transfer({"filename": "cat"}, {"extension": ".jpg"})


# --- a real filesystem permission error (not a mock) ------------------------


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="root bypasses filesystem permission checks",
)
def test_image_transfer_unwritable_dest_raises(dirs, no_retry_wait):
    src, dest = dirs
    _seed(src, "images", "cat.jpg")
    dest.mkdir(parents=True, exist_ok=True)
    os.chmod(dest, 0o500)  # r-x: not writable
    try:
        with pytest.raises(ValueError):
            file_transfer.image_transfer({"filename": "cat"}, {"extension": ".jpg"})
    finally:
        os.chmod(dest, 0o700)  # restore so pytest can clean up tmp_path


# --- #172: dest dir is created group-writable + setgid for teardown ---------


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="root's created dirs ignore umask/perms nuances",
)
def test_ensure_dest_dir_new_dir_is_setgid_and_group_writable(tmp_path):
    """#172: a dir the ingestor creates is setgid + group-writable so the
    `data delete` teardown group (65532) can reclaim the tree on hostPath."""
    dest = tmp_path / "storage" / "tbl"
    file_transfer._ensure_dest_dir(str(dest))
    mode = os.stat(dest).st_mode
    assert mode & 0o2000, "setgid bit must be set so entries inherit the group"
    assert mode & 0o020, "group-write bit must be set so the group can delete"


def test_ensure_dest_dir_leaves_existing_dir_mode_untouched(tmp_path):
    """A pre-existing dest is not re-chmod'd — we don't override an operator's
    mode or mask a permission problem."""
    dest = tmp_path / "storage" / "tbl"
    dest.mkdir(parents=True)
    os.chmod(dest, 0o750)
    file_transfer._ensure_dest_dir(str(dest))
    assert (os.stat(dest).st_mode & 0o777) == 0o750


# --- atomic skip leaves no orphan (the #99 data-integrity invariant) --------


def test_object_detection_missing_annotation_leaves_no_orphan(dirs):
    src, dest = dirs
    _seed(src, "images", "x.jpg")  # image present, annotation absent
    rec = file_transfer.map_file_transfer(
        TaskCategory.OBJECT_DETECTION, {"filename": "x"}, {"extension": ".jpg"}
    )
    assert rec is None
    # the image must NOT have been copied before the missing annotation was caught
    assert not dest.exists() or not any(dest.iterdir())


def test_segmentation_missing_mask_leaves_no_orphan(dirs):
    src, dest = dirs
    _seed(src, "images", "x.jpg")  # image present, mask file absent
    rec = file_transfer.map_file_transfer(
        TaskCategory.SEMANTIC_SEGMENTATION,
        {"filename": "x", "mask_id": "m"},
        {"extension": ".jpg"},
    )
    assert rec is None
    assert not dest.exists() or not any(dest.iterdir())


# --- client#653: the teardown runs as a DIFFERENT uid, so group-write is not enough


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="root's created dirs ignore umask/perms nuances",
)
def test_ensure_dest_dir_is_world_writable_not_just_group_writable(tmp_path):
    """Removing a file needs write on its PARENT DIR, and the pods that create
    and delete these trees never share an identity: the ingestion Job runs as
    65534 (or HOST_UID), the CLI teardown pod as 65532/fsGroup 65532, and the
    chart chowns the shared mount to 1000:1000 with setgid -- so a dir created
    underneath inherits group 1000, never 65532.

    Group-write therefore could not work, and fsGroup cannot rescue it because
    kubelet ignores fsGroup on hostPath volumes (kubernetes#138411) -- the layout
    every installer-provisioned cluster uses. The observed failure was
    "rm: can't remove '/data/shared/<table>/<file>': Permission denied" with the
    table already dropped, leaving a half-deleted dataset.
    """
    dest = tmp_path / "storage" / "tbl"
    file_transfer._ensure_dest_dir(str(dest))
    mode = os.stat(dest).st_mode
    assert mode & 0o002, "other-write must be set: the teardown uid shares no group"
    assert mode & 0o001, "other-execute must be set, or the dir cannot be traversed"


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="root's created dirs ignore umask/perms nuances",
)
def test_duplicate_validator_creates_dest_reclaimable_too(tmp_path):
    """The duplicate validator creates the SAME DEST_PATH and runs BEFORE the
    transfer, so it decides the mode -- and _ensure_dest_dir deliberately does not
    re-chmod a dir that already exists. While this used a bare mkdir, the mode fix
    in file_transfer was dead code on every run that validated first: the tree was
    left at the umask default and the teardown could not remove it (client#653).
    """
    from tracebloc_ingestor.validators.duplicate_validator import DuplicateValidator

    dest = tmp_path / "storage" / "tbl"
    validator = DuplicateValidator(dest_path=str(dest))
    assert validator._create_directory_if_needed() is True
    mode = os.stat(dest).st_mode
    assert mode & 0o002, "the validator-created dir must be reclaimable as well"
    assert mode & 0o2000, "setgid, so entries keep inheriting the mount's group"


def test_both_creation_sites_share_one_mode_definition():
    """Two independent copies of this mode would drift, and the drift is invisible
    until a delete fails in the field. Both sites must resolve to the same object.
    """
    from tracebloc_ingestor.utils import fs
    from tracebloc_ingestor.validators import duplicate_validator

    assert file_transfer.DEST_DIR_MODE is fs.DEST_DIR_MODE
    assert duplicate_validator.ensure_reclaimable_dir is fs.ensure_reclaimable_dir
