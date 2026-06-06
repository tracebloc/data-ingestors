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
        {"filename": "x", "mask_id": "m"}, {"extension": ".jpg"},
    )
    assert rec is None
    assert not dest.exists() or not any(dest.iterdir())
