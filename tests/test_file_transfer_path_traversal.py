"""Security: path traversal via the manifest's filename / mask_id columns (#239).

Those per-row values come straight from the user's CSV/JSON manifest and used
to flow unsanitised into ``os.path.join`` on both the read (SRC_PATH) and write
(DEST_PATH) sides. A crafted value — an absolute path or ``..`` traversal —
escaped both sandboxes:

  - read: a file outside SRC_PATH was resolved and copied INTO the dataset
    (exfiltration of another tenant's data / a pod-mounted secret), and
  - write: the copy destination landed outside DEST_PATH (arbitrary write,
    e.g. ``/etc/cron.d/evil``) on the shared cluster PVC.

``_safe_join`` now rejects any join that resolves outside its root. These tests
demonstrate each primitive is blocked and that the legitimate basename path is
unchanged.
"""

from __future__ import annotations

import os

import pytest

from tracebloc_ingestor import file_transfer
from tracebloc_ingestor.utils.constants import TaskCategory


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    """Point file_transfer's Config at tmp src + storage dirs, and plant a
    'secret' file OUTSIDE both sandboxes."""
    src = tmp_path / "src"
    storage = tmp_path / "storage"
    src.mkdir()
    storage.mkdir()
    monkeypatch.setenv("SRC_PATH", str(src))
    monkeypatch.setenv("TABLE_NAME", "tbl")
    monkeypatch.setattr(file_transfer.config, "STORAGE_PATH", str(storage))
    secret = tmp_path / "SECRET.txt"
    secret.write_text("top secret")
    return src, storage / "tbl", secret


# ── _safe_join unit ─────────────────────────────────────────────────────────


def test_safe_join_allows_basename(tmp_path):
    root = str(tmp_path)
    assert file_transfer._safe_join(root, "images", "cat.jpg") == os.path.abspath(
        os.path.join(root, "images", "cat.jpg")
    )


@pytest.mark.parametrize(
    "evil",
    ["../evil", "../../etc/passwd", "/etc/passwd", "a/../../evil", "../../../x"],
)
def test_safe_join_rejects_escape(tmp_path, evil):
    with pytest.raises(ValueError, match="escapes"):
        file_transfer._safe_join(str(tmp_path), evil)


# ── read primitive: _find_src / _find_mask_src ──────────────────────────────


def test_find_src_rejects_absolute_filename(dirs):
    _, _, secret = dirs
    with pytest.raises(ValueError, match="escapes"):
        file_transfer._find_src("images", str(secret), ".txt")


def test_find_src_rejects_traversal_filename(dirs):
    with pytest.raises(ValueError, match="escapes"):
        file_transfer._find_src("images", "../../SECRET", ".txt")


def test_find_src_allows_legit_missing(dirs):
    # A normal basename that simply doesn't exist -> (None, fname), a tolerated
    # skip — NOT a raise. The traversal guard must not turn missing files into
    # hard errors.
    path, fname = file_transfer._find_src("images", "cat", ".jpg")
    assert path is None and fname == "cat.jpg"


def test_find_mask_src_rejects_absolute(dirs):
    # An absolute mask_id (no leading '.') survives the extension-split and
    # reaches _safe_join, which rejects it for escaping masks/.
    with pytest.raises(ValueError, match="escapes"):
        file_transfer._find_mask_src("/etc/hostname")


def test_find_mask_src_traversal_does_not_escape(dirs):
    # A dotted '..' mask_id is neutralised by the extension-split
    # (mask_id.split(".")[0] collapses "../../SECRET" to ""), so it resolves to
    # a missing file inside the sandbox -> (None, ...), never to a path outside
    # masks/. Either way no file outside the sandbox is reached; _safe_join is
    # the backstop if the split ever lets a real traversal through.
    src_path, ext, _ = file_transfer._find_mask_src("../../SECRET")
    assert src_path is None


# ── write primitive: image_transfer / text_transfer DEST ────────────────────


def test_image_transfer_rejects_traversal_dest(dirs):
    src, dest, secret = dirs
    # src_path provided (atomic-branch style) so we reach the DEST join with a
    # crafted filename_with_ext; the write target must not escape DEST.
    with pytest.raises(ValueError):
        file_transfer.image_transfer(
            {"filename": "x"},
            {"extension": ".jpg"},
            src_path=str(secret),
            filename_with_ext="../../PWNED.jpg",
        )
    # Nothing was written outside the dataset directory.
    assert not (dest.parent.parent / "PWNED.jpg").exists()


def test_text_transfer_rejects_traversal(dirs):
    with pytest.raises(ValueError):
        file_transfer.text_transfer({"filename": "../../PWNED"}, {"extension": ".txt"})


# ── end-to-end via map_file_transfer ────────────────────────────────────────


def test_map_file_transfer_rejects_absolute_filename(dirs):
    # image_classification -> image_transfer -> _find_src raises on the absolute
    # path. (The base ingest loop catches this per-record as a failure, so the
    # record is rejected + counted, not silently ingested.)
    with pytest.raises(ValueError):
        file_transfer.map_file_transfer(
            TaskCategory.IMAGE_CLASSIFICATION,
            {"filename": "/etc/hostname"},
            {"extension": ".jpg"},
        )


# ── the legitimate path is unchanged ────────────────────────────────────────


def test_legit_basename_copies_inside_dest(dirs):
    src, dest, _ = dirs
    (src / "images").mkdir()
    (src / "images" / "cat.jpg").write_bytes(b"img")

    rec = file_transfer.image_transfer({"filename": "cat"}, {"extension": ".jpg"})

    assert rec is not None
    assert (dest / "cat.jpg").exists()  # copied INSIDE the dataset directory
