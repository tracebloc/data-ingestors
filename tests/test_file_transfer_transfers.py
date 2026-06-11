"""Tests for the file_transfer copy functions and map_file_transfer routing.

The 'missing source returns None' paths are covered in
test_file_transfer_summary.py; here we cover the successful-copy paths,
source resolution helpers, and the multi-file map_file_transfer branches.
"""

from __future__ import annotations

import os

import pytest

from tracebloc_ingestor import file_transfer
from tracebloc_ingestor.utils.constants import TaskCategory


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    """Point file_transfer's Config at tmp src + storage dirs.

    SRC_PATH is env-backed; DEST_PATH derives from STORAGE_PATH / TABLE_NAME,
    so override STORAGE_PATH on the live module-level Config and set TABLE_NAME.
    """
    src = tmp_path / "src"
    storage = tmp_path / "storage"
    src.mkdir()
    storage.mkdir()
    monkeypatch.setenv("SRC_PATH", str(src))
    monkeypatch.setenv("TABLE_NAME", "tbl")
    monkeypatch.setattr(file_transfer.config, "STORAGE_PATH", str(storage))
    dest = storage / "tbl"
    return src, dest


def _seed(src, subdir, name, content=b"data"):
    d = src / subdir
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_bytes(content)
    return d / name


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def test_find_src_with_extension(dirs):
    src, _ = dirs
    _seed(src, "images", "cat.jpg")
    path, fname = file_transfer._find_src("images", "cat.jpg", ".jpg")
    assert path is not None and fname == "cat.jpg"


def test_find_src_appends_extension(dirs):
    src, _ = dirs
    _seed(src, "images", "cat.jpg")
    path, fname = file_transfer._find_src("images", "cat", ".jpg")
    assert path is not None and fname == "cat.jpg"


def test_find_src_missing(dirs):
    path, fname = file_transfer._find_src("images", "ghost", ".jpg")
    assert path is None and fname == "ghost.jpg"


def test_find_mask_src_tries_extensions(dirs):
    src, _ = dirs
    _seed(src, "masks", "m1.png")
    path, ext, name = file_transfer._find_mask_src("m1")
    assert path is not None and ext == ".png" and name == "m1"


def test_find_mask_src_missing(dirs):
    path, ext, name = file_transfer._find_mask_src("nope")
    assert path is None and ext is None and name == "nope"


def test_copy_file_with_retry_overwrites(dirs, tmp_path):
    src, dest = dirs
    s = _seed(src, "images", "a.jpg", b"new")
    dest.mkdir(parents=True, exist_ok=True)
    target = dest / "a.jpg"
    target.write_bytes(b"old")
    file_transfer._copy_file_with_retry(str(s), str(target))
    assert target.read_bytes() == b"new"


# ---------------------------------------------------------------------------
# image_transfer / text_transfer / annotation / mask success
# ---------------------------------------------------------------------------


def test_image_transfer_success(dirs):
    src, dest = dirs
    _seed(src, "images", "cat.jpg")
    rec = file_transfer.image_transfer({"filename": "cat"}, {"extension": ".jpg"})
    assert rec is not None
    assert rec["filename"] == "cat"
    assert rec["extension"] == ".jpg"
    assert (dest / "cat.jpg").exists()


def test_text_transfer_success(dirs):
    src, dest = dirs
    _seed(src, "texts", "doc.txt", b"hello")
    rec = file_transfer.text_transfer({"filename": "doc"}, {"extension": ".txt"})
    assert rec is not None
    assert (dest / "doc.txt").exists()


def test_text_transfer_custom_subdir(dirs):
    src, dest = dirs
    _seed(src, "sequences", "seq.txt", b"tokens")
    rec = file_transfer.text_transfer(
        {"filename": "seq"}, {"extension": ".txt"}, src_subdir="sequences"
    )
    assert rec is not None
    assert (dest / "seq.txt").exists()


def test_text_transfer_missing_filename_returns_none(dirs):
    assert file_transfer.text_transfer({}, {"extension": ".txt"}) is None


def test_annotation_transfer_success(dirs):
    src, dest = dirs
    s = _seed(src, "annotations", "img.xml", b"<x/>")
    rec = file_transfer.annotation_transfer(
        {"filename": "img"}, {}, ".xml", str(s), "img.xml"
    )
    assert rec is not None
    assert (dest / "img.xml").exists()


def test_mask_transfer_success(dirs):
    src, dest = dirs
    s = _seed(src, "masks", "m.png")
    rec = file_transfer.mask_transfer({"filename": "x"}, str(s), ".png", "m")
    assert rec is not None
    assert (dest / "m.png").exists()


# ---------------------------------------------------------------------------
# map_file_transfer routing
# ---------------------------------------------------------------------------


def test_map_image_classification(dirs):
    src, dest = dirs
    _seed(src, "images", "cat.jpg")
    rec = file_transfer.map_file_transfer(
        TaskCategory.IMAGE_CLASSIFICATION, {"filename": "cat"}, {"extension": ".jpg"}
    )
    assert rec is not None


def test_map_object_detection_atomic_success(dirs):
    src, dest = dirs
    _seed(src, "images", "x.jpg")
    _seed(src, "annotations", "x.xml", b"<a/>")
    rec = file_transfer.map_file_transfer(
        TaskCategory.OBJECT_DETECTION, {"filename": "x"}, {"extension": ".jpg"}
    )
    assert rec is not None
    assert (dest / "x.jpg").exists()
    assert (dest / "x.xml").exists()


def test_map_object_detection_missing_annotation_returns_none(dirs):
    src, _ = dirs
    _seed(src, "images", "x.jpg")  # no annotation
    rec = file_transfer.map_file_transfer(
        TaskCategory.OBJECT_DETECTION, {"filename": "x"}, {"extension": ".jpg"}
    )
    assert rec is None


def test_map_object_detection_missing_filename_returns_none(dirs):
    rec = file_transfer.map_file_transfer(
        TaskCategory.OBJECT_DETECTION, {}, {"extension": ".jpg"}
    )
    assert rec is None


def test_map_semantic_segmentation_success(dirs):
    src, dest = dirs
    _seed(src, "images", "x.jpg")
    _seed(src, "masks", "m.png")
    rec = file_transfer.map_file_transfer(
        TaskCategory.SEMANTIC_SEGMENTATION,
        {"filename": "x", "mask_id": "m"},
        {"extension": ".jpg"},
    )
    assert rec is not None
    assert (dest / "m.png").exists()


def test_map_semantic_segmentation_missing_mask_id_returns_none(dirs):
    src, _ = dirs
    _seed(src, "images", "x.jpg")
    rec = file_transfer.map_file_transfer(
        TaskCategory.SEMANTIC_SEGMENTATION, {"filename": "x"}, {"extension": ".jpg"}
    )
    assert rec is None


def test_map_mlm_copies_tokenizer(dirs):
    src, dest = dirs
    _seed(src, "sequences", "seq.txt", b"tokens")
    (src / "tokenizer.json").write_text("{}")
    rec = file_transfer.map_file_transfer(
        TaskCategory.MASKED_LANGUAGE_MODELING,
        {"filename": "seq"},
        {"extension": ".txt"},
    )
    assert rec is not None
    assert (dest / "tokenizer.json").exists()


def test_map_token_classification(dirs):
    src, dest = dirs
    _seed(src, "texts", "doc.txt", b"John Smith")
    rec = file_transfer.map_file_transfer(
        TaskCategory.TOKEN_CLASSIFICATION, {"filename": "doc"}, {"extension": ".txt"}
    )
    assert rec is not None
    assert (dest / "doc.txt").exists()


def test_map_keypoint_detection(dirs):
    src, dest = dirs
    _seed(src, "images", "p.jpg")
    rec = file_transfer.map_file_transfer(
        TaskCategory.KEYPOINT_DETECTION, {"filename": "p"}, {"extension": ".jpg"}
    )
    assert rec is not None


def test_map_unknown_category_returns_none(dirs):
    assert file_transfer.map_file_transfer("weird", {"filename": "x"}, {}) is None


def test_map_text_classification_copies_optional_tokenizer(dirs):
    src, dest = dirs
    _seed(src, "texts", "doc.txt", b"hello world")
    (src / "tokenizer.json").write_text("{}")
    rec = file_transfer.map_file_transfer(
        TaskCategory.TEXT_CLASSIFICATION, {"filename": "doc"}, {"extension": ".txt"}
    )
    assert rec is not None
    assert (dest / "tokenizer.json").exists()


def test_map_token_classification_copies_optional_tokenizer(dirs):
    src, dest = dirs
    _seed(src, "texts", "doc.txt", b"John Smith")
    (src / "tokenizer.json").write_text("{}")
    rec = file_transfer.map_file_transfer(
        TaskCategory.TOKEN_CLASSIFICATION, {"filename": "doc"}, {"extension": ".txt"}
    )
    assert rec is not None
    assert (dest / "tokenizer.json").exists()


def test_map_token_classification_without_tokenizer_is_fine(dirs):
    src, dest = dirs
    _seed(src, "texts", "doc.txt", b"John Smith")  # no tokenizer.json
    rec = file_transfer.map_file_transfer(
        TaskCategory.TOKEN_CLASSIFICATION, {"filename": "doc"}, {"extension": ".txt"}
    )
    assert rec is not None
    assert (dest / "doc.txt").exists()
    assert not (dest / "tokenizer.json").exists()
