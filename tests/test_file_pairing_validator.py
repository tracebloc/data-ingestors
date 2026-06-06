"""Tests for FilePairingValidator — image <-> sidecar (annotation/mask) pairing.

The validator reads directories under ``config.SRC_PATH`` (``<src>/images`` and
``<src>/annotations`` or ``<src>/masks``), matching by filename stem.
"""

from __future__ import annotations

from tracebloc_ingestor.validators.file_pairing_validator import FilePairingValidator


def _setup(tmp_path, images, sidecars, sidecar_dir="annotations"):
    (tmp_path / "images").mkdir()
    (tmp_path / sidecar_dir).mkdir()
    for n in images:
        (tmp_path / "images" / n).write_text("x")
    for n in sidecars:
        (tmp_path / sidecar_dir / n).write_text("x")


def test_pairing_all_matched(clean_env, tmp_path):
    _setup(tmp_path, ["a.jpg", "b.jpg"], ["a.xml", "b.xml"])
    clean_env.setenv("SRC_PATH", str(tmp_path))
    assert FilePairingValidator().validate(None).is_valid


def test_pairing_image_without_sidecar(clean_env, tmp_path):
    _setup(tmp_path, ["a.jpg", "b.jpg"], ["a.xml"])  # b.jpg has no annotation
    clean_env.setenv("SRC_PATH", str(tmp_path))
    res = FilePairingValidator().validate(None)
    assert not res.is_valid
    assert "no matching annotation" in res.errors[0]
    assert "b" in res.errors[0]


def test_pairing_orphan_sidecar(clean_env, tmp_path):
    _setup(tmp_path, ["a.jpg"], ["a.xml", "ghost.xml"])  # ghost.xml has no image
    clean_env.setenv("SRC_PATH", str(tmp_path))
    res = FilePairingValidator().validate(None)
    assert not res.is_valid
    assert any("no matching image" in e for e in res.errors)
    assert any("ghost" in e for e in res.errors)


def test_pairing_masks_label(clean_env, tmp_path):
    _setup(tmp_path, ["a.jpg"], ["b.png"], sidecar_dir="masks")
    clean_env.setenv("SRC_PATH", str(tmp_path))
    res = FilePairingValidator(sidecar_path="masks", sidecar_label="mask").validate(None)
    assert not res.is_valid
    assert any("mask" in e for e in res.errors)


def test_pairing_skips_when_sidecar_dir_missing(clean_env, tmp_path):
    # A missing directory is the FileTypeValidator's concern — don't double-report.
    (tmp_path / "images").mkdir()
    clean_env.setenv("SRC_PATH", str(tmp_path))
    assert FilePairingValidator().validate(None).is_valid
