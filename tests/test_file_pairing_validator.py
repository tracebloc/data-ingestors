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


# ---------------------------------------------------------------------------
# #196 — semantic_segmentation _mask suffix convention
# ---------------------------------------------------------------------------

def test_pairing_mask_suffix_all_matched(clean_env, tmp_path):
    # Documented + shipped convention: image_001.jpg <-> image_001_mask.png.
    # Plain stem comparison treated them as unrelated, so the shipped
    # template sample failed pairing out-of-box (#196).
    _setup(
        tmp_path,
        ["image_001.jpg", "image_002.jpg", "image_003.jpg"],
        ["image_001_mask.png", "image_002_mask.png", "image_003_mask.png"],
        sidecar_dir="masks",
    )
    clean_env.setenv("SRC_PATH", str(tmp_path))
    res = FilePairingValidator(
        sidecar_path="masks", sidecar_label="mask", sidecar_suffix="_mask"
    ).validate(None)
    assert res.is_valid, f"errors={res.errors}"


def test_pairing_mask_suffix_image_without_mask(clean_env, tmp_path):
    # image_002 has no corresponding _mask file → flagged as missing.
    _setup(
        tmp_path,
        ["image_001.jpg", "image_002.jpg"],
        ["image_001_mask.png"],
        sidecar_dir="masks",
    )
    clean_env.setenv("SRC_PATH", str(tmp_path))
    res = FilePairingValidator(
        sidecar_path="masks", sidecar_label="mask", sidecar_suffix="_mask"
    ).validate(None)
    assert not res.is_valid
    assert "no matching mask" in res.errors[0]
    assert "image_002" in res.errors[0]


def test_pairing_mask_suffix_orphan_mask(clean_env, tmp_path):
    # ghost_mask.png has no corresponding image → flagged as orphan
    # (compared by stripped stem, "ghost", not in images).
    _setup(
        tmp_path,
        ["image_001.jpg"],
        ["image_001_mask.png", "ghost_mask.png"],
        sidecar_dir="masks",
    )
    clean_env.setenv("SRC_PATH", str(tmp_path))
    res = FilePairingValidator(
        sidecar_path="masks", sidecar_label="mask", sidecar_suffix="_mask"
    ).validate(None)
    assert not res.is_valid
    assert any("no matching image" in e for e in res.errors)
    assert any("ghost" in e for e in res.errors)


def test_pairing_mask_suffix_flags_non_conforming_filename(clean_env, tmp_path):
    # A mask that doesn't follow the `_mask` convention at all (e.g. someone
    # dropped a plain `image_001.png` into the masks/ folder) is reported
    # as an orphan rather than silently accepted — naming the convention
    # consistently matters because the training-time mask-loader will look
    # for the suffix.
    _setup(
        tmp_path,
        ["image_001.jpg"],
        ["image_001.png"],  # missing the _mask suffix
        sidecar_dir="masks",
    )
    clean_env.setenv("SRC_PATH", str(tmp_path))
    res = FilePairingValidator(
        sidecar_path="masks", sidecar_label="mask", sidecar_suffix="_mask"
    ).validate(None)
    assert not res.is_valid
    assert any("image_001" in e and "no matching image" in e for e in res.errors)


def test_pairing_no_suffix_object_detection_unchanged(clean_env, tmp_path):
    # Object detection's plain-stem pairing must NOT regress: a.jpg pairs
    # with a.xml, no suffix involved.
    _setup(tmp_path, ["a.jpg", "b.jpg"], ["a.xml", "b.xml"])
    clean_env.setenv("SRC_PATH", str(tmp_path))
    # default sidecar_suffix="" preserves the old behaviour.
    assert FilePairingValidator().validate(None).is_valid
