"""Tests for ImageResolutionValidator's validate() flow and helpers.

The resolution-matching unit tests live in test_image_validator_resolution.py;
this file covers the file-discovery + uniformity flow, which reads images from
``config.SRC_PATH/images``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image

from tracebloc_ingestor.validators.image_validator import ImageResolutionValidator


@pytest.fixture
def images_dir(tmp_path):
    """Return a factory that creates <tmp>/images/<name> at a given size."""
    d = tmp_path / "images"
    d.mkdir()

    def _add(name, size=(64, 64), color=(120, 120, 120)):
        Image.new("RGB", size, color).save(d / name)
        return d / name

    return tmp_path, _add


def test_uniform_images_pass(clean_env, images_dir):
    src, add = images_dir
    add("a.jpg", (64, 64))
    add("b.jpg", (64, 64))
    clean_env.setenv("SRC_PATH", str(src))
    result = ImageResolutionValidator().validate(None)
    assert result.is_valid, result.errors
    assert result.metadata["files_checked"] == 2


def test_mixed_resolutions_fail(clean_env, images_dir):
    src, add = images_dir
    add("a.jpg", (64, 64))
    add("b.jpg", (32, 32))
    clean_env.setenv("SRC_PATH", str(src))
    result = ImageResolutionValidator().validate(None)
    assert not result.is_valid
    assert any("Multiple image resolutions" in e for e in result.errors)


def test_no_images_fails(clean_env, tmp_path):
    (tmp_path / "images").mkdir()
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = ImageResolutionValidator().validate(None)
    assert not result.is_valid
    assert "No image files found" in result.errors[0]


def test_explicit_resolution_mismatch_fails(clean_env, images_dir):
    src, add = images_dir
    add("a.jpg", (64, 64))
    clean_env.setenv("SRC_PATH", str(src))
    result = ImageResolutionValidator(expected_resolution=(128, 128)).validate(None)
    assert not result.is_valid
    assert any("incorrect resolution" in e for e in result.errors)


def test_nonexistent_src_path_fails(clean_env):
    clean_env.setenv("SRC_PATH", "/no/such/dir")
    result = ImageResolutionValidator().validate(None)
    assert not result.is_valid


# ---- helpers --------------------------------------------------------------

def test_is_image_file():
    v = ImageResolutionValidator()
    assert v._is_image_file(Path("a.JPG"))
    assert v._is_image_file(Path("a.png"))
    assert not v._is_image_file(Path("a.txt"))


def test_get_image_files_single(tmp_path):
    p = tmp_path / "a.jpg"
    Image.new("RGB", (10, 10)).save(p)
    v = ImageResolutionValidator()
    assert v._get_image_files(str(p), True, True) == [p]


def test_get_image_files_list(tmp_path):
    p = tmp_path / "a.png"
    Image.new("RGB", (10, 10)).save(p)
    v = ImageResolutionValidator()
    files = v._get_image_files([str(p), str(tmp_path / "b.txt")], True, True)
    assert files == [p]


def test_get_image_files_unsupported_type_raises():
    with pytest.raises(ValueError):
        ImageResolutionValidator()._get_image_files(123, True, True)


def test_get_image_files_missing_path_raises(tmp_path):
    with pytest.raises(ValueError):
        ImageResolutionValidator()._get_image_files(str(tmp_path / "nope"), True, True)


def test_get_image_resolution_bad_file(tmp_path):
    bad = tmp_path / "a.jpg"
    bad.write_text("not an image")
    assert ImageResolutionValidator()._get_image_resolution(bad) is None


def test_validate_image_resolutions_empty_list():
    res = ImageResolutionValidator()._validate_image_resolutions([])
    assert not res.is_valid


def test_validate_image_resolutions_unprocessable(tmp_path):
    bad = tmp_path / "a.jpg"
    bad.write_text("nope")
    res = ImageResolutionValidator(expected_resolution=(10, 10))._validate_image_resolutions([bad])
    assert not res.is_valid
    assert any("could not be processed" in e for e in res.errors)
