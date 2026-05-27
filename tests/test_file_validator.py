"""Tests for FileTypeValidator — extension uniformity under config.SRC_PATH/<path>."""

from __future__ import annotations

from pathlib import Path

import pytest

from tracebloc_ingestor.validators.file_validator import FileTypeValidator


def test_invalid_allowed_extension_raises():
    with pytest.raises(ValueError):
        FileTypeValidator(allowed_extension=".gif")


def test_uniform_extension_passes(clean_env, tmp_path):
    images = tmp_path / "images"
    images.mkdir()
    (images / "a.jpg").write_bytes(b"x")
    (images / "b.jpg").write_bytes(b"y")
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = FileTypeValidator(allowed_extension=".jpg").validate(None)
    assert result.is_valid, result.errors
    assert result.metadata["uniform_extension"] == ".jpg"


def test_mixed_extensions_fail(clean_env, tmp_path):
    images = tmp_path / "images"
    images.mkdir()
    (images / "a.jpg").write_bytes(b"x")
    (images / "b.png").write_bytes(b"y")
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = FileTypeValidator(allowed_extension=".jpg").validate(None)
    assert not result.is_valid
    assert any("Multiple file extensions" in e for e in result.errors)


def test_invalid_extension_in_strict_mode_fails(clean_env, tmp_path):
    images = tmp_path / "images"
    images.mkdir()
    (images / "a.png").write_bytes(b"x")
    (images / "b.png").write_bytes(b"y")
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = FileTypeValidator(allowed_extension=".jpg").validate(None)
    assert not result.is_valid
    assert any("invalid extensions" in e for e in result.errors)


def test_no_files_fails(clean_env, tmp_path):
    (tmp_path / "images").mkdir()
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = FileTypeValidator(allowed_extension=".jpg").validate(None)
    assert not result.is_valid
    assert "No files found" in result.errors[0]


def test_nonexistent_path_fails(clean_env, tmp_path):
    clean_env.setenv("SRC_PATH", str(tmp_path))  # no "images" subdir
    result = FileTypeValidator(allowed_extension=".jpg").validate(None)
    assert not result.is_valid


def test_custom_subpath(clean_env, tmp_path):
    texts = tmp_path / "texts"
    texts.mkdir()
    (texts / "a.txt").write_text("hi")
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = FileTypeValidator(allowed_extension=".txt", path="texts").validate(None)
    assert result.is_valid


# ---- helpers --------------------------------------------------------------

def test_get_files_single_file(tmp_path):
    p = tmp_path / "a.jpg"
    p.write_bytes(b"x")
    v = FileTypeValidator(allowed_extension=".jpg")
    assert v._get_files_to_validate(str(p), True, True) == [p]


def test_get_files_list(tmp_path):
    p = tmp_path / "a.jpg"
    p.write_bytes(b"x")
    v = FileTypeValidator(allowed_extension=".jpg")
    files = v._get_files_to_validate([str(p), str(tmp_path / "missing.jpg")], True, True)
    assert files == [p]


def test_get_files_unsupported_type_raises():
    v = FileTypeValidator(allowed_extension=".jpg")
    with pytest.raises(ValueError):
        v._get_files_to_validate(123, True, True)


def test_get_files_ignores_hidden(tmp_path):
    (tmp_path / ".h.jpg").write_bytes(b"x")
    visible = tmp_path / "v.jpg"
    visible.write_bytes(b"x")
    v = FileTypeValidator(allowed_extension=".jpg")
    files = v._get_files_to_validate(str(tmp_path), True, True)
    assert visible in files
    assert all(not p.name.startswith(".") for p in files)


def test_validate_file_extensions_empty():
    v = FileTypeValidator(allowed_extension=".jpg")
    res = v._validate_file_extensions([])
    assert not res.is_valid
