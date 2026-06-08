"""Tests for BIOLabelValidator — token-classification label/word alignment."""

from __future__ import annotations

import pandas as pd
import pytest

from tracebloc_ingestor.validators.bio_label_validator import BIOLabelValidator


@pytest.fixture
def texts_dir(tmp_path, monkeypatch):
    """Create a SRC_PATH/texts dir and point the validator's config at it."""
    (tmp_path / "texts").mkdir()
    monkeypatch.setenv("SRC_PATH", str(tmp_path))
    return tmp_path / "texts"


def _write(texts_dir, name, words):
    (texts_dir / f"{name}.txt").write_text(words, encoding="utf-8")


@pytest.fixture
def validator():
    return BIOLabelValidator()


def test_valid_alignment_passes(validator, texts_dir):
    _write(texts_dir, "s1", "John Smith works at Google")
    _write(texts_dir, "s2", "Paris is nice")
    df = pd.DataFrame(
        {
            "filename": ["s1", "s2"],
            "label": ["B-PER I-PER O O B-ORG", "B-LOC O O"],
        }
    )
    result = validator.validate(df)
    assert result.is_valid, result.errors
    assert result.metadata["rows_checked"] == 2


def test_count_mismatch_fails(validator, texts_dir):
    _write(texts_dir, "s1", "John Smith works")  # 3 words
    df = pd.DataFrame({"filename": ["s1"], "label": ["B-PER I-PER"]})  # 2 tags
    result = validator.validate(df)
    assert not result.is_valid
    assert "count mismatch" in result.errors[0]


def test_invalid_tag_format_fails(validator, texts_dir):
    _write(texts_dir, "s1", "John lives here")
    df = pd.DataFrame({"filename": ["s1"], "label": ["B-PER BOGUS O"]})
    result = validator.validate(df)
    assert not result.is_valid
    assert "invalid BIO tag" in result.errors[0]


def test_missing_text_file_fails(validator, texts_dir):
    df = pd.DataFrame({"filename": ["nope"], "label": ["O"]})
    result = validator.validate(df)
    assert not result.is_valid
    assert "not found" in result.errors[0]


def test_empty_dataframe_fails(validator):
    result = validator.validate(pd.DataFrame())
    assert not result.is_valid
    assert "No data found" in result.errors[0]


def test_missing_label_column_fails(validator, texts_dir):
    df = pd.DataFrame({"filename": ["s1"]})
    result = validator.validate(df)
    assert not result.is_valid
    assert "Missing required column" in result.errors[0]


def test_missing_filename_column_fails(validator, texts_dir):
    df = pd.DataFrame({"label": ["O"]})
    result = validator.validate(df)
    assert not result.is_valid
    assert "filename" in result.errors[0]


def test_case_insensitive_columns(validator, texts_dir):
    _write(texts_dir, "s1", "Paris")
    df = pd.DataFrame({"Filename": ["s1"], "Label": ["B-LOC"]})
    result = validator.validate(df)
    assert result.is_valid, result.errors


def test_extension_without_leading_dot_is_normalized(texts_dir):
    _write(texts_dir, "s1", "Paris")
    v = BIOLabelValidator(extension="txt")  # no leading dot
    df = pd.DataFrame({"filename": ["s1"], "label": ["B-LOC"]})
    result = v.validate(df)
    assert result.is_valid, result.errors


def test_error_reporting_is_capped(validator, texts_dir):
    # 60 mismatched rows -> errors are capped with a suppression notice
    n = 60
    for i in range(n):
        _write(texts_dir, f"s{i}", "one two")  # 2 words
    df = pd.DataFrame(
        {"filename": [f"s{i}" for i in range(n)], "label": ["O"] * n}  # 1 tag
    )
    result = validator.validate(df)
    assert not result.is_valid
    assert any("further errors suppressed" in e for e in result.errors)
