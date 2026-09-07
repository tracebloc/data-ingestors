"""Tests for ContrastivePairsValidator — the structural check that each
``embeddings`` ``.txt`` is a tab-separated pair (``anchor\\tpositive``) or
triplet (``anchor\\tpositive\\tnegative``). FileTypeValidator only checks the
extension and TextContentValidator only checks UTF-8 decodability — neither sees
this structure, so this validator is what rejects malformed contrastive rows.
"""

from __future__ import annotations

import pandas as pd
import pytest

from tracebloc_ingestor.validators.contrastive_pairs_validator import (
    ContrastivePairsValidator,
)


@pytest.fixture
def staged(clean_env, tmp_path):
    """Return (texts_dir, make_csv) with SRC_PATH pointed at src and a texts/."""
    src = tmp_path / "src"
    texts = src / "texts"
    texts.mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))

    def make_csv(filenames, column="filename"):
        path = tmp_path / "data.csv"
        pd.DataFrame({column: filenames}).to_csv(path, index=False)
        return path

    return texts, make_csv


def _validate(filenames, staged):
    texts, make_csv = staged
    path = make_csv(filenames)
    return ContrastivePairsValidator(texts_path="texts").validate(str(path))


def test_valid_pair_and_triplet_pass(staged):
    texts, make_csv = staged
    (texts / "pair.txt").write_text("anchor text\tpositive text\n", encoding="utf-8")
    (texts / "triplet.txt").write_text("anchor\tpositive\tnegative\n", encoding="utf-8")
    result = _validate(["pair", "triplet"], staged)
    assert result.is_valid, result.errors
    assert result.metadata["rows_checked"] == 2


def test_plain_text_no_tab_rejected(staged):
    """No tab -> one field. Valid for seq2seq/causal LM, malformed here."""
    texts, _ = staged
    (texts / "bad.txt").write_text("just prose, no tab at all\n", encoding="utf-8")
    result = _validate(["bad"], staged)
    assert not result.is_valid
    assert any("tab-separated fields" in e and "found 1" in e for e in result.errors)


def test_four_fields_rejected(staged):
    texts, _ = staged
    (texts / "bad.txt").write_text("a\tb\tc\td\n", encoding="utf-8")
    result = _validate(["bad"], staged)
    assert not result.is_valid
    assert any("found 4" in e for e in result.errors)


def test_empty_field_rejected(staged):
    texts, _ = staged
    (texts / "bad.txt").write_text("anchor\t\n", encoding="utf-8")
    result = _validate(["bad"], staged)
    assert not result.is_valid
    assert any("are empty" in e for e in result.errors)


def test_whitespace_only_field_rejected(staged):
    texts, _ = staged
    (texts / "bad.txt").write_text("anchor\t   \tnegative\n", encoding="utf-8")
    result = _validate(["bad"], staged)
    assert not result.is_valid
    assert any("are empty" in e for e in result.errors)


def test_multiline_file_rejected(staged):
    """The contract is one record per file; a second non-empty line is
    malformed (several records crammed into one .txt)."""
    texts, _ = staged
    (texts / "bad.txt").write_text("a\tb\nc\td\n", encoding="utf-8")
    result = _validate(["bad"], staged)
    assert not result.is_valid
    assert any("multiple lines" in e for e in result.errors)


def test_trailing_newline_and_blank_lines_tolerated(staged):
    """A trailing newline / surrounding blank lines are stripped — the record
    itself is a single valid pair, so it passes."""
    texts, _ = staged
    (texts / "ok.txt").write_text("\nanchor\tpositive\n\n", encoding="utf-8")
    result = _validate(["ok"], staged)
    assert result.is_valid, result.errors


def test_empty_file_left_to_text_content_validator(staged):
    """An empty / whitespace-only file is the shared TextContentValidator's
    warning to raise; the structural validator stays silent (no double-report)."""
    texts, _ = staged
    (texts / "empty.txt").write_text("   \n", encoding="utf-8")
    result = _validate(["empty"], staged)
    assert result.is_valid, result.errors


def test_missing_file_reported(staged):
    result = _validate(["does_not_exist"], staged)
    assert not result.is_valid
    assert any("not found" in e for e in result.errors)


def test_path_traversal_manifest_value_skipped(staged):
    """A manifest value that escapes the dataset dir (``..`` / absolute) is
    resolved with ``_safe_join`` exactly as the transfer does (#239): the
    transfer rejects it, so preflight neither reads nor flags a file outside
    ``texts/`` — it is skipped here (mirrors TextContentValidator), not read
    from disk."""
    texts, make_csv = staged
    # A real readable pair OUTSIDE texts/ — proves we don't read it.
    outside = texts.parent.parent / "secret.txt"
    outside.write_text("anchor\tpositive\n", encoding="utf-8")
    path = make_csv(["../../secret"])
    result = ContrastivePairsValidator(texts_path="texts").validate(str(path))
    # Escaping value is skipped (no structural error raised for it).
    assert result.is_valid, result.errors


def test_missing_filename_column_reported(staged):
    texts, make_csv = staged
    path = make_csv(["x"], column="wrongname")
    result = ContrastivePairsValidator(texts_path="texts").validate(str(path))
    assert not result.is_valid
    assert any("filename" in e for e in result.errors)


def test_filename_with_explicit_extension_resolved(staged):
    """A manifest value that already carries the extension is used as-is (mirrors
    text_transfer's _has_extension rule), not double-suffixed."""
    texts, make_csv = staged
    (texts / "pair.txt").write_text("a\tb\n", encoding="utf-8")
    path = make_csv(["pair.txt"])
    result = ContrastivePairsValidator(texts_path="texts").validate(str(path))
    assert result.is_valid, result.errors


def test_unreadable_non_utf8_file_reported(staged):
    """A file the structural reader can't decode as UTF-8 is reported here as
    unreadable (binary/encoding rejection itself is TextContentValidator's job,
    but this validator must not crash on it)."""
    texts, _ = staged
    # Latin-1 bytes that are not valid UTF-8 (0xE9 = é in latin-1).
    (texts / "bad.txt").write_bytes("Caf\xe9\tBrasil".encode("latin-1"))
    result = _validate(["bad"], staged)
    assert not result.is_valid
    assert any("could not read text file" in e for e in result.errors)


def test_unexpected_error_surfaces_as_validation_error(staged, monkeypatch):
    """A blow-up in data loading is caught and surfaced as a failed result
    (mirrors sibling validators) rather than escaping the validator."""
    v = ContrastivePairsValidator(texts_path="texts")
    monkeypatch.setattr(
        v, "_load_data", lambda data: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    result = v.validate("anything")
    assert not result.is_valid
    assert any("validation error" in e for e in result.errors)


def test_error_cap_summarizes(staged):
    """A wholly-malformed dataset yields a capped, actionable summary rather
    than tens of thousands of lines (mirrors BIOLabelValidator)."""
    texts, _ = staged
    names = []
    for i in range(120):
        name = f"bad{i}"
        (texts / f"{name}.txt").write_text("no tab here\n", encoding="utf-8")
        names.append(name)
    result = _validate(names, staged)
    assert not result.is_valid
    assert any("further errors suppressed" in e for e in result.errors)
    assert len(result.errors) <= 51


def test_csv_padded_filename_header_still_resolves(staged, tmp_path):
    """A padded ``filename`` header (``id, filename``) must resolve, not report
    the column missing.

    Regression (backend#1828): TabSeparatedRecordValidator — this validator's
    base — carried a private ``_resolve_column`` that lower-cased but did NOT
    strip, so pandas' parsed header ``" filename"`` never matched. ``embeddings``
    then rejected a manifest for a column visibly present, while sibling
    validators in the same preflight run resolved it. ``filename`` is padded
    whenever it is not the first CSV field. Written as a raw file: pandas keeps
    the space because ``_load_data`` does not pass ``skipinitialspace``.
    """
    texts, _ = staged
    (texts / "pair.txt").write_text("anchor text\tpositive text\n", encoding="utf-8")
    path = tmp_path / "padded.csv"
    path.write_text("id, filename\n1,pair\n", encoding="utf-8")

    result = ContrastivePairsValidator(texts_path="texts").validate(str(path))

    assert result.is_valid, result.errors
