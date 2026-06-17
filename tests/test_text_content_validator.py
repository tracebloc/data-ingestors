"""Tests for TextContentValidator — content-level UTF-8 / emptiness checks
for NLP text files (the File Type Validator only checks the extension)."""

from __future__ import annotations

import pytest

from tracebloc_ingestor.validators.text_content_validator import TextContentValidator


@pytest.fixture
def staged(clean_env, tmp_path):
    """Return (src_dir, sequences_dir, set_csv) with SRC_PATH pointed at src."""
    src = tmp_path / "src"
    seq = src / "sequences"
    seq.mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))

    def make_csv(filenames):
        import pandas as pd

        path = tmp_path / "data.csv"
        pd.DataFrame({"filename": filenames}).to_csv(path, index=False)
        return path

    return src, seq, make_csv


def test_valid_utf8_text_passes(staged):
    _, seq, make_csv = staged
    (seq / "a.txt").write_text("a clean english sentence", encoding="utf-8")
    (seq / "b.txt").write_text("café — naïve façade", encoding="utf-8")
    path = make_csv(["a", "b"])
    result = TextContentValidator(texts_path="sequences").validate(str(path))
    assert result.is_valid, result.errors
    assert result.metadata["docs_checked"] == 2


def test_binary_content_is_rejected(staged):
    _, seq, make_csv = staged
    (seq / "a.txt").write_bytes(b"\x89PNG\r\n\x1a\n\x00\x00\x00binary\xff\xfe")
    path = make_csv(["a"])
    result = TextContentValidator(texts_path="sequences").validate(str(path))
    assert not result.is_valid
    assert any("a.txt" in e for e in result.errors)


def test_non_utf8_bytes_are_rejected(staged):
    _, seq, make_csv = staged
    # Latin-1 encoded bytes that are not valid UTF-8 (0xE9 = é in latin-1).
    (seq / "a.txt").write_bytes("Caf\xe9 do Brasil".encode("latin-1"))
    path = make_csv(["a"])
    result = TextContentValidator(texts_path="sequences").validate(str(path))
    assert not result.is_valid
    assert any("not valid UTF-8" in e for e in result.errors)


def test_empty_file_is_warned_not_rejected(staged):
    _, seq, make_csv = staged
    (seq / "a.txt").write_bytes(b"")
    path = make_csv(["a"])
    result = TextContentValidator(texts_path="sequences").validate(str(path))
    assert result.is_valid  # warn, don't fail
    assert any("empty or whitespace-only" in w for w in result.warnings)


def test_whitespace_only_file_is_warned(staged):
    _, seq, make_csv = staged
    (seq / "a.txt").write_text("   \n\t  \n", encoding="utf-8")
    path = make_csv(["a"])
    result = TextContentValidator(texts_path="sequences").validate(str(path))
    assert result.is_valid
    assert any("empty or whitespace-only" in w for w in result.warnings)


def test_missing_referenced_files_are_skipped_here(staged):
    # Missing files are the IngestableRecordsValidator's concern; this validator
    # only inspects content of files that exist.
    _, _, make_csv = staged
    path = make_csv(["does_not_exist"])
    result = TextContentValidator(texts_path="sequences").validate(str(path))
    assert result.is_valid
    assert result.metadata["docs_checked"] == 0


def test_multibyte_char_split_at_byte_cap_is_not_a_false_positive(staged):
    # A valid UTF-8 file longer than max_bytes whose cap lands mid-character
    # must NOT be flagged as binary (incremental decode handles the partial).
    _, seq, make_csv = staged
    text = "é" * 100  # each char is 2 bytes in UTF-8
    (seq / "a.txt").write_text(text, encoding="utf-8")
    path = make_csv(["a"])
    # max_bytes=51 lands in the middle of a 2-byte char (odd offset).
    result = TextContentValidator(texts_path="sequences", max_bytes=51).validate(
        str(path)
    )
    assert result.is_valid, result.errors


def test_sampling_bounds_files_checked(staged):
    _, seq, make_csv = staged
    for i in range(20):
        (seq / f"f{i}.txt").write_text("ok", encoding="utf-8")
    path = make_csv([f"f{i}" for i in range(20)])
    result = TextContentValidator(texts_path="sequences", sample_size=5).validate(
        str(path)
    )
    assert result.is_valid
    assert result.metadata["docs_checked"] == 5


def test_no_filename_column_is_a_noop(staged):
    _, _, _ = staged
    import pandas as pd

    src, _, _ = staged
    path = src.parent / "noname.csv"
    pd.DataFrame({"text": ["a", "b"]}).to_csv(path, index=False)
    result = TextContentValidator(texts_path="sequences").validate(str(path))
    assert result.is_valid
    assert result.metadata["docs_checked"] == 0


def test_binary_errors_are_capped(staged):
    # More than _MAX_REPORTED broken files -> errors are bounded with a
    # suppression marker rather than thousands of lines.
    from tracebloc_ingestor.validators.text_content_validator import _MAX_REPORTED

    _, seq, make_csv = staged
    n = _MAX_REPORTED + 10
    for i in range(n):
        (seq / f"f{i}.txt").write_bytes(b"\x00\xff binary")
    path = make_csv([f"f{i}" for i in range(n)])
    result = TextContentValidator(
        texts_path="sequences", sample_size=n
    ).validate(str(path))
    assert not result.is_valid
    assert any("further errors suppressed" in e for e in result.errors)
    assert len(result.errors) <= _MAX_REPORTED + 1


def test_traversal_path_is_skipped_not_inspected(staged):
    # A manifest value escaping SRC_PATH/<subdir> is rejected by the transfer;
    # this validator neither reads nor flags a file outside the dataset dir.
    src, _, make_csv = staged
    outside = src.parent / "outside.bin"
    outside.write_bytes(b"\x00\xff binary outside")  # would be flagged if read
    path = make_csv([str(outside), "../../outside.bin"])
    result = TextContentValidator(texts_path="sequences").validate(str(path))
    assert result.is_valid
    assert result.metadata["docs_checked"] == 0
