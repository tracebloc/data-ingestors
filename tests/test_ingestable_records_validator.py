"""Tests for IngestableRecordsValidator — the zero-record fail-fast guard.

Covers the two "0 ingestable records" inputs the adversarial MLM run surfaced:
a header-only / empty CSV, and a CSV whose every referenced file is missing.
Both must be rejected at preflight (before the table is created), not fail late
with a misleading "rows already in the database" message.
"""

from __future__ import annotations

import pandas as pd

from tracebloc_ingestor.validators.ingestable_records_validator import (
    IngestableRecordsValidator,
)


def _write(path, text):
    path.write_text(text, encoding="utf-8")
    return path


def test_header_only_csv_is_rejected(make_csv):
    # A CSV with a header row but no data rows.
    path = make_csv(pd.DataFrame(columns=["filename", "label"]))
    result = IngestableRecordsValidator().validate(str(path))
    assert not result.is_valid
    assert "No data rows found" in result.errors[0]
    assert result.metadata["data_rows"] == 0


def test_empty_zero_byte_csv_is_rejected(tmp_path):
    path = _write(tmp_path / "empty.csv", "")
    result = IngestableRecordsValidator().validate(str(path))
    assert not result.is_valid
    assert "No data rows found" in result.errors[0]


def test_non_csv_input_is_a_noop():
    # JSON / non-path inputs are handled by their own validators.
    result = IngestableRecordsValidator().validate("data.json")
    assert result.is_valid
    assert result.metadata["checked"] is False
    assert IngestableRecordsValidator().validate(None).is_valid


def test_all_referenced_files_missing_is_rejected(clean_env, tmp_path, make_csv):
    src = tmp_path / "src"
    (src / "sequences").mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))
    # CSV references two files that were never staged.
    path = make_csv([{"filename": "doc1"}, {"filename": "doc2"}])
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert not result.is_valid
    assert "No referenced data files could be found" in result.errors[0]
    assert result.metadata["found_at_least_one"] is False


def test_passes_when_at_least_one_referenced_file_exists(clean_env, tmp_path, make_csv):
    src = tmp_path / "src"
    seq = src / "sequences"
    seq.mkdir(parents=True)
    _write(seq / "doc2.txt", "hello world")  # only the second file is staged
    clean_env.setenv("SRC_PATH", str(src))
    path = make_csv([{"filename": "doc1"}, {"filename": "doc2"}])
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert result.is_valid, result.errors
    assert result.metadata["found_at_least_one"] is True


def test_filename_with_extension_resolves_without_double_suffix(
    clean_env, tmp_path, make_csv
):
    src = tmp_path / "src"
    seq = src / "sequences"
    seq.mkdir(parents=True)
    _write(seq / "doc1.txt", "x")
    clean_env.setenv("SRC_PATH", str(src))
    # filename already carries the extension -> must not become doc1.txt.txt
    path = make_csv([{"filename": "doc1.txt"}])
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert result.is_valid, result.errors


def test_filename_column_is_case_insensitive(clean_env, tmp_path, make_csv):
    src = tmp_path / "src"
    (src / "sequences").mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))
    path = make_csv([{"FileName": "doc1"}])  # header cased differently
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    # column resolves, file is missing -> rejected (not a no-op)
    assert not result.is_valid
    assert "No referenced data files" in result.errors[0]


def test_tabular_style_no_subdir_only_checks_rows(make_csv):
    # With no file_subdir, a populated CSV passes (no file cross-check).
    path = make_csv([{"a": "1"}, {"a": "2"}])
    result = IngestableRecordsValidator(file_subdir=None).validate(str(path))
    assert result.is_valid


def test_valid_dataset_with_files_passes(clean_env, tmp_path, make_csv):
    src = tmp_path / "src"
    seq = src / "sequences"
    seq.mkdir(parents=True)
    for name in ("a", "b", "c"):
        _write(seq / f"{name}.txt", "content")
    clean_env.setenv("SRC_PATH", str(src))
    path = make_csv([{"filename": n} for n in ("a", "b", "c")])
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert result.is_valid, result.errors


def test_file_bearing_but_no_filename_column_is_noop(clean_env, tmp_path, make_csv):
    # Rows present, file_subdir set, but the CSV has no filename column to
    # cross-check -> not this validator's error to raise (pass through).
    src = tmp_path / "src"
    (src / "sequences").mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))
    path = make_csv([{"text_col": "hi"}, {"text_col": "yo"}])
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert result.is_valid
    assert result.metadata["reason"] == "no_filename_column"


def test_absolute_or_traversal_path_is_not_counted_as_found(
    clean_env, tmp_path, make_csv
):
    # A manifest value that escapes SRC_PATH/<subdir> (absolute path or ``..``)
    # is rejected by the transfer's _safe_join (#239); even though the file
    # exists on disk it is NOT an ingestable file, so it must not mask the
    # zero-record case. (Bugbot: unsafe manifest path resolution.)
    src = tmp_path / "src"
    (src / "sequences").mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))
    outside = tmp_path / "outside.txt"  # exists, but outside the dataset dir
    outside.write_text("secret", encoding="utf-8")
    path = make_csv(
        [{"filename": str(outside)}, {"filename": "../../outside.txt"}]
    )
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert not result.is_valid
    assert "No referenced data files" in result.errors[0]
