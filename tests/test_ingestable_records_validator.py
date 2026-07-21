"""Tests for IngestableRecordsValidator — the zero-record fail-fast guard.

Covers the two "0 ingestable records" inputs the adversarial MLM run surfaced:
a header-only / empty CSV, and a CSV whose every referenced file is missing.
Both must be rejected at preflight (before the table is created), not fail late
with a misleading "rows already in the database" message.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from tracebloc_ingestor.validators.ingestable_records_validator import (
    IngestableRecordsValidator,
)


def _write(path, text):
    path.write_text(text, encoding="utf-8")
    return path


def _write_json(tmp_path, obj, name="manifest.json"):
    path = tmp_path / name
    path.write_text(json.dumps(obj), encoding="utf-8")
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


def test_filename_column_case_variant_is_rejected(clean_env, tmp_path, make_csv):
    # #372: the cluster's transfer read is case-sensitive record.get("filename"),
    # so a `FileName` case variant is doomed after upload. It must be rejected up
    # front with a targeted "rename to lowercase" hint (NOT accepted as before).
    src = tmp_path / "src"
    (src / "sequences").mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))
    path = make_csv([{"FileName": "doc1"}])  # header cased differently
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert not result.is_valid
    assert "must be lowercase 'filename'" in result.errors[0]
    assert "'FileName'" in result.errors[0]
    assert result.metadata["reason"] == "filename_column_case_variant"


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


def test_file_bearing_but_no_filename_column_is_rejected(clean_env, tmp_path, make_csv):
    # #372: rows present, file_subdir set, but the CSV names its file column
    # something other than `filename` (e.g. image_id / text_col). The cluster
    # can't resolve it and drops every row after upload, so reject up front
    # instead of deferring (the pre-#372 no-op).
    src = tmp_path / "src"
    (src / "sequences").mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))
    path = make_csv([{"text_col": "hi"}, {"text_col": "yo"}])
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert not result.is_valid
    assert "No 'filename' column" in result.errors[0]
    assert "text_col" in result.errors[0]  # lists the actual columns
    assert result.metadata["reason"] == "filename_column_missing"


def test_image_id_filename_column_is_rejected(clean_env, tmp_path, make_csv):
    # #371/#372 image case: labels.csv uses `image_id` instead of `filename`.
    # Doomed at the cluster's case-sensitive transfer read -> reject up front.
    src = tmp_path / "src"
    (src / "images").mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))
    path = make_csv([{"image_id": "001", "label": "cat"}])
    result = IngestableRecordsValidator(
        file_subdir="images", extension=".jpg"
    ).validate(str(path))
    assert not result.is_valid
    assert "No 'filename' column" in result.errors[0]
    assert result.metadata["reason"] == "filename_column_missing"


def test_exact_filename_column_with_whitespace_is_accepted(
    clean_env, tmp_path, make_csv
):
    # `" filename "` resolves at transfer (pandas strips header whitespace), so
    # it must pass the column check too — only case, not whitespace, is strict.
    src = tmp_path / "src"
    seq = src / "sequences"
    seq.mkdir(parents=True)
    _write(seq / "doc1.txt", "x")
    clean_env.setenv("SRC_PATH", str(src))
    path = make_csv([{" filename ": "doc1"}])
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert result.is_valid, result.errors


def test_semicolon_manifest_not_false_rejected_with_csv_options(clean_env, tmp_path):
    # #376/#372: a non-comma manifest must be tokenized with the run's dialect.
    # Without it the header reads as one column ("filename;label"), the exact-
    # filename check finds nothing, and a VALID dataset is false-rejected —
    # defeating the purpose of the check. With csv_options it parses correctly.
    src = tmp_path / "src"
    seq = src / "sequences"
    seq.mkdir(parents=True)
    _write(seq / "doc1.txt", "x")
    clean_env.setenv("SRC_PATH", str(src))
    path = _write(tmp_path / "m.csv", "filename;label\ndoc1;cat\n")
    result = IngestableRecordsValidator(
        file_subdir="sequences", csv_options={"delimiter": ";"}
    ).validate(str(path))
    assert result.is_valid, result.errors


def test_semicolon_manifest_mis_tokenized_without_dialect(clean_env, tmp_path):
    # The failure mode the dialect threading prevents: comma-tokenized, the
    # semicolon header is one mashed column with no exact `filename`.
    src = tmp_path / "src"
    seq = src / "sequences"
    seq.mkdir(parents=True)
    _write(seq / "doc1.txt", "x")
    clean_env.setenv("SRC_PATH", str(src))
    path = _write(tmp_path / "m.csv", "filename;label\ndoc1;cat\n")
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert not result.is_valid  # mashed header -> no exact filename column


def test_malformed_csv_options_rejected_at_construction():
    # Mirrors the grouped validators (#376): fail fast at construction.
    with pytest.raises(ValueError):
        IngestableRecordsValidator(csv_options={"delimiter": 123})


def test_missing_filename_column_message_is_capped(clean_env, tmp_path, make_csv):
    # #372 fix-2: a wide manifest must not produce an unbounded column list in
    # the error (redaction.column_preview caps it).
    src = tmp_path / "src"
    (src / "images").mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))
    wide = {f"c{i}": "v" for i in range(40)}  # 40 columns, none named filename
    path = make_csv([wide])
    result = IngestableRecordsValidator(file_subdir="images").validate(str(path))
    assert not result.is_valid
    assert "more of 40)" in result.errors[0]  # capped preview, not all 40 names
    # ...but the FULL header set is retained in metadata (learned Bugbot rule).
    assert len(result.metadata["columns"]) == 40
    assert result.metadata["columns"][0] == "c0"


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


# --- JSON manifests: same exact-`filename` contract as CSV (#384 / #3) --------
# Removing BaseIngestor._resolve_filename_key dropped the ingest-time case/
# whitespace remap; the preflight now enforces the exact key for JSON too, so a
# case-variant no longer passes preflight and fails late cluster-side (exit 9).


def test_json_file_bearing_with_exact_filename_passes(tmp_path):
    path = _write_json(tmp_path, [{"filename": "a.txt", "label": "x"}])
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert result.is_valid


def test_json_file_bearing_case_variant_filename_is_rejected(tmp_path):
    # The cluster reads case-sensitive record.get("filename") for JSON too, so a
    # `Filename` key would upload then fail late — reject up front.
    path = _write_json(tmp_path, [{"Filename": "a.txt", "label": "x"}])
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert not result.is_valid
    assert "must be lowercase 'filename'" in result.errors[0]
    assert result.metadata["reason"] == "filename_column_case_variant"


def test_json_file_bearing_missing_filename_is_rejected(tmp_path):
    path = _write_json(tmp_path, [{"image_id": "a", "label": "x"}])
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert not result.is_valid
    assert result.metadata["reason"] == "filename_column_missing"


def test_json_single_object_form_is_checked(tmp_path):
    # Object form (not an array) is read via json.load; its keys are checked.
    path = _write_json(tmp_path, {"Filename": "a.txt"})
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert not result.is_valid
    assert result.metadata["reason"] == "filename_column_case_variant"


def test_json_non_file_bearing_passes(tmp_path):
    # No file_subdir → no filename contract to enforce.
    path = _write_json(tmp_path, [{"anything": 1}])
    result = IngestableRecordsValidator(file_subdir=None).validate(str(path))
    assert result.is_valid


def test_malformed_json_is_skipped_not_rejected(tmp_path):
    # A parse error is the JSON-structure validators' concern — the filename
    # preflight must skip (not false-reject) when it can't read the keys.
    path = _write(tmp_path / "bad.json", "{ this is not valid json")
    result = IngestableRecordsValidator(file_subdir="sequences").validate(str(path))
    assert result.is_valid
    assert result.metadata.get("checked") is False
