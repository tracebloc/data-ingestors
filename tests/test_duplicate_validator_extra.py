"""Supplementary DuplicateValidator coverage: parent-dir warnings, dir
creation, and the error-handling fallbacks not exercised by
test_duplicate_validator.py."""

from __future__ import annotations

from tracebloc_ingestor.validators.duplicate_validator import DuplicateValidator


def test_missing_parent_directory_warns(tmp_path):
    dest = tmp_path / "missing_parent" / "table"
    result = DuplicateValidator(dest_path=str(dest)).validate(None)
    assert result.is_valid
    assert any("Parent directory" in w for w in result.warnings)
    assert result.metadata["parent_directory_exists"] is False


def test_check_directory_exists_true(tmp_path):
    d = tmp_path / "existing"
    d.mkdir()
    assert DuplicateValidator(dest_path=str(d))._check_directory_exists() is True


def test_check_directory_exists_false_for_file(tmp_path):
    f = tmp_path / "a_file"
    f.write_text("x")
    # A file is not a directory.
    assert DuplicateValidator(dest_path=str(f))._check_directory_exists() is False


def test_is_directory_empty_true(tmp_path):
    d = tmp_path / "empty"
    d.mkdir()
    assert DuplicateValidator(dest_path=str(d))._is_directory_empty() is True


def test_is_directory_empty_handles_missing(tmp_path):
    # iterdir on a nonexistent dir raises -> caught -> returns False.
    assert DuplicateValidator(dest_path=str(tmp_path / "nope"))._is_directory_empty() is False


def test_create_directory_if_needed(tmp_path):
    dest = tmp_path / "to_create" / "nested"
    v = DuplicateValidator(dest_path=str(dest))
    assert v._create_directory_if_needed() is True
    assert dest.exists()


def test_create_directory_if_needed_existing(tmp_path):
    dest = tmp_path / "already"
    dest.mkdir()
    assert DuplicateValidator(dest_path=str(dest))._create_directory_if_needed() is True


# --- Within-CSV duplicate filename detection (warning, not a hard failure) ---


def test_within_csv_duplicate_filenames_warn(tmp_path, make_csv):
    dest = tmp_path / "new_table"  # does not exist -> no dest collision
    path = make_csv(
        [
            {"filename": "a", "label": "x"},
            {"filename": "b", "label": "y"},
            {"filename": "a", "label": "z"},  # duplicate of row 1
        ]
    )
    result = DuplicateValidator(dest_path=str(dest)).validate(str(path))
    assert result.is_valid  # warning only — does NOT fail
    assert any("appear more than once" in w for w in result.warnings)
    assert result.metadata["within_csv_duplicate_filenames"] is True


def test_within_csv_no_duplicates_no_warning(tmp_path, make_csv):
    dest = tmp_path / "new_table"
    path = make_csv([{"filename": "a"}, {"filename": "b"}])
    result = DuplicateValidator(dest_path=str(dest)).validate(str(path))
    assert result.is_valid
    assert not any("appear more than once" in w for w in result.warnings)
    assert result.metadata["within_csv_duplicate_filenames"] is False


def test_within_csv_duplicate_case_insensitive_column(tmp_path, make_csv):
    import pandas as pd

    dest = tmp_path / "new_table"
    path = make_csv(pd.DataFrame({"FileName": ["a", "a"]}))
    result = DuplicateValidator(dest_path=str(dest)).validate(str(path))
    assert any("appear more than once" in w for w in result.warnings)


def test_within_csv_no_filename_column_is_noop(tmp_path, make_csv):
    dest = tmp_path / "new_table"
    path = make_csv([{"feature_a": "1"}, {"feature_a": "1"}])
    result = DuplicateValidator(dest_path=str(dest)).validate(str(path))
    assert not any("appear more than once" in w for w in result.warnings)


def test_within_csv_none_input_is_noop(tmp_path):
    # The table-only callers pass None; must not error or warn.
    dest = tmp_path / "new_table"
    result = DuplicateValidator(dest_path=str(dest)).validate(None)
    assert result.is_valid
    assert result.metadata["within_csv_duplicate_filenames"] is False
