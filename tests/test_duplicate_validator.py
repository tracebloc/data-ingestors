"""Regression tests for #101: DuplicateValidator must allow reruns
when the destination dir was left behind empty by a previous aborted
ingestion.

Before the fix, *any* existing destination directory failed validation,
forcing customers to ``kubectl exec`` and ``rm -rf`` on the shared PVC
between attempts. After the fix, an empty leftover dir passes with a
warning; a populated dir still fails.
"""

import os

from tracebloc_ingestor.validators.duplicate_validator import DuplicateValidator


def test_missing_destination_passes(tmp_path):
    """Happy path: destination doesn't exist yet."""
    dest = tmp_path / "fresh_table"
    validator = DuplicateValidator(dest_path=str(dest))

    result = validator.validate(data=None)

    assert result.is_valid
    assert result.errors == []
    assert result.metadata["directory_exists"] is False


def test_empty_destination_passes_with_warning(tmp_path):
    """An empty leftover dir (previous run aborted before any data
    landed) is reused, with a warning. This is the #101 case."""
    dest = tmp_path / "leftover_empty_table"
    dest.mkdir()
    assert os.listdir(dest) == []
    validator = DuplicateValidator(dest_path=str(dest))

    result = validator.validate(data=None)

    assert result.is_valid, f"unexpected errors: {result.errors}"
    assert result.errors == []
    assert any("empty" in w for w in result.warnings), result.warnings
    assert result.metadata["directory_exists"] is True
    assert result.metadata["directory_empty"] is True


def test_non_empty_destination_fails(tmp_path):
    """Populated destination is still treated as a real collision — we
    must not clobber an existing dataset."""
    dest = tmp_path / "populated_table"
    dest.mkdir()
    (dest / "row_0.png").write_bytes(b"not-actually-a-png")
    validator = DuplicateValidator(dest_path=str(dest))

    result = validator.validate(data=None)

    assert not result.is_valid
    assert any("already exists" in e for e in result.errors), result.errors
    assert result.metadata["directory_exists"] is True
    assert result.metadata["directory_empty"] is False
