"""Tests for LabelColumnValidator — the missing-label-column fail-fast guard.

Adversarial text-classification ingestion surfaced a CSV whose header is
``filename,extension`` (no ``label``), configured with ``label: label``,
slipping past every validator: it ingested records with ``label=None`` and the
backend rejected each row with ``HTTP 400 "label: may not be null"`` — a late,
confusing failure. This validator rejects that at preflight with a clear,
actionable message naming the columns that ARE present.
"""

from __future__ import annotations

import pandas as pd

from tracebloc_ingestor.validators.label_column_validator import LabelColumnValidator


def test_missing_label_column_is_rejected(make_csv):
    path = make_csv(
        pd.DataFrame({"filename": ["a", "b"], "extension": [".txt", ".txt"]})
    )
    result = LabelColumnValidator().validate(str(path))
    assert not result.is_valid
    assert "Configured label column 'label' not found" in result.errors[0]
    # message lists the columns that ARE present, to guide the fix
    assert "filename" in result.errors[0] and "extension" in result.errors[0]
    assert result.metadata["columns"] == ["filename", "extension"]


def test_present_label_column_passes(make_csv):
    path = make_csv(
        pd.DataFrame({"filename": ["a"], "extension": [".txt"], "label": ["pos"]})
    )
    result = LabelColumnValidator().validate(str(path))
    assert result.is_valid
    assert result.metadata["checked"] is True


def test_custom_label_column_name_missing_is_rejected(make_csv):
    # A YAML configuring label_column: sentiment, but the CSV only has 'label'.
    path = make_csv(
        pd.DataFrame({"filename": ["a"], "extension": [".txt"], "label": ["pos"]})
    )
    result = LabelColumnValidator(label_column="sentiment").validate(str(path))
    assert not result.is_valid
    assert "Configured label column 'sentiment' not found" in result.errors[0]


def test_custom_label_column_name_present_passes(make_csv):
    path = make_csv(
        pd.DataFrame({"filename": ["a"], "extension": [".txt"], "sentiment": ["pos"]})
    )
    result = LabelColumnValidator(label_column="sentiment").validate(str(path))
    assert result.is_valid


def test_label_column_match_is_case_insensitive(make_csv):
    # CSVIngestor resolves headers case-insensitively; mirror that here.
    path = make_csv(
        pd.DataFrame({"filename": ["a"], "extension": [".txt"], "Label": ["pos"]})
    )
    result = LabelColumnValidator().validate(str(path))
    assert result.is_valid


def test_none_or_empty_label_column_defaults_to_label(make_csv):
    path = make_csv(pd.DataFrame({"filename": ["a"], "extension": [".txt"]}))
    # An unset/blank configured name must default to "label", not crash.
    result = LabelColumnValidator(label_column="").validate(str(path))
    assert not result.is_valid
    assert "'label'" in result.errors[0]


def test_dataframe_input_missing_and_present():
    missing = LabelColumnValidator().validate(pd.DataFrame({"filename": ["a"]}))
    assert not missing.is_valid
    present = LabelColumnValidator().validate(
        pd.DataFrame({"filename": ["a"], "label": ["x"]})
    )
    assert present.is_valid


def test_non_csv_input_is_a_noop():
    # JSON / non-path inputs are covered by their own validators.
    result = LabelColumnValidator().validate("data.json")
    assert result.is_valid
    assert result.metadata["checked"] is False
    assert LabelColumnValidator().validate(None).is_valid


def test_unreadable_csv_is_a_benign_skip(tmp_path):
    # A missing CSV path can't be introspected here; the read/transfer path
    # raises its own clear error. Don't double-report.
    result = LabelColumnValidator().validate(str(tmp_path / "nope.csv"))
    assert result.is_valid
    assert result.metadata["checked"] is False
