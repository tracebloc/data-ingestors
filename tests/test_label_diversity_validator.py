"""Tests for LabelDiversityValidator — fail-fast on single-label classification.

Surfaces the cause locally (with the actual distinct label values) instead
of letting the backend reject with the misleading "Backend failed to
prepare the dataset; it was NOT registered" cascade once rows have already
landed in MySQL. Issue #251.
"""

from __future__ import annotations

import pandas as pd
import pytest

from tracebloc_ingestor.validators.label_diversity_validator import (
    LabelDiversityValidator,
)


# ---------------------------------------------------------------------------
# Positive cases — must pass
# ---------------------------------------------------------------------------

def test_two_distinct_labels_passes():
    df = pd.DataFrame({"a": [1, 2, 3, 4], "label": ["A", "B", "A", "B"]})
    result = LabelDiversityValidator().validate(df)
    assert result.is_valid


def test_many_distinct_labels_passes():
    df = pd.DataFrame({"label": ["A", "B", "C", "D", "E"]})
    result = LabelDiversityValidator().validate(df)
    assert result.is_valid


def test_distinct_labels_with_nulls_counts_only_non_null():
    """Null cells don't count toward distinct labels; if there are still
    ≥2 distinct non-null values the dataset is fine."""
    df = pd.DataFrame({"label": ["A", "B", None, None]})
    result = LabelDiversityValidator().validate(df)
    assert result.is_valid
    assert result.metadata["distinct_count"] == 2


def test_label_column_case_insensitive_match():
    """A CSV header ``Label`` should still resolve when the validator is
    configured for ``label`` (default). Matches the case-insensitive
    pattern BIOLabelValidator uses."""
    df = pd.DataFrame({"a": [1, 2], "Label": ["X", "Y"]})
    result = LabelDiversityValidator().validate(df)
    assert result.is_valid


# ---------------------------------------------------------------------------
# Failure cases — must reject with a CLEAR message
# ---------------------------------------------------------------------------

def test_single_label_fails_with_distinct_value_listed():
    """A 10-row dataset where every row has ``label = "X"`` is not a
    classification dataset. The error must name the offending distinct
    value(s) and the count so the user immediately sees what's wrong."""
    df = pd.DataFrame({"label": ["X"] * 10})
    result = LabelDiversityValidator().validate(df)
    assert not result.is_valid
    err = result.errors[0]
    # User-facing message must include the actual single value found.
    assert "'X'" in err or "'X'" in str(result.metadata.get("value_counts", {}))
    # And explain WHY it's rejected.
    assert "classification" in err.lower()
    assert "distinct" in err.lower()


def test_single_label_only_nulls_fails():
    """All-null label column → 0 distinct → rejected."""
    df = pd.DataFrame({"label": [None, None, None]})
    result = LabelDiversityValidator().validate(df)
    assert not result.is_valid


def test_single_label_one_value_with_some_nulls_fails():
    """One distinct value plus nulls is still only 1 distinct value."""
    df = pd.DataFrame({"label": ["A", "A", None, "A", None]})
    result = LabelDiversityValidator().validate(df)
    assert not result.is_valid


def test_error_mentions_regression_alternative():
    """A user who has a continuous target shouldn't be told 'add a fake
    second label' — the error should point them at regression-family
    categories which legitimately accept a single target column."""
    df = pd.DataFrame({"label": ["A"] * 5})
    result = LabelDiversityValidator().validate(df)
    assert not result.is_valid
    assert "regression" in result.errors[0].lower()


# ---------------------------------------------------------------------------
# Defensive paths — must not double-report (other validators own those)
# ---------------------------------------------------------------------------

def test_empty_dataframe_passes_silently():
    """Empty input is the 'no rows' / empty-CSV class — handled by other
    validators with their own clear messages. Don't double-report."""
    result = LabelDiversityValidator().validate(pd.DataFrame())
    assert result.is_valid
    assert result.metadata["rows_checked"] == 0


def test_label_column_missing_passes_with_warning():
    """If the CSV has no label column at all, that's a schema-mismatch
    case handled by other layers. This validator just warns and skips."""
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    result = LabelDiversityValidator().validate(df)
    assert result.is_valid
    assert any("not found in CSV" in w for w in (result.warnings or []))


# ---------------------------------------------------------------------------
# CSV-path streaming check — must not load the whole wide CSV into memory
# ---------------------------------------------------------------------------

def test_csv_path_reads_only_the_label_column(tmp_path):
    """For a wide CSV (one label + many feature columns), the validator
    must read only the label column — counting distinct labels doesn't
    need the features, and a multi-GB proteomics panel would otherwise
    OOM. Mirrors the streaming-first patterns elsewhere in the codebase
    (DataValidator's chunked path)."""
    p = tmp_path / "wide.csv"
    cols = ",".join([f"f{i:02d}" for i in range(50)] + ["label"])
    rows = "\n".join(
        [",".join(["0.5"] * 50 + [("A" if i % 2 else "B")]) for i in range(20)]
    )
    p.write_text(cols + "\n" + rows + "\n")
    result = LabelDiversityValidator().validate(str(p))
    assert result.is_valid
    assert result.metadata["distinct_count"] == 2


def test_csv_path_rejects_single_label(tmp_path):
    """End-to-end CSV-path test of the failure case — mirrors the
    adversarial test against v0.3.10-rc1 that surfaced #251."""
    p = tmp_path / "single.csv"
    p.write_text("id,label\n1,X\n2,X\n3,X\n")
    result = LabelDiversityValidator().validate(str(p))
    assert not result.is_valid
    assert "1 distinct" in result.errors[0]
    assert "'X'" in result.errors[0] or "'X'" in str(result.metadata.get("value_counts", {}))


# ---------------------------------------------------------------------------
# Custom column name
# ---------------------------------------------------------------------------

def test_custom_label_column_name():
    """When the user configures a non-default label column, the
    validator must check THAT column (mirrors BIOLabelValidator's
    behavior with custom columns)."""
    df = pd.DataFrame({"target": ["A", "B"], "label": ["X", "X"]})
    # Custom column is the diverse one — should pass.
    assert LabelDiversityValidator(label_column="target").validate(df).is_valid
    # The default `label` column is single-value here — should fail.
    assert not LabelDiversityValidator(label_column="label").validate(df).is_valid
