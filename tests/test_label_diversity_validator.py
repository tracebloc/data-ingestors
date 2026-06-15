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


def test_csv_whitespace_header_still_checked(tmp_path):
    """A label header with surrounding whitespace (" label ") must still be
    found and checked, not skipped.

    Regression (bugbot #252): CSVIngestor strips column-name whitespace on
    read, so " label " is ingested as `label`. The validator resolved against
    the raw header, treated the column as missing, skipped the diversity check
    with a warning, and a single-class CSV sailed through preflight to fail at
    backend prepare. _resolve_column now strips, matching the ingestor.
    """
    p = tmp_path / "ws_header.csv"
    p.write_text("id, label \n1,X\n2,X\n3,X\n")
    result = LabelDiversityValidator().validate(str(p))
    assert not result.is_valid
    assert "1 distinct" in result.errors[0]


def test_csv_whitespace_header_schema_na_rules_apply(tmp_path):
    """A whitespace label header that IS a schema column must still get the
    schema NA rules — " label " strips to `label`, matches the schema entry,
    so "null" reads as missing and a single-class dataset is flagged
    (bugbot #252)."""
    p = tmp_path / "ws_schema.csv"
    p.write_text("id, label \n1,X\n2,null\n3,X\n")
    result = LabelDiversityValidator(
        label_column="label", schema={"id": "INT", "label": "VARCHAR(8)"}
    ).validate(str(p))
    assert not result.is_valid
    assert "1 distinct" in result.errors[0]


def test_csv_quoted_header_does_not_skew_multilabel(tmp_path):
    """A quoted/comma-bearing header must not trip the column resolution.

    Regression (bugbot #252, medium): the old loader resolved the label
    column with a naive ``header_line.split(",")``, which splits inside
    quoted headers and diverges from pandas. When it failed to find the
    column it fell back to ``nrows=1`` and counted distinct labels on that
    single row — rejecting a perfectly diverse dataset. Resolving against
    pandas' own header parse (nrows=0) fixes it, so a header like
    ``"feature,with,commas"`` alongside ``label`` reads the full column.
    """
    p = tmp_path / "quoted.csv"
    # The first column's header literally contains commas (quoted).
    p.write_text(
        '"feature,with,commas",label\n'
        + "\n".join(f"{i},{'A' if i % 2 else 'B'}" for i in range(20))
        + "\n"
    )
    result = LabelDiversityValidator().validate(str(p))
    assert result.is_valid, f"expected valid; errors={result.errors}"
    assert result.metadata["distinct_count"] == 2


def test_csv_read_error_fails_closed_not_skipped(tmp_path, monkeypatch):
    """A read failure must FAIL the check, not silently pass.

    Regression (bugbot #252, high): ``_load_data`` previously swallowed any
    read exception and returned ``None``, which ``validate`` treats as an
    empty/benign dataset → valid. A single-label CSV whose targeted read
    errored could sail through preflight and hit the backend rejection this
    validator exists to prevent. Read errors now propagate to ``validate``'s
    handler and fail the check.
    """
    p = tmp_path / "boom.csv"
    p.write_text("id,label\n1,X\n2,X\n")

    real_read_csv = pd.read_csv

    def _boom(path, *args, **kwargs):
        # Let the cheap header probe (nrows=0) succeed, then blow up on the
        # actual data read — mimics a usecols/encoding failure mid-load.
        if kwargs.get("nrows") == 0:
            return real_read_csv(path, *args, **kwargs)
        raise ValueError("simulated CSV read failure")

    monkeypatch.setattr(pd, "read_csv", _boom)
    result = LabelDiversityValidator().validate(str(p))
    assert not result.is_valid
    assert "validation error" in result.errors[0].lower()


def test_schema_label_sentinel_tokens_count_as_missing(tmp_path):
    """When the label IS a schema column, NA sentinels ("null", "NA", …) must
    be read as missing — matching CSVIngestor/DataValidator — so a CSV whose
    only real class is "X" plus sentinel rows is correctly flagged
    single-label (bugbot #252)."""
    p = tmp_path / "schema_label.csv"
    p.write_text("id,label\n1,X\n2,null\n3,NA\n4,X\n")
    result = LabelDiversityValidator(
        label_column="label", schema={"id": "INT", "label": "VARCHAR(8)"}
    ).validate(str(p))
    assert not result.is_valid
    assert "1 distinct" in result.errors[0]


def test_non_schema_label_keeps_sentinel_as_real_class(tmp_path):
    """When the label is NOT a schema column (image/text classification),
    the ingestor keeps "NA"/"null" as literal class values — so the validator
    must too, or it would miss / mis-count classes (bugbot #252). Here "X"
    and "NA" are two genuine classes; the dataset must pass."""
    p = tmp_path / "non_schema_label.csv"
    p.write_text("filename,label\na.jpg,X\nb.jpg,NA\nc.jpg,X\nd.jpg,NA\n")
    result = LabelDiversityValidator(label_column="label").validate(str(p))
    assert result.is_valid, f"expected valid; errors={result.errors}"
    assert result.metadata["distinct_count"] == 2


def test_string_schema_label_no_numeric_collapse(tmp_path):
    """A VARCHAR label column with numeric-looking values ("1.0", "1.00",
    "01") must keep them distinct as strings — matching the ingestor's
    string-dtype pin — rather than collapsing to a single float class and
    wrongly rejecting a diverse dataset (bugbot #252)."""
    p = tmp_path / "numeric_strings.csv"
    p.write_text("id,label\n1,01\n2,1\n3,1.0\n4,02\n")
    result = LabelDiversityValidator(
        label_column="label", schema={"id": "INT", "label": "VARCHAR(8)"}
    ).validate(str(p))
    assert result.is_valid, f"expected valid; errors={result.errors}"
    assert result.metadata["distinct_count"] == 4


# ---------------------------------------------------------------------------
# Whitespace handling — silent-corruption fix (#261)
# ---------------------------------------------------------------------------

def test_whitespace_duplicates_collapsed_and_warned():
    """Whitespace-padded label values must collapse to the same class
    AND surface a WARNING so the user knows the framework will strip
    them at write time. Without this, a CSV with ``"  A  "`` and
    ``"A"`` mixed would pass diversity (2 distinct) but train a model
    with 3 classes after ingest — silent label-set corruption (#261).
    """
    df = pd.DataFrame({"label": ["  A  ", "A", "B", "  A"]})
    result = LabelDiversityValidator().validate(df)
    # After collapsing whitespace duplicates, distinct count == 2 (A, B).
    assert result.is_valid
    assert result.metadata["distinct_count"] == 2
    # Warning must name the offending pre-strip variants so the user
    # can fix their upstream data if they meant them to be different.
    assert result.warnings, "expected a whitespace-duplicate warning"
    w = result.warnings[0]
    assert "whitespace" in w.lower()


def test_whitespace_only_single_value_still_fails():
    """If the dataset has ``["  A  ", "A", "  A"]`` (all collapse to ``A``),
    after stripping it's a single-class dataset and the validator must
    still reject — the silent-corruption fix doesn't undo the diversity
    requirement, it just refuses to inflate distinct count via
    whitespace differences.
    """
    df = pd.DataFrame({"label": ["  A  ", "A", "  A"]})
    result = LabelDiversityValidator().validate(df)
    assert not result.is_valid
    assert "1 distinct" in result.errors[0]
    # The error should mention whitespace stripping so the user knows
    # what the validator did.
    assert "whitespace" in result.errors[0].lower()


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
