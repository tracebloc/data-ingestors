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


def test_custom_label_column(texts_dir):
    _write(texts_dir, "s1", "Barack Obama")
    v = BIOLabelValidator(label_column="ner_tags")
    df = pd.DataFrame({"filename": ["s1"], "ner_tags": ["B-PER I-PER"]})
    result = v.validate(df)
    assert result.is_valid, result.errors


def test_filename_with_extension_not_double_appended(validator, texts_dir):
    # CSV filename already carries the extension -> must resolve sample1.txt,
    # not sample1.txt.txt.
    _write(texts_dir, "s1", "Paris is nice")
    df = pd.DataFrame({"filename": ["s1.txt"], "label": ["B-LOC O O"]})
    result = validator.validate(df)
    assert result.is_valid, result.errors


def test_resolution_matches_text_transfer_when_both_paths_exist(validator, texts_dir):
    # texts/s1 (bare) and texts/s1.txt both exist with different word
    # counts. text_transfer appends the extension for a bare CSV filename
    # and copies s1.txt, so validation must align tags against s1.txt —
    # never the bare file a filesystem probe would have found first.
    (texts_dir / "s1").write_text("one two three four five", encoding="utf-8")
    _write(texts_dir, "s1", "Paris is nice")  # writes s1.txt, 3 words

    df = pd.DataFrame({"filename": ["s1"], "label": ["B-LOC O O"]})  # 3 tags
    result = validator.validate(df)
    assert result.is_valid, result.errors

    # Tags sized to the bare file must fail — that file is never ingested.
    df_bad = pd.DataFrame({"filename": ["s1"], "label": ["O O O O O"]})  # 5 tags
    result_bad = validator.validate(df_bad)
    assert not result_bad.is_valid
    assert "count mismatch" in result_bad.errors[0]


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


# --- IOB2 sequence anomaly (warning, not a hard error) -----------------------


def test_iob2_orphan_i_warns_but_does_not_block(validator, texts_dir):
    # "I-PER" opens the entity (no preceding "B-PER"): malformed under IOB2,
    # legal under IOB1 -> warn, but still valid (5 tags / 5 words).
    _write(texts_dir, "s1", "John Smith works at Google")
    df = pd.DataFrame({"filename": ["s1"], "label": ["I-PER O O O B-ORG"]})
    result = validator.validate(df)
    assert result.is_valid, result.errors
    assert result.warnings and "IOB2 transition anomaly" in result.warnings[0]


def test_iob2_type_switch_warns(validator, texts_dir):
    # "I-ORG" right after "B-PER" is a type switch -> IOB2 anomaly.
    _write(texts_dir, "s1", "Foo Bar baz")
    df = pd.DataFrame({"filename": ["s1"], "label": ["B-PER I-ORG O"]})
    result = validator.validate(df)
    assert result.is_valid, result.errors
    assert result.warnings and "IOB2 transition anomaly" in result.warnings[0]


def test_iob2_well_formed_sequence_has_no_warning(validator, texts_dir):
    _write(texts_dir, "s1", "John Smith works")
    df = pd.DataFrame({"filename": ["s1"], "label": ["B-PER I-PER O"]})
    result = validator.validate(df)
    assert result.is_valid, result.errors
    assert not result.warnings


def test_iob2_check_skipped_when_tag_format_invalid(validator, texts_dir):
    # A format-invalid tag is the (hard) error; don't also emit the IOB2
    # sequence warning on garbage tags.
    _write(texts_dir, "s1", "John lives here")
    df = pd.DataFrame({"filename": ["s1"], "label": ["I-PER BOGUS O"]})
    result = validator.validate(df)
    assert not result.is_valid
    assert any("invalid BIO tag" in e for e in result.errors)
    assert not result.warnings


def test_csv_padded_header_still_resolves_label(texts_dir, tmp_path):
    """``filename, label`` — the header Excel writes by default — must resolve,
    not report the label column missing.

    Regression (backend#1828): BIOLabelValidator carried a private
    ``_resolve_column`` that lower-cased but did NOT strip, so pandas' parsed
    header ``[" label"]`` never matched the configured ``label``. The dataset
    was rejected for a column the user can see in their file, on
    token_classification only, while sibling validators in the same preflight
    run resolved it fine. Exercised through the real CSV read path (a raw file,
    not a DataFrame) because that is where the padding survives —
    ``BaseValidator._load_data`` calls ``pd.read_csv`` without
    ``skipinitialspace``.
    """
    _write(texts_dir, "s1", "John Smith")
    path = tmp_path / "padded.csv"
    path.write_text("filename, label\ns1,B-PER I-PER\n", encoding="utf-8")

    result = BIOLabelValidator().validate(str(path))

    assert result.is_valid, result.errors
    assert result.metadata["rows_checked"] == 1


def test_csv_padded_filename_header_still_resolves(texts_dir, tmp_path):
    """The same strip must apply to the ``filename`` column, which is padded
    whenever it is not the first CSV field (backend#1828)."""
    _write(texts_dir, "s1", "John Smith")
    path = tmp_path / "padded_fn.csv"
    path.write_text("label, filename\nB-PER I-PER,s1\n", encoding="utf-8")

    result = BIOLabelValidator().validate(str(path))

    assert result.is_valid, result.errors


# ---------------------------------------------------------------------------
# tracebloc/backend#3352 — a row with NO tags at all
#
# THE DEFECT THIS SECTION EXISTS FOR. The count check is a MISMATCH check:
# `word_count != len(tags)`. On a row whose label is whitespace-only and whose
# `.txt` is empty, both sides are zero, `0 == 0` passes, and the row is ingested
# as a training sample carrying no annotation — every token resolves to `-100`.
# The guard existed to enforce "one tag per word" and was satisfied vacuously on
# exactly the input it should reject.
#
# NOT ASSERTED AS AN ERROR COUNT, and that is the whole point. Measured on the
# three-row manifest below, BEFORE the fix and AFTER it:
#
#   before:  2 errors — BOTH from row 0 (a phantom "nan" tag plus the count
#            mismatch that phantom tag invents); row 1 passes silently
#   after:   2 errors — one per bad row
#
# So `len(errors) == 2` holds in both worlds and proves nothing. What separates
# them is WHICH ROWS are named, so that is what these assert.


def _rejected_rows(result):
    """The set of row indices named by the errors, e.g. ``{0, 1}``."""
    import re

    return {
        int(m.group(1)) for err in result.errors if (m := re.match(r"Row (\d+)", err))
    }


@pytest.fixture
def three_row_manifest(texts_dir):
    """One blank label, one whitespace-only label, one well-formed row.

    The blank and whitespace-only cells are DIFFERENT shapes and only one of
    them was ever caught: read through pandas a blank cell becomes ``NaN``
    (rejected, but as a bogus ``"nan"`` tag), while ``" "`` survives as a
    string and passed. Both are built here so a fix that handles one and not
    the other cannot look complete.
    """
    _write(texts_dir, "blank", "")
    _write(texts_dir, "white", "   ")
    _write(texts_dir, "good", "Hello Ada")
    return pd.DataFrame(
        {
            "filename": ["blank", "white", "good"],
            "label": [float("nan"), " ", "O B-PER"],
        }
    )


def test_a_row_with_no_tags_is_rejected_whether_blank_or_whitespace(
    validator, three_row_manifest
):
    result = validator.validate(three_row_manifest)
    rejected = _rejected_rows(result)
    # Asserted FIRST because it is the specific claim and it carries the
    # diagnosis. `not result.is_valid` and the error count are both true of
    # other, less interesting failures, and reddening on one of those instead
    # would hide which rows the validator actually let through.
    assert rejected == {0, 1}, (
        f"rows {sorted(rejected)} were rejected, expected {{0, 1}} — a row whose "
        "label holds no BIO tags was accepted as a training sample, because "
        "0 words against 0 tags satisfies the count check (backend#3352)"
    )
    assert not result.is_valid
    # A COUNT as well as the set, so a future edit that reports one row twice
    # instead of two rows once cannot pass: one error per rejected row. This is
    # not redundant with the set — measured: with only half the fix in place the
    # set is right and the count is 3, because the blank cell is reported both
    # as a phantom "nan" tag and as having no tags.
    assert len(result.errors) == len(rejected) == 2
    for err in result.errors:
        assert "holds no BIO tags" in err


def test_the_well_formed_row_is_not_swept_up(validator, three_row_manifest):
    """The other half of the invariant: refusing everything must not pass.

    Without this, a check that rejected every row would satisfy the test above.
    """
    result = validator.validate(three_row_manifest)
    assert 2 not in _rejected_rows(result)
    assert result.metadata["rows_checked"] == 3


def test_a_blank_label_says_the_label_is_empty_not_that_nan_is_a_bad_tag(
    validator, texts_dir
):
    """The blank cell WAS rejected — for the wrong reason, and twice.

    `str(NaN)` is the literal `"nan"`, which is tag-shaped, so the format check
    reported it as an invalid BIO tag and the count check then compared 1
    phantom tag against 0 words. Two errors, neither of them "your label is
    empty". A user reading them would go looking for a tag they never wrote.
    """
    _write(texts_dir, "s1", "")
    df = pd.DataFrame({"filename": ["s1"], "label": [float("nan")]})
    result = validator.validate(df)
    assert not result.is_valid
    assert len(result.errors) == 1, result.errors
    assert "holds no BIO tags" in result.errors[0]
    assert "invalid BIO tag" not in result.errors[0]
    assert "count mismatch" not in result.errors[0]
    assert "nan" not in result.errors[0]


def test_words_but_no_tags_is_rejected_and_names_the_word_count(validator, texts_dir):
    """The reachable-today shape: real text, label cell left empty.

    Distinguished from the empty/empty case by the word count in the message,
    because the two want different fixes — this one is a missing annotation,
    the other is a row that should not be in the manifest.
    """
    _write(texts_dir, "s1", "John lives here")
    df = pd.DataFrame({"filename": ["s1"], "label": ["   "]})
    result = validator.validate(df)
    assert not result.is_valid
    assert "holds no BIO tags" in result.errors[0]
    assert "3 word(s)" in result.errors[0]


def test_a_missing_text_file_still_wins_over_the_no_tags_error(validator, texts_dir):
    """Precedence, named and proved: file-missing outranks no-tags.

    Both conditions hold for this row. The file error wins because the word
    count the no-tags message quotes cannot be read without the file — so the
    ordering is forced, not a preference. The case where the first check is
    present but NOT decisive is covered by the tests above, where the file
    exists and the no-tags error is the one that fires.
    """
    df = pd.DataFrame({"filename": ["absent"], "label": [" "]})
    result = validator.validate(df)
    assert not result.is_valid
    assert len(result.errors) == 1, result.errors
    assert "not found" in result.errors[0]
    assert "holds no BIO tags" not in result.errors[0]


def test_a_list_valued_label_is_malformed_not_missing(validator, texts_dir):
    """`pd.isna` returns an ARRAY for a list-like, and `bool()` of that raises.

    A list-valued cell is not a missing label, it is a malformed one, so it
    must reach the format check rather than crash or be waved through as
    "no tags". Guards the `is_scalar` half of the absence test.
    """
    _write(texts_dir, "s1", "John lives here")
    df = pd.DataFrame({"filename": ["s1"], "label": [["B-PER", "O", "O"]]})
    result = validator.validate(df)  # must not raise
    assert not result.is_valid
    assert "invalid BIO tag" in result.errors[0]


def test_pd_na_counts_as_missing_too(validator, texts_dir):
    """The nullable-dtype flavour of absence, not just float NaN."""
    _write(texts_dir, "s1", "")
    df = pd.DataFrame({"filename": ["s1"], "label": pd.array([None], dtype="string")})
    result = validator.validate(df)
    assert not result.is_valid
    assert "holds no BIO tags" in result.errors[0]
