"""Shared per-column NA policy in CSVIngestor (and the DataValidator gate).

Before #237 the NA set was chosen by *category*: the tabular family treated
"NA"/"NULL"/"None" as missing, every other category kept them as literal
strings — so a numeric column with an "NA" cell PASSED the validator (which
read with pandas defaults) and then CRASHED the ingestor's numeric cast
(validate-pass -> ingest-crash).

The policy now lives in one place — ``coercion.build_csv_na_values`` — used by
both the ingestor read and the validator gate, so the two can't disagree.
Every *schema* column treats ""/NA/null/None as missing; the framework's own
filename/mask_id columns are protected (a file named "NA.jpg" survives).
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
from tracebloc_ingestor.utils import coercion
from tracebloc_ingestor.utils.constants import TaskCategory


def _make_ingestor(category, schema=None):
    return CSVIngestor(
        database=MagicMock(),
        api_client=MagicMock(),
        table_name="t",
        schema=schema or {"feature_a": "INT", "label": "VARCHAR(50)"},
        category=category,
        label_column="label",
    )


# ── build_csv_na_values: the single source of truth ─────────────────────────


def test_na_builder_covers_every_schema_column():
    na = coercion.build_csv_na_values({"a": "INT", "b": "VARCHAR(10)", "t": "DATE"})
    assert set(na) == {"a", "b", "t"}
    # Numeric, string AND date columns alike get the full sentinel set.
    for col in na:
        assert na[col] == list(coercion.NA_SENTINELS)
    assert "NA" in na["a"] and "" in na["a"] and "null" in na["a"]


def test_na_builder_omits_non_schema_columns():
    """filename / mask_id aren't schema columns, so they're omitted. With
    keep_default_na=False that means no NA coercion — a file named 'NA' keeps
    its name."""
    na = coercion.build_csv_na_values({"feature_a": "INT"})
    assert "filename" not in na
    assert "mask_id" not in na


# ── the #237 fix: identical verdict regardless of category ──────────────────


@pytest.mark.parametrize(
    "category",
    [
        TaskCategory.TABULAR_CLASSIFICATION,
        TaskCategory.IMAGE_CLASSIFICATION,
        TaskCategory.TEXT_CLASSIFICATION,
    ],
)
def test_na_token_in_numeric_column_is_missing_for_every_category(category, tmp_path):
    """An 'NA'/'null' cell in a numeric column parses as NaN (-> NULL) for
    EVERY category, not just tabular. Before #237 the non-tabular path kept
    'NA' as a literal string and then crashed the numeric type-cast."""
    p = tmp_path / "x.csv"
    p.write_text("filename,score\nimg1,1.5\nimg2,NA\nimg3,null\n")
    ing = _make_ingestor(category, schema={"score": "FLOAT"})

    records = list(ing.read_data(str(p)))  # must not raise for ANY category

    scores = [r["score"] for r in records]
    assert scores[0] == 1.5
    assert pd.isna(scores[1]) and pd.isna(scores[2])


def test_read_uses_per_column_dict_and_keep_default_na_false(tmp_path):
    """The read passes the per-column na_values dict + keep_default_na=False,
    so NA detection is driven entirely by the schema (pandas' global default
    set never reaches a protected column)."""
    p = tmp_path / "x.csv"
    p.write_text("feature_a,label\n1,a\n")
    ing = _make_ingestor(TaskCategory.IMAGE_CLASSIFICATION)

    with patch.object(pd, "read_csv", return_value=iter([])) as mock_read:
        list(ing.read_data(str(p)))

    _, kwargs = mock_read.call_args
    assert kwargs["keep_default_na"] is False
    assert isinstance(kwargs["na_values"], dict)
    assert kwargs["na_values"]["feature_a"] == list(coercion.NA_SENTINELS)


def test_filename_named_NA_survives(tmp_path):
    """A non-schema 'filename' column holding 'NA' is NOT coerced to missing —
    it's the framework's structural column, not a declared schema column, so a
    file genuinely named 'NA' keeps its name."""
    p = tmp_path / "x.csv"
    p.write_text("filename,score\nNA,1.5\n")
    ing = _make_ingestor(TaskCategory.IMAGE_CLASSIFICATION, schema={"score": "FLOAT"})

    records = list(ing.read_data(str(p)))

    assert records[0]["filename"] == "NA"


def test_string_schema_column_treats_na_tokens_as_missing(tmp_path):
    """A declared VARCHAR schema column still treats NA/NULL as missing — the
    team's existing convention (a *schema* string column of "NA" is missing,
    unlike the structural filename column). Locks the boundary so a future
    change is conscious."""
    p = tmp_path / "x.csv"
    p.write_text("a,b\n1,NA\n2,NULL\n3,hello\n")
    ing = _make_ingestor(
        TaskCategory.TABULAR_CLASSIFICATION,
        schema={"a": "INT", "b": "VARCHAR(10)"},
    )

    records = list(ing.read_data(str(p)))

    bs = [r["b"] for r in records]
    assert bs[0] is None  # "NA"   -> NULL (VARCHAR cast maps NaN -> None)
    assert bs[1] is None  # "NULL" -> NULL
    assert bs[2] == "hello"
