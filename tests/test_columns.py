"""Tests for the shared column-name resolution rule (#340).

``resolve_column`` is the single source of truth for matching a declared
column name to the actual header, used by BOTH the validators
(``BaseValidator._match_column`` delegates here) and the ingest read path
(``BaseIngestor._resolve_label_column``). The two used to diverge — that was
the #340 silent-null-label bug.
"""

import pandas as pd
import pytest

from tracebloc_ingestor.utils.columns import resolve_column
from tracebloc_ingestor.validators.base import BaseValidator


@pytest.mark.parametrize(
    "cols,name,expected",
    [
        (["a", "b"], "a", "a"),                     # exact match wins
        (["Label", "feat"], "label", "Label"),      # case-insensitive; returns original spelling
        ([" label ", "n"], "label", " label "),     # whitespace-insensitive; returns raw spelling
        (["Age", "Income"], "AGE", "Age"),           # fold on both sides
        (["label"], " label ", "label"),             # whitespace on the query side too
        (["a", "b"], "missing", None),               # nothing matches
        ([], "x", None),                             # empty columns
    ],
)
def test_resolve_column(cols, name, expected):
    assert resolve_column(cols, name) == expected


def test_resolve_column_accepts_pandas_columns_and_dict_keys():
    assert resolve_column(pd.DataFrame({"Label": [1]}).columns, "label") == "Label"
    assert resolve_column({"Label": 1, "n": 2}.keys(), "label") == "Label"


def test_validator_match_column_delegates_to_resolve_column():
    # Pin the single-source contract: the validator helper must produce the
    # same answer as the shared rule for the same inputs (#340).
    for cols, name in ([["Label", "feat"], "label"], [[" id "], "ID"], [["x"], "y"]):
        assert BaseValidator._match_column(cols, name) == resolve_column(cols, name)
