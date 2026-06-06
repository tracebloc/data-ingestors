"""Per-category na_values defaults inside CSVIngestor.

Templates in ``templates/tabular_*/`` and ``templates/time_*/`` pass
``na_values=["", "NA", "NULL", "None"]`` via csv_options. The YAML path
can't express that key (schema restricts ``spec.csv_options`` to a small
whitelist), so the ingestor itself widens its internal default for
tabular-family categories. Other categories keep the narrower ``[""]``.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
from tracebloc_ingestor.utils.constants import TaskCategory


def _make_ingestor(category):
    return CSVIngestor(
        database=MagicMock(),
        api_client=MagicMock(),
        table_name="t",
        schema={"feature_a": "str", "label": "str"},
        category=category,
        label_column="label",
    )


@pytest.mark.parametrize(
    "category",
    [
        TaskCategory.TABULAR_CLASSIFICATION,
        TaskCategory.TABULAR_REGRESSION,
        TaskCategory.TIME_SERIES_FORECASTING,
        TaskCategory.TIME_TO_EVENT_PREDICTION,
    ],
)
def test_tabular_family_uses_wider_na_values(category, tmp_path):
    """Tabular-family ingestors must call pd.read_csv with the wider
    na_values set so 'NA'/'NULL'/'None' string cells parse as NaN."""
    csv_path = tmp_path / "x.csv"
    csv_path.write_text("feature_a,label\n1,a\n")
    ing = _make_ingestor(category)

    with patch.object(pd, "read_csv", return_value=iter([])) as mock_read:
        list(ing.read_data(str(csv_path)))

    _, kwargs = mock_read.call_args
    assert kwargs["na_values"] == ["", "NA", "NULL", "None"]
    # keep_default_na=True for tabular so pandas' full NA set (NaN/N/A/null/...)
    # is recognised — matching the validators' read, which closes the gate-lie
    # where a file passes validation but crashes the ingestor's type-conversion.
    assert kwargs["keep_default_na"] is True


def test_non_tabular_keeps_narrow_na_values(tmp_path):
    """Image / text categories don't get the wider sentinel."""
    csv_path = tmp_path / "x.csv"
    csv_path.write_text("feature_a,label\n1,a\n")
    ing = _make_ingestor(TaskCategory.IMAGE_CLASSIFICATION)

    with patch.object(pd, "read_csv", return_value=iter([])) as mock_read:
        list(ing.read_data(str(csv_path)))

    _, kwargs = mock_read.call_args
    assert kwargs["na_values"] == [""]
    assert kwargs["keep_default_na"] is False


def test_tabular_na_token_in_numeric_column_does_not_crash(tmp_path):
    """A 'NaN'/'N/A' cell in a numeric column used to PASS the validator but then
    crash the ingestor's numeric type-conversion (the validator read it as NaN,
    the ingestor kept it as text). With keep_default_na=True both agree: the cell
    parses as NaN, read_data succeeds, and the value is missing."""
    csv_path = tmp_path / "x.csv"
    csv_path.write_text("x,label\n1.5,a\nNaN,b\nN/A,c\n")
    ing = CSVIngestor(
        database=MagicMock(),
        api_client=MagicMock(),
        table_name="t",
        schema={"x": "FLOAT", "label": "str"},
        category=TaskCategory.TABULAR_CLASSIFICATION,
        label_column="label",
    )
    records = list(ing.read_data(str(csv_path)))  # must not raise
    xs = [r["x"] for r in records]
    assert xs[0] == 1.5
    assert pd.isna(xs[1]) and pd.isna(xs[2])
