"""Cross-layer coercion consistency — the validator gate and the ingest paths
must reach the SAME verdict on the same value (#236, #237).

This is the regression net for the whole bug class: the three layers
(DataValidator gate, CSVIngestor cast, JSONIngestor per-record check) used to
decide independently what a type permits and what counts as missing, so they
drifted (#189/#204) and disagreed — a file passed validation and then crashed
mid-ingest. Each case below pins one cell of the matrix
(edge-case × layer) so a future divergence fails here instead of in a
customer's cluster.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
from tracebloc_ingestor.ingestors.json_ingestor import _validate_value_against_dtype
from tracebloc_ingestor.utils import coercion
from tracebloc_ingestor.utils.constants import TaskCategory
from tracebloc_ingestor.validators.data_validator import DataValidator

BIG_INT = "99999999999999999999999999"  # 26 digits, well beyond signed int64


def _validate(schema, csv_text, tmp_path):
    p = tmp_path / "v.csv"
    p.write_text(csv_text)
    return DataValidator(schema=schema).validate(str(p))


def _csv_ingestor(schema, category=TaskCategory.TABULAR_CLASSIFICATION):
    return CSVIngestor(
        database=MagicMock(),
        api_client=MagicMock(),
        table_name="t",
        schema=schema,
        category=category,
    )


# ── #236: out-of-int64 value in an INT column — REJECTED everywhere ─────────


def test_236_int_overflow_unit_helpers():
    assert coercion.int_value_overflows(BIG_INT) is True
    assert coercion.int_value_overflows("5") is False
    # Non-numeric and non-finite inputs are not this helper's concern (the
    # caller's non-numeric / finite checks handle them) — it returns False.
    assert coercion.int_value_overflows("abc") is False
    assert coercion.int_value_overflows(None) is False
    assert coercion.int_value_overflows(float("inf")) is False
    err = coercion.int_range_error(pd.Series([BIG_INT, "5"]), "n", "INT")
    assert err is not None and "64-bit" in err and "BIGINT" in err
    # A valid FLOAT-sized value is NOT an INT overflow concern only for INT/BIGINT
    assert coercion.int_range_error(pd.Series([BIG_INT]), "n", "FLOAT") is None


def test_236_validator_rejects_int_overflow(tmp_path):
    res = _validate({"id": "INT", "n": "INT"}, f"id,n\n1,{BIG_INT}\n2,5\n", tmp_path)
    assert not res.is_valid
    assert "64-bit" in " ".join(res.errors)


def test_236_csv_ingest_rejects_int_overflow_cleanly(tmp_path):
    """The cryptic numpy "ufunc 'isinf'" / pandas "Integer out of range" is
    replaced by a clear, actionable message — and the ingest aborts rather than
    crashing or corrupting."""
    p = tmp_path / "x.csv"
    p.write_text(f"id,n\n1,{BIG_INT}\n2,5\n")
    ing = _csv_ingestor({"id": "INT", "n": "INT"})
    with pytest.raises(ValueError, match="64-bit"):
        list(ing.read_data(str(p)))


def test_236_json_detects_int_overflow():
    with pytest.raises(ValueError, match="64-bit"):
        _validate_value_against_dtype(BIG_INT, "INT")
    # BIGINT column: still rejected (its ceiling is int64 too), but no
    # "declare BIGINT" hint.
    with pytest.raises(ValueError, match="64-bit") as exc:
        _validate_value_against_dtype(BIG_INT, "BIGINT")
    assert "declare" not in str(exc.value).lower()


def test_236_big_value_in_float_column_is_accepted(tmp_path):
    """A 26-digit value is a valid FLOAT (1e26) — the gate accepts it and the
    ingest stores it. Only INT/BIGINT columns reject it."""
    schema = {"id": "INT", "x": "FLOAT"}
    res = _validate(schema, f"id,x\n1,{BIG_INT}\n", tmp_path)
    assert res.is_valid, res.errors

    p = tmp_path / "x.csv"
    p.write_text(f"id,x\n1,{BIG_INT}\n")
    records = list(_csv_ingestor(schema).read_data(str(p)))  # must not raise
    assert float(records[0]["x"]) > 1e25


# ── #237: NA token in a numeric column — MISSING everywhere ─────────────────


@pytest.mark.parametrize(
    "category",
    [TaskCategory.TABULAR_CLASSIFICATION, TaskCategory.IMAGE_CLASSIFICATION],
)
def test_237_na_in_numeric_passes_gate_and_ingests_as_missing(category, tmp_path):
    """'NA' in a numeric column: the gate passes it (missing), and the ingest
    yields it as missing — for tabular AND non-tabular alike. Before the fix
    the non-tabular ingest crashed on the same file the gate had passed."""
    schema = {"score": "INT"}
    csv_text = "filename,score\nimg1,7\nimg2,NA\n"

    res = _validate(schema, csv_text, tmp_path)
    assert res.is_valid, res.errors

    p = tmp_path / "x.csv"
    p.write_text(csv_text)
    records = list(_csv_ingestor(schema, category).read_data(str(p)))
    assert pd.isna(records[1]["score"])


def test_237_genuine_non_numeric_still_rejected_both_layers(tmp_path):
    """The NA-tolerance must not swallow a genuinely bad token: 'abc' in an INT
    column is rejected by the gate AND the ingest (it's data corruption, not a
    missing marker)."""
    schema = {"id": "INT", "n": "INT"}
    csv_text = "id,n\n1,abc\n2,5\n"

    res = _validate(schema, csv_text, tmp_path)
    assert not res.is_valid

    p = tmp_path / "x.csv"
    p.write_text(csv_text)
    with pytest.raises(ValueError):
        list(_csv_ingestor(schema).read_data(str(p)))
