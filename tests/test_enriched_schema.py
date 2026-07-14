"""Tests for the enriched-schema emission (data-ingestors#360 slice 1b).

The emitted ``schema`` gains an optional per-column object shape
``{col: {dtype, role}}`` behind the ``EMIT_ENRICHED_SCHEMA`` gate — a breaking
wire-format change cut over with backend#1037. Default stays the flat
``{col: SQL_type}`` map. The framework ``label`` column carries
``role: "target"`` for supervised tasks so combine-time alignment identifies
the prediction target from the contract instead of inferring it.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor


def make_ingestor(*, enriched: bool, label_column=None):
    db = MagicMock()
    db.config = Config(EMIT_ENRICHED_SCHEMA=enriched)
    api = MagicMock()
    return CSVIngestor(
        database=db,
        api_client=api,
        table_name="tbl",
        schema={"a": "INT"},
        intent="train",
        category=None,
        label_column=label_column,
    )


# ---- the config gate ----------------------------------------------------


def test_gate_defaults_off(monkeypatch):
    monkeypatch.delenv("EMIT_ENRICHED_SCHEMA", raising=False)
    assert Config().EMIT_ENRICHED_SCHEMA is False


@pytest.mark.parametrize("val", ["1", "true", "TRUE", "yes", "On"])
def test_gate_truthy_env(monkeypatch, val):
    monkeypatch.setenv("EMIT_ENRICHED_SCHEMA", val)
    assert Config().EMIT_ENRICHED_SCHEMA is True


@pytest.mark.parametrize("val", ["0", "false", "no", "", "maybe"])
def test_gate_falsy_env(monkeypatch, val):
    monkeypatch.setenv("EMIT_ENRICHED_SCHEMA", val)
    assert Config().EMIT_ENRICHED_SCHEMA is False


def test_gate_override_wins_over_env(monkeypatch):
    monkeypatch.setenv("EMIT_ENRICHED_SCHEMA", "true")
    assert Config(EMIT_ENRICHED_SCHEMA=False).EMIT_ENRICHED_SCHEMA is False


# ---- the schema payload -------------------------------------------------


def test_flat_schema_unchanged_when_gate_off():
    ing = make_ingestor(enriched=False, label_column="y")
    flat = {"a": "INT", "b": "VARCHAR(255)", "label": "VARCHAR(255)"}
    # Identity: the legacy shape ships verbatim, target unmarked.
    assert ing._schema_payload(flat) == flat


def test_enriched_shape_and_target_role_for_supervised():
    ing = make_ingestor(enriched=True, label_column="y")
    flat = {"a": "INT", "b": "VARCHAR(255)", "label": "VARCHAR(255)"}

    out = ing._schema_payload(flat)

    assert out["a"] == {"dtype": "INT"}
    assert out["b"] == {"dtype": "VARCHAR(255)"}
    # `label` is the framework target column → role:"target" for supervised.
    assert out["label"] == {"dtype": "VARCHAR(255)", "role": "target"}


def test_no_target_role_for_self_supervised():
    # No label_column (e.g. masked language modeling) → the `label` column is
    # present but is not a prediction target, so it must not be marked.
    ing = make_ingestor(enriched=True, label_column=None)
    out = ing._schema_payload({"a": "INT", "label": "VARCHAR(255)"})

    assert out["label"] == {"dtype": "VARCHAR(255)"}
    assert "role" not in out["label"]


def test_enriched_does_not_mutate_input():
    ing = make_ingestor(enriched=True, label_column="y")
    flat = {"a": "INT", "label": "VARCHAR(255)"}
    ing._schema_payload(flat)
    # The caller's dict is untouched (values still strings).
    assert flat == {"a": "INT", "label": "VARCHAR(255)"}
