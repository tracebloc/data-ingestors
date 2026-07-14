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


def make_ingestor(*, enriched: bool, label_column=None, file_options=None):
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
        file_options=file_options,
    )


# ---- the config gate ----------------------------------------------------


def test_gate_defaults_on(monkeypatch):
    # Default is now ON — backend#1037 / engine#460 depend on the enriched shape.
    monkeypatch.delenv("EMIT_ENRICHED_SCHEMA", raising=False)
    assert Config().EMIT_ENRICHED_SCHEMA is True


def test_gate_empty_env_is_unset_defaults_on(monkeypatch):
    # An empty value is treated as unset (not a falsey override) -> default on.
    monkeypatch.setenv("EMIT_ENRICHED_SCHEMA", "")
    assert Config().EMIT_ENRICHED_SCHEMA is True


@pytest.mark.parametrize("val", ["1", "true", "TRUE", "yes", "On"])
def test_gate_truthy_env(monkeypatch, val):
    monkeypatch.setenv("EMIT_ENRICHED_SCHEMA", val)
    assert Config().EMIT_ENRICHED_SCHEMA is True


@pytest.mark.parametrize("val", ["0", "false", "no", "maybe"])
def test_gate_falsy_env(monkeypatch, val):
    # A non-empty, non-truthy value forces the legacy flat map back off.
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
    flat = {"a": "INT", "b": "VARCHAR(255)", "label": "DECIMAL(10,2)"}

    out = ing._schema_payload(flat)

    # dtype is the CANONICAL logical type, not the raw storage type; every column
    # states the ingested null encoding (missing → SQL NULL).
    assert out["a"] == {"dtype": "int", "null_encoding": "null"}
    assert out["b"] == {"dtype": "string", "null_encoding": "null"}
    # `label` is the framework target column → role:"target" for supervised.
    assert out["label"] == {
        "dtype": "float",
        "role": "target",
        "null_encoding": "null",
    }


def test_dtype_is_canonicalized_across_storage_variants():
    # Two datasets whose "same" column differs only in storage width/size must
    # canonicalize to the SAME logical dtype (so #1037 doesn't spuriously block).
    ing = make_ingestor(enriched=True, label_column=None)
    a = ing._schema_payload({"name": "VARCHAR(255)", "n": "BIGINT"})
    b = ing._schema_payload({"name": "VARCHAR(100)", "n": "INT"})
    assert a["name"]["dtype"] == b["name"]["dtype"] == "string"
    assert a["n"]["dtype"] == b["n"]["dtype"] == "int"


def test_no_target_role_for_self_supervised():
    # No label_column (e.g. masked language modeling) → the `label` column is
    # present but is not a prediction target, so it must not be marked.
    ing = make_ingestor(enriched=True, label_column=None)
    out = ing._schema_payload({"a": "INT", "label": "VARCHAR(255)"})

    assert out["label"] == {"dtype": "string", "null_encoding": "null"}
    assert "role" not in out["label"]


def test_declared_unit_and_ordinal_merged_into_descriptors():
    # Uploader-declared column descriptors (di#360) ride on file_options and are
    # merged into the enriched schema for columns that exist in it.
    ing = make_ingestor(
        enriched=True,
        label_column="y",
        file_options={
            "column_descriptors": {
                "a": {"unit": "years"},
                "b": {"ordinal": ["low", "med", "high"]},
            }
        },
    )
    out = ing._schema_payload({"a": "INT", "b": "VARCHAR(10)"})
    assert out["a"] == {"dtype": "int", "null_encoding": "null", "unit": "years"}
    assert out["b"] == {
        "dtype": "string",
        "null_encoding": "null",
        "ordinal": ["low", "med", "high"],
    }


def test_declared_descriptor_for_unknown_column_ignored():
    ing = make_ingestor(
        enriched=True,
        file_options={"column_descriptors": {"ghost": {"unit": "kg"}}},
    )
    out = ing._schema_payload({"a": "INT"})
    assert out == {"a": {"dtype": "int", "null_encoding": "null"}}


def test_bool_and_null_encoding():
    ing = make_ingestor(enriched=True, label_column=None)
    out = ing._schema_payload({"flag": "BOOLEAN", "age": "INT", "name": "VARCHAR(9)"})

    # Bool columns state the stored 1/0 encoding; non-bool columns don't.
    assert out["flag"] == {
        "dtype": "bool",
        "null_encoding": "null",
        "bool_encoding": "1/0",
    }
    assert "bool_encoding" not in out["age"]
    assert "bool_encoding" not in out["name"]
    # Every column states the null encoding.
    assert all(d["null_encoding"] == "null" for d in out.values())


def test_declared_descriptors_ignored_when_gate_off():
    # Descriptors are part of the enriched contract; the flat legacy shape
    # carries none.
    ing = make_ingestor(
        enriched=False,
        file_options={"column_descriptors": {"a": {"unit": "years"}}},
    )
    assert ing._schema_payload({"a": "INT"}) == {"a": "INT"}


def test_enriched_does_not_mutate_input():
    ing = make_ingestor(enriched=True, label_column="y")
    flat = {"a": "INT", "label": "VARCHAR(255)"}
    ing._schema_payload(flat)
    # The caller's dict is untouched (values still storage-type strings).
    assert flat == {"a": "INT", "label": "VARCHAR(255)"}
