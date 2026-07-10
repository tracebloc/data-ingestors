"""time_series_classification (backend#1054 WS1) — the sequence-grouped
ingest category, end to end at the unit level.

Covers, per the ticket's done-contract:
- the grouped validator set composition (map_validators);
- the ``grouping`` ModalitySpec trait and its layout-contract emission;
- the 3-patient toy CSV scenario (T=5/3/7 -> 15 rows): sequence-unit label
  counts ({"0": 2, "1": 1}), ``meta_data.number_of_sequences == 3``, the
  composite (sequence_id, timestamp) index request;
- the four negative fixtures, each rejected with a readable message
  (mid-group label flip / non-monotonic timestamps / null sequence_id /
  data_id=column pointed at sequence_id);
- the post-insert group-integrity pass (T5);
- the two new Database helpers' SQL (COUNT(DISTINCT …), scoped IN-delete).

The MySQL-backed row/index assertions live in e2e/test_ingest_e2e.py
(test_tsc_sequence_semantics); this file mocks the DB boundary like the rest
of the unit suite.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.database import Database
from tracebloc_ingestor.ingestors import base as base_mod
from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
from tracebloc_ingestor.modalities import REGISTRY, spec_for
from tracebloc_ingestor.modalities.layout import build_layout_contract
from tracebloc_ingestor.utils.constants import DataFormat, TaskCategory
from tracebloc_ingestor.utils.validators_mapping import map_validators
from tracebloc_ingestor.validators.data_validator import DataValidator
from tracebloc_ingestor.validators.duplicate_validator import DuplicateValidator
from tracebloc_ingestor.validators.ingestable_records_validator import (
    IngestableRecordsValidator,
)
from tracebloc_ingestor.validators.label_constant_within_group_validator import (
    LabelConstantWithinGroupValidator,
)
from tracebloc_ingestor.validators.label_diversity_validator import (
    LabelDiversityValidator,
)
from tracebloc_ingestor.validators.numeric_columns_validator import (
    NumericColumnsValidator,
)
from tracebloc_ingestor.validators.per_group_time_ordered_validator import (
    PerGroupTimeOrderedValidator,
)
from tracebloc_ingestor.validators.sequence_group_validator import (
    SequenceGroupValidator,
)
from tracebloc_ingestor.validators.table_name_validator import TableNameValidator

TSC = TaskCategory.TIME_SERIES_CLASSIFICATION

SCHEMA = {
    "sequence_id": "VARCHAR(64)",
    "timestamp": "TIMESTAMP",
    "heart_rate": "FLOAT",
    "label": "INT",
}


def _toy_frame() -> pd.DataFrame:
    """The done-contract toy: 3 patients, T = 5/3/7, labels 1/0/0 -> 15 rows,
    sequence-unit labels {"1": 1, "0": 2}."""
    rows = []
    for pid, T, label in (("p1", 5, 1), ("p2", 3, 0), ("p3", 7, 0)):
        rows += [
            {
                "sequence_id": pid,
                "timestamp": f"2024-01-01 {8 + t:02d}:00:00",
                "heart_rate": 70.0 + t,
                "label": label,
            }
            for t in range(T)
        ]
    return pd.DataFrame(rows)


def _make_ingestor(csv_path, **overrides):
    db = MagicMock(name="Database")
    # Real Config so the config-reading validators (TableName, Duplicate)
    # behave: TABLE_NAME set; DEST_PATH (/data/shared/<table>) won't exist.
    db.config = Config(TABLE_NAME="tsc_toy")
    db.create_table.return_value = MagicMock(name="table")
    db.insert_batch.side_effect = lambda table, batch: (
        list(range(len(batch))),
        [],
    )
    db.get_table_schema.return_value = dict(SCHEMA)
    db.get_label_sequence_counts.return_value = {"0": 2, "1": 1}
    db.get_samples.return_value = []
    api = MagicMock(name="APIClient")
    api.config.TITLE = "toy tsc"
    api.send_ingest_summary.return_value = {"dataset_id": 1}

    kwargs = dict(
        database=db,
        api_client=api,
        table_name="tsc_toy",
        schema=dict(SCHEMA),
        label_column="label",
        intent="train",
        category=TSC,
        data_format=DataFormat.TABULAR,
    )
    kwargs.update(overrides)
    return CSVIngestor(**kwargs)


def _run_full_ingest(ing, csv_path):
    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        return ing.ingest(str(csv_path), batch_size=50)


# ---------------------------------------------------------------------------
# Registry trait + layout contract
# ---------------------------------------------------------------------------


def test_spec_grouping_trait_pinned():
    spec = spec_for(TSC)
    assert spec.is_tabular_family and not spec.is_file_bearing
    assert spec.is_classification and not spec.is_self_supervised
    assert spec.data_format == DataFormat.TABULAR
    g = spec.grouping
    assert g is not None
    assert g.group_column == "sequence_id"
    assert g.time_column == "timestamp"
    assert g.count_unit == "sequences"


def test_grouping_is_unique_to_tsc():
    # Decision-4: today exactly one grouped category. A second one is a
    # deliberate registry decision — update this test when it lands.
    grouped = {c for c, s in REGISTRY.items() if s.grouping is not None}
    assert grouped == {TSC}


def test_layout_contract_carries_grouping():
    doc = build_layout_contract()
    assert doc["version"] == "2"  # shape change: the grouping block
    tsc = doc["tasks"][TSC]
    assert tsc["grouping"] == {
        "group_column": "sequence_id",
        "time_column": "timestamp",
        "count_unit": "sequences",
    }
    # Non-grouped categories emit an explicit null (mirrors record_format).
    assert doc["tasks"][TaskCategory.TABULAR_CLASSIFICATION]["grouping"] is None
    assert doc["tasks"][TaskCategory.TIME_SERIES_FORECASTING]["grouping"] is None


# ---------------------------------------------------------------------------
# Validator set composition
# ---------------------------------------------------------------------------


def test_validator_set_composition():
    validators = map_validators(TSC, {"schema": dict(SCHEMA), "label_column": "label"})
    assert [type(v) for v in validators] == [
        IngestableRecordsValidator,
        SequenceGroupValidator,
        LabelConstantWithinGroupValidator,
        PerGroupTimeOrderedValidator,
        NumericColumnsValidator,
        DataValidator,
        LabelDiversityValidator,  # is_classification=True
        TableNameValidator,
        DuplicateValidator,
    ]


def test_numeric_validator_excludes_sequence_columns():
    validators = map_validators(TSC, {"schema": dict(SCHEMA)})
    numeric = next(v for v in validators if isinstance(v, NumericColumnsValidator))
    assert numeric.excluded_columns == frozenset({"sequence_id", "timestamp"})


def test_data_validator_gets_schema_without_timestamp():
    validators = map_validators(TSC, {"schema": dict(SCHEMA)})
    dv = next(v for v in validators if isinstance(v, DataValidator))
    assert "timestamp" not in dv.schema
    assert "sequence_id" in dv.schema  # VARCHAR — DataValidator handles it


def test_factory_threads_unique_id_column_to_guard():
    validators = map_validators(
        TSC, {"schema": dict(SCHEMA), "unique_id_column": "sequence_id"}
    )
    guard = next(v for v in validators if isinstance(v, SequenceGroupValidator))
    assert guard.unique_id_column == "sequence_id"


# ---------------------------------------------------------------------------
# Done-contract scenario: 3-patient toy CSV, mocked DB boundary
# ---------------------------------------------------------------------------


def test_toy_csv_ingests_with_sequence_unit_counts(make_csv, monkeypatch):
    csv_path = make_csv(_toy_frame(), name="toy.csv")
    ing = _make_ingestor(csv_path)
    failed = _run_full_ingest(ing, csv_path)

    assert failed == []
    # All 15 rows hit the DB write path (row unit for storage) …
    inserted = sum(
        len(call.args[1]) for call in ing.database.insert_batch.call_args_list
    )
    assert inserted == 15
    # … while label counts were queried SEQUENCE-unit, on the grouped helper,
    # never the row-unit one (T2).
    ing.database.get_label_sequence_counts.assert_called_once_with(
        "tsc_toy", ing.ingestor_id, group_column="sequence_id"
    )
    ing.database.get_label_counts.assert_not_called()

    # The summary carries the per-sequence labels payload and
    # number_of_sequences in meta_data (serializer unchanged — counts only,
    # Decision-3).
    summary_kwargs = ing.api_client.send_ingest_summary.call_args.kwargs
    assert summary_kwargs["labels"] == {"0": 2, "1": 1}
    assert summary_kwargs["meta_data"]["number_of_sequences"] == 3
    assert summary_kwargs["category"] == TSC
    # number_of_columns still counts the CLEANED schema columns (k=2 site:
    # sequence_id + timestamp + F features; label stripped).
    assert summary_kwargs["meta_data"]["number_of_columns"] == 3


def test_toy_csv_requests_composite_index(make_csv):
    csv_path = make_csv(_toy_frame(), name="toy.csv")
    ing = _make_ingestor(csv_path)
    _run_full_ingest(ing, csv_path)
    create_kwargs = ing.database.create_table.call_args.kwargs
    assert create_kwargs["index_columns"] == ["sequence_id", "timestamp"]


def test_non_grouped_category_keeps_row_counts(make_csv):
    # Control: tabular_classification still uses the row-unit helper and no
    # index request — the grouped behavior is trait-gated, not global.
    df = _toy_frame().drop(columns=["sequence_id", "timestamp"])
    csv_path = make_csv(df, name="tab.csv")
    ing = _make_ingestor(
        csv_path,
        category=TaskCategory.TABULAR_CLASSIFICATION,
        schema={"heart_rate": "FLOAT", "label": "INT"},
    )
    ing.database.get_label_counts.return_value = {"0": 10, "1": 5}
    _run_full_ingest(ing, csv_path)
    ing.database.get_label_counts.assert_called_once()
    ing.database.get_label_sequence_counts.assert_not_called()
    assert ing.database.create_table.call_args.kwargs["index_columns"] is None
    meta = ing.api_client.send_ingest_summary.call_args.kwargs["meta_data"]
    assert "number_of_sequences" not in meta


# ---------------------------------------------------------------------------
# The four negative fixtures — readable rejections (done-contract)
# ---------------------------------------------------------------------------


def _reject(make_csv, df, match, **ingestor_overrides):
    csv_path = make_csv(df, name="bad.csv")
    ing = _make_ingestor(csv_path, **ingestor_overrides)
    with pytest.raises(ValueError, match=match) as exc:
        with patch.object(base_mod, "Session"):
            ing.ingest(str(csv_path))
    # Rejected before any table/row exists (#260).
    ing.database.create_table.assert_not_called()
    ing.database.insert_batch.assert_not_called()
    return str(exc.value)


def test_negative_mid_group_label_flip(make_csv):
    df = _toy_frame()
    df.loc[10, "label"] = 1  # p3 flips mid-sequence
    msg = _reject(make_csv, df, match="changes mid-sequence")
    assert "ONE label per" in msg


def test_negative_non_monotonic_timestamps(make_csv):
    df = _toy_frame()
    df.loc[6, "timestamp"] = "2024-01-01 23:00:00"  # p2 jumps back afterwards
    msg = _reject(make_csv, df, match="out-of-order")
    assert "sort each sequence" in msg.lower()


def test_negative_null_sequence_id(make_csv):
    df = _toy_frame()
    df.loc[3, "sequence_id"] = None
    msg = _reject(make_csv, df, match="null/empty")
    assert "sequence_id" in msg


def test_negative_data_id_column_on_sequence_id(make_csv):
    msg = _reject(
        make_csv,
        _toy_frame(),
        match="collapse",
        unique_id_column="sequence_id",
    )
    assert "data_id" in msg


# ---------------------------------------------------------------------------
# Post-insert group-integrity pass (T5)
# ---------------------------------------------------------------------------


def test_partial_sequence_is_deleted_after_row_drop(make_csv):
    csv_path = make_csv(_toy_frame(), name="toy.csv")
    ing = _make_ingestor(csv_path)

    # One p3 row fails DB insertion mid-run; the other 14 insert fine.
    def _insert(table, batch):
        ok, failed = [], []
        for i, record in enumerate(batch):
            if record.get("sequence_id") == "p3" and record.get("heart_rate") == "72.0":
                failed.append({"record": record, "error": "dup key"})
            else:
                ok.append(i)
        return ok, failed

    ing.database.insert_batch.side_effect = _insert
    ing.database.delete_sequences.return_value = 6  # p3's surviving rows
    ing.database.get_label_sequence_counts.return_value = {"0": 1, "1": 1}

    failed = _run_full_ingest(ing, csv_path)

    # The dropped row is still reported (run exits non-zero upstream) …
    assert [f["error"] for f in failed] == ["dup key"]
    # … and the integrity pass removed the WHOLE partial sequence, scoped to
    # this run, so no truncated series can ever be trained on.
    ing.database.delete_sequences.assert_called_once_with(
        "tsc_toy", ing.ingestor_id, ["p3"], group_column="sequence_id"
    )
    # Counts were taken AFTER the delete: p3's label ("0") lost a sequence.
    meta = ing.api_client.send_ingest_summary.call_args.kwargs["meta_data"]
    assert meta["number_of_sequences"] == 2
    labels = ing.api_client.send_ingest_summary.call_args.kwargs["labels"]
    assert labels == {"0": 1, "1": 1}


def test_clean_run_skips_integrity_delete(make_csv):
    csv_path = make_csv(_toy_frame(), name="toy.csv")
    ing = _make_ingestor(csv_path)
    _run_full_ingest(ing, csv_path)
    ing.database.delete_sequences.assert_not_called()


def test_non_grouped_category_never_runs_integrity_delete(make_csv):
    df = _toy_frame().drop(columns=["sequence_id", "timestamp"])
    csv_path = make_csv(df, name="tab.csv")
    ing = _make_ingestor(
        csv_path,
        category=TaskCategory.TABULAR_CLASSIFICATION,
        schema={"heart_rate": "FLOAT", "label": "INT"},
    )
    ing.database.get_label_counts.return_value = {"0": 10, "1": 5}
    ing.database.insert_batch.side_effect = lambda t, batch: (
        list(range(len(batch) - 1)),
        [{"record": batch[0], "error": "dup"}],
    )
    _run_full_ingest(ing, csv_path)
    ing.database.delete_sequences.assert_not_called()


# ---------------------------------------------------------------------------
# Database helpers (SQL shape; engine mocked like tests/test_database.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    with patch("tracebloc_ingestor.database.create_engine") as ce:
        engine = MagicMock(name="engine")
        conn = MagicMock(name="conn")
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        ce.return_value = engine
        db = Database(Config(DB_USER="u", DB_PASSWORD="p", DB_NAME="d"))
        conn.reset_mock()  # drop __init__'s CREATE DATABASE call
        yield db, conn


def test_get_label_sequence_counts_counts_distinct(mock_db):
    db, conn = mock_db
    conn.execute.return_value.fetchall.return_value = [("0", 2), ("1", 1)]
    counts = db.get_label_sequence_counts("t", "ing-1")
    assert counts == {"0": 2, "1": 1}
    sql = str(conn.execute.call_args.args[0])
    assert "COUNT(DISTINCT `sequence_id`)" in sql
    assert "GROUP BY label" in sql
    assert conn.execute.call_args.args[1] == {"ingestor_id": "ing-1"}


def test_get_label_sequence_counts_null_label_folds_to_empty(mock_db):
    db, conn = mock_db
    conn.execute.return_value.fetchall.return_value = [(None, 3)]
    assert db.get_label_sequence_counts("t", "ing-1") == {"": 3}


def test_get_label_sequence_counts_custom_group_column(mock_db):
    db, conn = mock_db
    conn.execute.return_value.fetchall.return_value = []
    db.get_label_sequence_counts("t", "ing-1", group_column="entity`id")
    sql = str(conn.execute.call_args.args[0])
    # Embedded backticks are doubled (identifier escape).
    assert "COUNT(DISTINCT `entity``id`)" in sql


def test_delete_sequences_scoped_to_run_and_ids(mock_db):
    db, conn = mock_db
    result = MagicMock(rowcount=6)
    with patch(
        "tracebloc_ingestor.database._execute_with_retry", return_value=result
    ) as exec_mock:
        deleted = db.delete_sequences("t", "ing-1", ["p3"])
    assert deleted == 6
    stmt = exec_mock.call_args.args[1]
    sql = str(stmt)
    assert "DELETE FROM `t`" in sql
    assert "ingestor_id = :ingestor_id" in sql
    assert "`sequence_id` IN" in sql


def test_delete_sequences_empty_list_is_noop(mock_db):
    db, conn = mock_db
    assert db.delete_sequences("t", "ing-1", []) == 0
    conn.execute.assert_not_called()


# ---------------------------------------------------------------------------
# create_table composite index (real Table build, engine mocked)
# ---------------------------------------------------------------------------


def _bare_db():
    from sqlalchemy import MetaData

    db = Database.__new__(Database)
    db.metadata = MetaData()
    db.tables = {}
    db.engine = MagicMock(name="engine")
    db.metadata.create_all = MagicMock()
    return db


def _create(db, name, schema, **kwargs):
    insp = MagicMock()
    insp.get_table_names.return_value = []
    with patch("tracebloc_ingestor.database.inspect", return_value=insp), patch.object(
        db.metadata, "create_all"
    ):
        return db.create_table(name, schema, **kwargs)


def test_create_table_composite_index_built():
    db = _bare_db()
    table = _create(
        db,
        "tsc",
        {"sequence_id": "VARCHAR(64)", "timestamp": "TIMESTAMP", "hr": "FLOAT"},
        index_columns=["sequence_id", "timestamp"],
    )
    assert len(table.indexes) == 1
    index = next(iter(table.indexes))
    assert [c.name for c in index.columns] == ["sequence_id", "timestamp"]
    assert index.name.startswith("ix_tsc")
    assert len(index.name) <= 64  # MySQL identifier cap


def test_create_table_index_name_truncated_to_64():
    db = _bare_db()
    long_name = "t" * 60
    table = _create(
        db,
        long_name,
        {"sequence_id": "VARCHAR(64)", "timestamp": "TIMESTAMP"},
        index_columns=["sequence_id", "timestamp"],
    )
    index = next(iter(table.indexes))
    assert len(index.name) == 64


def test_create_table_index_column_not_in_schema_fails_fast():
    db = _bare_db()
    with pytest.raises(ValueError, match="not in the table schema"):
        _create(db, "tsc", {"hr": "FLOAT"}, index_columns=["sequence_id"])


def test_create_table_without_index_columns_has_no_index():
    db = _bare_db()
    table = _create(db, "plain", {"hr": "FLOAT"})
    assert table.indexes == set()


# ---------------------------------------------------------------------------
# Conventions / example YAML
# ---------------------------------------------------------------------------


def test_example_yaml_resolves():
    import yaml

    from tracebloc_ingestor.cli.conventions import resolve

    example = (
        Path(__file__).resolve().parent.parent
        / "examples"
        / "yaml"
        / "time_series_classification.yaml"
    )
    cfg = yaml.safe_load(example.read_text(encoding="utf-8"))
    resolved = resolve(cfg)
    assert resolved.category == TSC
    assert resolved.data_format == DataFormat.TABULAR
    assert resolved.label_column == "sepsis"
    assert resolved.label_policy == "passthrough"
    assert resolved.file_options == {}
    assert "sequence_id" in resolved.schema and "timestamp" in resolved.schema


def test_regression_class_set_excludes_tsc():
    # TSC is classification-class: string label shorthand, passthrough
    # policy — it must never join the bucket-required set.
    from tracebloc_ingestor.cli.conventions import REGRESSION_CLASS_CATEGORIES

    assert TSC not in REGRESSION_CLASS_CATEGORIES


# ---------------------------------------------------------------------------
# ingest.v1.json — the TSC rules (positive cases live in
# tests/test_schema_validation.py's example sweep)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def schema_validator():
    import json

    from jsonschema import Draft7Validator

    schema_path = (
        Path(__file__).resolve().parent.parent
        / "tracebloc_ingestor"
        / "schema"
        / "ingest.v1.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema)


def _tsc_config(**overrides):
    cfg = {
        "apiVersion": "tracebloc.io/v1",
        "kind": "IngestConfig",
        "category": "time_series_classification",
        "table": "tsc_train",
        "intent": "train",
        "csv": "/data/x.csv",
        "schema": {
            "sequence_id": "VARCHAR(64)",
            "timestamp": "TIMESTAMP",
            "hr": "FLOAT",
            "label": "INT",
        },
        "label": "label",
    }
    cfg.update(overrides)
    return {k: v for k, v in cfg.items() if v is not None}


def test_schema_accepts_valid_tsc_config(schema_validator):
    assert list(schema_validator.iter_errors(_tsc_config())) == []


def test_schema_rejects_tsc_without_schema(schema_validator):
    errors = list(schema_validator.iter_errors(_tsc_config(schema=None)))
    assert errors


def test_schema_rejects_tsc_schema_missing_sequence_id(schema_validator):
    cfg = _tsc_config(schema={"timestamp": "TIMESTAMP", "hr": "FLOAT", "label": "INT"})
    errors = list(schema_validator.iter_errors(cfg))
    assert errors
    assert any("sequence_id" in e.message for e in errors)


def test_schema_rejects_tsc_schema_missing_timestamp(schema_validator):
    cfg = _tsc_config(
        schema={"sequence_id": "VARCHAR(64)", "hr": "FLOAT", "label": "INT"}
    )
    errors = list(schema_validator.iter_errors(cfg))
    assert errors
    assert any("timestamp" in e.message for e in errors)


def test_schema_rejects_tsc_without_label(schema_validator):
    errors = list(schema_validator.iter_errors(_tsc_config(label=None)))
    assert errors
    assert any("label" in e.message for e in errors)


def test_schema_accepts_string_label_shorthand(schema_validator):
    # Classification-class: NOT in the regression label.policy rule.
    assert list(schema_validator.iter_errors(_tsc_config(label="label"))) == []


def test_sequence_columns_rule_has_customer_readable_description(
    schema_validator,
):
    # cli/run.py surfaces the failing rule's `description` to the customer —
    # the new allOf rule must carry one that names both fixed columns and
    # the fix (rename before ingest).
    import json

    schema_path = (
        Path(__file__).resolve().parent.parent
        / "tracebloc_ingestor"
        / "schema"
        / "ingest.v1.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    rules = [
        rule
        for rule in schema["allOf"]
        if rule.get("if", {}).get("properties", {}).get("category", {}).get("const")
        == "time_series_classification"
    ]
    assert len(rules) == 1
    description = rules[0]["description"]
    assert "sequence_id" in description
    assert "timestamp" in description
    assert "rename" in description.lower()
