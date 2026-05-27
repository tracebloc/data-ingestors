"""Tests for JSONIngestor: read_data (object/array), record validation, counting."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from tracebloc_ingestor.ingestors.json_ingestor import JSONIngestor


def make_json_ingestor(schema=None, **overrides):
    db = MagicMock()
    db.create_table.return_value = MagicMock()
    api = MagicMock()
    kwargs = dict(
        database=db,
        api_client=api,
        table_name="tbl",
        schema=schema if schema is not None else {"a": "INT"},
        intent="train",
        category=None,
    )
    kwargs.update(overrides)
    return JSONIngestor(**kwargs)


def _write_json(tmp_path, obj, name="d.json"):
    p = tmp_path / name
    p.write_text(json.dumps(obj))
    return p


def test_read_data_array(tmp_path):
    p = _write_json(tmp_path, [{"a": 1}, {"a": 2}])
    ing = make_json_ingestor(schema={"a": "INT"})
    records = list(ing.read_data(str(p)))
    assert len(records) == 2


def test_read_data_single_object(tmp_path):
    p = _write_json(tmp_path, {"a": 1})
    ing = make_json_ingestor(schema={"a": "INT"})
    records = list(ing.read_data(str(p)))
    assert records == [{"a": 1}]


def test_read_data_missing_file_raises():
    ing = make_json_ingestor()
    with pytest.raises(FileNotFoundError):
        list(ing.read_data("/no/such.json"))


def test_read_data_invalid_top_level_type_raises(tmp_path):
    p = _write_json(tmp_path, 42)  # neither dict nor list
    ing = make_json_ingestor(schema={"a": "INT"})
    with pytest.raises(ValueError):
        list(ing.read_data(str(p)))


def test_read_data_skips_non_dict_items(tmp_path):
    p = _write_json(tmp_path, [{"a": 1}, "not-a-dict", {"a": 2}])
    ing = make_json_ingestor(schema={"a": "INT"})
    records = list(ing.read_data(str(p)))
    assert len(records) == 2


def test_read_data_skips_records_failing_validation(tmp_path):
    # record missing unique_id_column -> _validate_record raises -> skipped
    p = _write_json(tmp_path, [{"a": 1}, {"a": 2, "uid": "x"}])
    ing = make_json_ingestor(schema={"a": "INT"}, unique_id_column="uid")
    records = list(ing.read_data(str(p)))
    assert records == [{"a": 2, "uid": "x"}]


def test_read_data_malformed_json_raises(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{not json")
    ing = make_json_ingestor()
    with pytest.raises(json.JSONDecodeError):
        list(ing.read_data(str(p)))


def test_validate_record_type_error_raises():
    ing = make_json_ingestor(schema={"n": "INT"})
    with pytest.raises(ValueError, match="Data type validation failed"):
        ing._validate_record({"n": "not-an-int"})


def test_validate_record_allows_empty_string():
    ing = make_json_ingestor(schema={"n": "INT"})
    # empty string is allowed (treated as None) -> no raise
    ing._validate_record({"n": ""})


def test_count_records_array(tmp_path):
    p = _write_json(tmp_path, [{"a": 1}, {"a": 2}, {"a": 3}])
    assert make_json_ingestor()._count_records(str(p)) == 3


def test_count_records_object(tmp_path):
    p = _write_json(tmp_path, {"a": 1})
    assert make_json_ingestor()._count_records(str(p)) == 1


def test_count_records_bad_path_returns_none():
    assert make_json_ingestor()._count_records("/no/such.json") is None
