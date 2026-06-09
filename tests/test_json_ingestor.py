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


def test_validate_record_allows_json_null_for_int():
    # Regression: a JSON ``null`` (Python None) was passed to int(None), which
    # raises TypeError and surfaced as "Data type validation failed for field
    # n" — an error the user can never clear since JSON null IS the
    # representation of missing.
    ing = make_json_ingestor(schema={"n": "INT"})
    ing._validate_record({"n": None})


def test_validate_record_allows_json_null_for_float():
    ing = make_json_ingestor(schema={"x": "FLOAT"})
    ing._validate_record({"x": None})


def test_validate_record_allows_json_null_for_bool():
    ing = make_json_ingestor(schema={"b": "BOOL"})
    ing._validate_record({"b": None})


def test_validate_record_still_rejects_real_type_errors():
    # NULL tolerance must NOT mask a genuine bad value.
    ing = make_json_ingestor(schema={"n": "INT"})
    with pytest.raises(ValueError, match="Data type validation failed"):
        ing._validate_record({"n": "not-an-int"})


# ---------------------------------------------------------------------------
# Issue #189: JSON validation must match CSV — bool()/int() were too lenient
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("garbage", ["maybe", "banana", 2, "yesno"])
def test_validate_record_rejects_garbage_boolean(garbage):
    # Regression: bool("maybe") is True, bool(2) is True, so the previous
    # implementation silently accepted these. Match DataValidator's fixed
    # valid-set so JSON and CSV give the same verdict.
    ing = make_json_ingestor(schema={"flag": "BOOL"})
    with pytest.raises(ValueError, match="Data type validation failed"):
        ing._validate_record({"flag": garbage})


@pytest.mark.parametrize("v", [True, False, "true", "FALSE", "yes", "n", 0, 1])
def test_validate_record_accepts_real_booleans(v):
    # The valid set: Python bools, the canonical strings DataValidator
    # accepts, and 0/1.
    ing = make_json_ingestor(schema={"flag": "BOOL"})
    ing._validate_record({"flag": v})


def test_validate_record_rejects_non_integer_float_for_int():
    # Regression: int(3.5) silently truncated to 3 with no error — INT label
    # could end up as 0/1 from a 0.4/1.6 source value, corrupting training.
    # Reject anything that isn't integer-valued.
    ing = make_json_ingestor(schema={"n": "INT"})
    with pytest.raises(ValueError, match="not an integer"):
        ing._validate_record({"n": 3.5})


def test_validate_record_accepts_integer_valued_float_for_int():
    # 3.0 is integer-valued and is fine (matches the CSV validator's
    # tolerance for whole-number floats stored as INT).
    ing = make_json_ingestor(schema={"n": "INT"})
    ing._validate_record({"n": 3.0})


def test_validate_record_rejects_overlong_varchar():
    # Regression: VARCHAR length was never enforced on JSON. The CSV path
    # rejects a too-long string; JSON must too.
    ing = make_json_ingestor(schema={"code": "VARCHAR(3)"})
    with pytest.raises(ValueError, match="exceeds the declared length 3"):
        ing._validate_record({"code": "ABCDE"})


def test_validate_record_accepts_numeric_in_varchar():
    # Issue #188 parity: VARCHAR accepts numeric scalars (zip codes,
    # numeric IDs declared VARCHAR). MySQL binds them as strings.
    ing = make_json_ingestor(schema={"code": "VARCHAR(10)"})
    ing._validate_record({"code": 12345})


def test_validate_record_rejects_unparseable_date():
    # Regression: DATE wasn't validated on JSON at all — a 'not-a-date'
    # string flowed through to the DB.
    ing = make_json_ingestor(schema={"d": "DATE"})
    with pytest.raises(ValueError, match="not a valid DATE"):
        ing._validate_record({"d": "not-a-date"})


def test_validate_record_accepts_iso_date():
    ing = make_json_ingestor(schema={"d": "DATE"})
    ing._validate_record({"d": "2024-01-15"})


def test_count_records_array(tmp_path):
    p = _write_json(tmp_path, [{"a": 1}, {"a": 2}, {"a": 3}])
    assert make_json_ingestor()._count_records(str(p)) == 3


def test_count_records_object(tmp_path):
    p = _write_json(tmp_path, {"a": 1})
    assert make_json_ingestor()._count_records(str(p)) == 1


def test_count_records_bad_path_returns_none():
    assert make_json_ingestor()._count_records("/no/such.json") is None
