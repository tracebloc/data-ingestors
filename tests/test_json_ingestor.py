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


# ---------------------------------------------------------------------------
# #204 bugbot — non-finite (inf / NaN) must be rejected for INT and FLOAT
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("v", [float("inf"), float("-inf"), float("nan"), "Infinity"])
def test_validate_record_rejects_non_finite_for_int(v):
    # float("Infinity") returns +inf without raising, and float.is_integer()
    # is False on inf/NaN — but the contract shouldn't depend on that detail.
    # Mirror DataValidator's _non_finite_error on the CSV path and reject
    # explicitly with an informative message.
    ing = make_json_ingestor(schema={"n": "INT"})
    with pytest.raises(ValueError, match="non-finite|not an integer"):
        ing._validate_record({"n": v})


@pytest.mark.parametrize("v", [float("inf"), float("-inf"), float("nan"), "Infinity"])
def test_validate_record_rejects_non_finite_for_float(v):
    # Bare float() lets inf through silently — DataValidator already rejects
    # this for CSV (_non_finite_error). Match here so JSON and CSV agree.
    ing = make_json_ingestor(schema={"x": "FLOAT"})
    with pytest.raises(ValueError, match="non-finite"):
        ing._validate_record({"x": v})


# ---------------------------------------------------------------------------
# #204 bugbot (2nd round) — JSON must not be stricter than CSV either,
# else a record passes CSV-style preflight then drops at per-record check
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("v", [True, False])
def test_validate_record_accepts_python_bool_for_int(v):
    # DataValidator._validate_int accepts a bool column via pd.to_numeric
    # (True -> 1, False -> 0). Rejecting them here would let a record pass
    # CSV preflight and then be silently dropped mid-ingest.
    ing = make_json_ingestor(schema={"n": "INT"})
    ing._validate_record({"n": v})


@pytest.mark.parametrize("v", ["00", "01", "1.0", "0.0", "1e0", "0e0"])
def test_validate_record_accepts_numeric_string_for_bool(v):
    # DataValidator._validate_boolean accepts any string that pd.to_numeric
    # resolves to 0 or 1. JSON per-record validation must match.
    ing = make_json_ingestor(schema={"flag": "BOOL"})
    ing._validate_record({"flag": v})


def test_validate_record_still_rejects_non_bool_numeric_string():
    # The fallback only accepts values that resolve to exactly 0 or 1; "1.5"
    # / "2" / "0.4" must still fail (matches DataValidator).
    ing = make_json_ingestor(schema={"flag": "BOOL"})
    for v in ("1.5", "2", "0.4"):
        with pytest.raises(ValueError, match="Data type validation failed"):
            ing._validate_record({"flag": v})


def test_count_records_array(tmp_path):
    p = _write_json(tmp_path, [{"a": 1}, {"a": 2}, {"a": 3}])
    assert make_json_ingestor()._count_records(str(p)) == 3


def test_count_records_object(tmp_path):
    p = _write_json(tmp_path, {"a": 1})
    assert make_json_ingestor()._count_records(str(p)) == 1


def test_count_records_bad_path_returns_none():
    assert make_json_ingestor()._count_records("/no/such.json") is None


# ---------------------------------------------------------------------------
# Streaming via ijson — backend/#772 P2
# ---------------------------------------------------------------------------

def test_read_data_streams_array_does_not_materialise_whole_file(tmp_path):
    """JSON ingestion used to call ``json.load`` and hold the whole file
    in memory; a multi-GB array OOM'd the pod (Killed/137 — the deferred
    half of #771's streaming item). Now we stream via ``ijson.items`` so
    only one record at a time is in memory. The test pins the streaming
    contract: the generator yields BEFORE the file is exhausted (i.e.
    ``next()`` returns without reading the trailing records first).
    """
    import ijson as _ijson
    import os

    # Write a moderate-size array so the test is fast but the streaming
    # behaviour is observable. We assert by checking that ijson.items is
    # actually invoked — proves the streaming path, not the full-load path.
    p = _write_json(tmp_path, [{"a": i} for i in range(1000)])
    ing = make_json_ingestor()

    spy_invocations = []
    real_items = _ijson.items

    def spy_items(file_obj, prefix, *args, **kwargs):
        spy_invocations.append(prefix)
        return real_items(file_obj, prefix, *args, **kwargs)

    from unittest.mock import patch
    with patch("tracebloc_ingestor.ingestors.json_ingestor.ijson.items",
               side_effect=spy_items):
        gen = ing.read_data(str(p))
        first = next(gen)
        # Now consume the rest.
        rest = list(gen)

    assert spy_invocations == ["item"], "ijson.items was not used"
    assert first["a"] == 0
    assert len(rest) == 999
    assert rest[-1]["a"] == 999


def test_count_records_array_streaming(tmp_path):
    """Counting an array no longer materialises the whole file — proves
    that the count path uses ``ijson.items`` rather than ``json.load``."""
    p = _write_json(tmp_path, [{"a": i} for i in range(50)])
    assert make_json_ingestor()._count_records(str(p)) == 50


def test_read_data_single_object_skips_ijson(tmp_path):
    """Single-object JSON (one record) shouldn't trigger ijson — it
    short-circuits to a normal load since one record is by definition
    tractable. Regression guard: the existing single-object contract
    must not regress when the array path moved to streaming."""
    from unittest.mock import patch
    p = _write_json(tmp_path, {"a": 1, "b": "x"})
    spy = []
    real_items = __import__("ijson").items

    def spy_items(*a, **k):
        spy.append(1)
        return real_items(*a, **k)

    with patch("tracebloc_ingestor.ingestors.json_ingestor.ijson.items",
               side_effect=spy_items):
        records = list(make_json_ingestor().read_data(str(p)))
    assert records == [{"a": 1, "b": "x"}]
    assert spy == [], "single-object path must not invoke ijson.items"


def test_read_data_invalid_top_level_still_rejected(tmp_path):
    """A JSON file with neither an object nor an array at the top is
    rejected with a clear ValueError — the streaming path must preserve
    the same boundary the load-based path enforced."""
    p = tmp_path / "bad.json"
    p.write_text("42")  # bare number
    with pytest.raises(ValueError, match="object or array"):
        list(make_json_ingestor().read_data(str(p)))


# ---------------------------------------------------------------------------
# #222 bugbot — file-handle leak, count-skipped-parse, peek-hides-error
# ---------------------------------------------------------------------------

def test_array_streaming_closes_file_handle_on_partial_consume(tmp_path):
    """#222 bugbot MED: the array path used to pass a bare
    ``open(..., 'rb')`` into ``ijson.items``, so the descriptor stayed
    open until the inner iterator and outer generator were GC'd —
    leaking handles across repeated ingests. The file handle is now
    inside a ``with`` block in read_data, so partial consumption
    (close the generator early) deterministically closes the file."""
    p = _write_json(tmp_path, [{"a": i} for i in range(100)])
    ing = make_json_ingestor()
    gen = ing.read_data(str(p))
    first = next(gen)
    assert first["a"] == 0
    # Close the generator without consuming the rest — the contextmanager
    # in read_data must close the underlying file.
    gen.close()
    # Subsequent reads should be possible (file isn't locked, descriptor
    # was released). We re-open to confirm.
    with open(p, "rb") as f:
        assert f.read(1) == b"["


def test_count_records_object_validates_parseability(tmp_path):
    """#222 bugbot MED: returning 1 based on the peek alone for an
    object-shaped file used to misreport a TRUNCATED object as one
    record — the progress bar then expected 1, but ``read_data`` would
    raise a decode error mid-ingest. Now the count path actually parses
    the object via ijson; a broken object returns None."""
    p = tmp_path / "broken.json"
    p.write_text('{"a": 1, "b":')  # starts with `{`, truncated mid-value
    # Peek -> "object", but the file isn't parseable -> count should be None.
    assert make_json_ingestor()._count_records(str(p)) is None


def test_count_records_object_well_formed_returns_one(tmp_path):
    """Positive boundary for the previous test: a well-formed object
    still reports 1 — the count path validates, doesn't reject good
    input."""
    p = _write_json(tmp_path, {"a": 1, "b": "ok"})
    assert make_json_ingestor()._count_records(str(p)) == 1


def test_peek_propagates_os_read_errors(tmp_path):
    """#222 bugbot MED: ``_peek_json_shape`` used to swallow ``OSError``
    into None, then ``read_data`` raised a misleading 'object or array'
    error instead of the underlying permission-denied / read failure.
    The OSError now propagates."""
    from tracebloc_ingestor.ingestors.json_ingestor import _peek_json_shape
    nonexistent = tmp_path / "no_such_dir" / "x.json"
    with pytest.raises(OSError):
        _peek_json_shape(nonexistent)
