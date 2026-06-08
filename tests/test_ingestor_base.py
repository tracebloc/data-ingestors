"""Tests for BaseIngestor: record processing, batch handling, validation, ingest flow.

We use a tiny concrete subclass and MagicMock Database/APIClient. The
SQLAlchemy Session is patched out so no real engine is touched.
"""

from __future__ import annotations

from typing import Any, Dict, Generator, List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tracebloc_ingestor.ingestors import base as base_mod
from tracebloc_ingestor.ingestors.base import BaseIngestor, IngestionSummary
from tracebloc_ingestor.validators.base import ValidationResult


class FakeIngestor(BaseIngestor):
    """Concrete BaseIngestor whose read_data yields preset records."""

    def __init__(self, records, **kwargs):
        self._records = records
        super().__init__(**kwargs)

    def read_data(self, source: Any) -> Generator[Dict[str, Any], None, None]:
        yield from self._records


def make_ingestor(records=None, **overrides):
    db = MagicMock(name="Database")
    db.create_table.return_value = MagicMock(name="table")
    db.insert_batch.return_value = ([1, 2], [])  # ids, db_failures
    db.get_table_schema.return_value = {"a": "INT"}
    api = MagicMock(name="APIClient")
    api.send_batch.return_value = True
    api.send_generate_edge_label_meta.return_value = True
    api.send_global_meta_meta.return_value = True
    api.prepare_dataset.return_value = True
    api.create_dataset.return_value = {"id": 1}

    kwargs = dict(
        database=db,
        api_client=api,
        table_name="tbl",
        schema={"a": "INT"},
        intent="train",
        category=None,
    )
    kwargs.update(overrides)
    return FakeIngestor(records or [], **kwargs)


# ---------------------------------------------------------------------------
# IngestionSummary.has_failures
# ---------------------------------------------------------------------------

def test_summary_clean_run_no_failures():
    s = IngestionSummary("id", 10, 10, 10, 10, 0, 0, 0)
    assert s.has_failures is False


@pytest.mark.parametrize("kwargs", [
    dict(failed_records=1),
    dict(file_transfer_failures=1),
    dict(inserted_records=9),       # < total
    dict(api_sent_records=9),       # < inserted
])
def test_summary_has_failures(kwargs):
    base = dict(ingestor_id="id", total_records=10, processed_records=10,
                inserted_records=10, api_sent_records=10, failed_records=0,
                skipped_records=0, file_transfer_failures=0)
    base.update(kwargs)
    assert IngestionSummary(**base).has_failures is True


# ---------------------------------------------------------------------------
# __init__ schema cleaning
# ---------------------------------------------------------------------------

def test_init_strips_label_annotation_unique_from_schema():
    ing = make_ingestor(
        schema={"a": "INT", "lbl": "VARCHAR", "ann": "TEXT", "uid": "VARCHAR"},
        label_column="lbl", annotation_column="ann", unique_id_column="uid",
        category=None,
    )
    # The cleaned table schema passed to create_table excludes the special cols.
    table_schema = ing.database.create_table.call_args[0][1]
    assert "lbl" not in table_schema and "ann" not in table_schema and "uid" not in table_schema
    assert "a" in table_schema


def test_init_injects_number_of_columns_for_tabular():
    from tracebloc_ingestor.utils.constants import TaskCategory
    ing = make_ingestor(
        schema={"a": "INT", "b": "FLOAT"},
        category=TaskCategory.TABULAR_CLASSIFICATION,
    )
    assert ing.file_options["number_of_columns"] == 2


# ---------------------------------------------------------------------------
# process_record / _map_unique_id
# ---------------------------------------------------------------------------

def test_process_record_generates_uuid_data_id():
    ing = make_ingestor(category=None, label_column="a")
    rec = ing.process_record({"a": "cat", "filename": "x", "extension": ".jpg"})
    assert rec["label"] == "cat"
    assert rec["data_intent"] == "train"
    assert rec["data_id"]  # uuid string
    assert rec["ingestor_id"] == ing.ingestor_id


def test_process_record_uses_unique_id_column():
    ing = make_ingestor(schema={"a": "INT"}, unique_id_column="uid", category=None)
    rec = ing.process_record({"a": "1", "uid": "  abc  ", "filename": "f"})
    assert rec["data_id"] == "abc"


def test_process_record_invalid_intent_returns_none():
    ing = make_ingestor(intent="bogus", category=None)
    assert ing.process_record({"a": "1"}) is None


def test_process_record_missing_unique_id_returns_none():
    ing = make_ingestor(unique_id_column="uid", category=None)
    assert ing.process_record({"a": "1", "uid": "   "}) is None


def test_process_record_applies_bucket_label_policy():
    from tracebloc_ingestor.utils.label_policy import BUCKET
    ing = make_ingestor(label_column="a", label_policy=BUCKET, category=None)
    rec = ing.process_record({"a": "12345", "filename": "f"})
    # bucket policy hashes the raw value -> not equal to the raw value
    assert rec["label"] != "12345"


def test_process_record_preserves_none_for_sql_null():
    """Null-like values (Python None, NaN, pd.NA, NaT) must round-trip as
    Python None so the DB binder writes SQL NULL — not as the literal
    string "nan"/"NaT"/"<NA>", and not as "".

    Regression: the cleaning dict mapped `None -> ""` and applied
    `str(v).strip()` to everything else, so pandas' NaN/NaT/pd.NA were
    silently stringified ("nan", "NaT", "<NA>") and explicit None inputs
    landed as empty-string. Both broke missing-data semantics in MySQL
    — a nullable VARCHAR ended up either with the 3-char string "nan"
    (before the upstream CSV-side fix in #172) or with "" (after #172,
    because this dict still mapped None -> ""). Surfaced by an
    end-to-end cluster ingestion of a 60-row CSV with an all-empty
    VARCHAR(50) column.
    """
    import numpy as np

    ing = make_ingestor(schema={"a": "VARCHAR(10)", "b": "INT", "c": "VARCHAR(50)"}, category=None)
    rec = ing.process_record({
        "a": None,            # explicit Python None
        "b": np.nan,           # float NaN (e.g. from pd.read_csv)
        "c": pd.NA,           # pd.NA (e.g. from pandas StringDtype)
        "filename": "f",
    })
    assert rec is not None
    assert rec["a"] is None, f"expected None, got {rec['a']!r}"
    assert rec["b"] is None, f"expected None, got {rec['b']!r}"
    assert rec["c"] is None, f"expected None, got {rec['c']!r}"


def test_process_record_preserves_real_values():
    """Non-null values continue to be stringified + stripped as before —
    this fix must not weaken the existing contract for present values.
    """
    ing = make_ingestor(schema={"a": "VARCHAR(10)", "b": "INT"}, category=None)
    rec = ing.process_record({"a": "  hello  ", "b": 42, "filename": "f"})
    assert rec["a"] == "hello"
    assert rec["b"] == "42"


def test_process_record_treats_empty_string_as_null():
    """Literal "" must become Python None (SQL NULL), matching the
    `value is None or value == ""` convention JSONIngestor._validate_record
    uses (#170).

    Regression context: JSONIngestor.read_data reads via `json.load`, not
    `pd.read_json`, so an empty-string JSON value (`"score": ""`) reaches
    here as the literal `""` — pd.isna("") is False, so without the `or
    v == ""` guard the empty string would be written verbatim to MySQL.
    The CSV path is unaffected because pandas' keep_default_na=True turns
    "" into NaN at read time (caught by the pd.isna branch).
    """
    ing = make_ingestor(schema={"a": "VARCHAR(10)", "b": "INT"}, category=None)
    rec = ing.process_record({"a": "", "b": "", "filename": "f"})
    assert rec["a"] is None
    assert rec["b"] is None


# ---------------------------------------------------------------------------
# _process_batch
# ---------------------------------------------------------------------------

def test_process_batch_success():
    ing = make_ingestor()
    session = MagicMock()
    ids, api_success, db_failures = ing._process_batch([{"data_id": "a"}], session)
    assert ids == [1, 2]
    assert api_success is True
    assert db_failures == []


def test_process_batch_no_ids_skips_api():
    ing = make_ingestor()
    ing.database.insert_batch.return_value = ([], [{"err": "x"}])
    session = MagicMock()
    ids, api_success, db_failures = ing._process_batch([{"data_id": "a"}], session)
    assert ids == []
    assert api_success is False
    ing.api_client.send_batch.assert_not_called()


def test_process_batch_reraises_on_insert_error():
    ing = make_ingestor()
    err = RuntimeError("db down")
    err.response = MagicMock(text="detail")
    ing.database.insert_batch.side_effect = err
    with pytest.raises(RuntimeError):
        ing._process_batch([{"data_id": "a"}], MagicMock())


# ---------------------------------------------------------------------------
# validate_data
# ---------------------------------------------------------------------------

def test_validate_data_no_validators_passes():
    ing = make_ingestor(category=None)
    assert ing.validate_data("src") is True


def test_validate_data_raises_when_validator_fails():
    ing = make_ingestor(category=None)
    bad = MagicMock()
    bad.name = "Bad"
    bad.validate.return_value = ValidationResult(False, ["nope"], [], {})
    with patch.object(base_mod, "map_validators", return_value=[bad]):
        with pytest.raises(ValueError):
            ing.validate_data("src")


def test_validate_data_validator_exception_raises():
    ing = make_ingestor(category=None)
    bad = MagicMock()
    bad.name = "Boom"
    bad.validate.side_effect = RuntimeError("kaboom")
    with patch.object(base_mod, "map_validators", return_value=[bad]):
        with pytest.raises(ValueError):
            ing.validate_data("src")


# ---------------------------------------------------------------------------
# ingest (full flow, Session patched)
# ---------------------------------------------------------------------------

def test_ingest_happy_path():
    records = [{"a": "1", "filename": "f1"}, {"a": "2", "filename": "f2"}]
    ing = make_ingestor(records=records, category=None)
    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest("src", batch_size=10)
    assert failed == []
    ing.database.insert_batch.assert_called()
    ing.api_client.create_dataset.assert_called_once()


def test_ingest_skips_records_that_fail_processing():
    # invalid intent -> process_record returns None -> counted as skipped
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(records=records, category=None, intent="bogus")
    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest("src", batch_size=10)
    assert failed == []
    ing.database.insert_batch.assert_not_called()


def test_ingest_reraises_on_session_error():
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(records=records, category=None)
    ing.database.insert_batch.side_effect = RuntimeError("boom")
    with patch.object(base_mod, "Session") as Sess:
        session = MagicMock()
        Sess.return_value.__enter__.return_value = session
        # final batch processing failure is caught; but make commit raise to hit rollback
        session.commit.side_effect = RuntimeError("commit fail")
        with pytest.raises(RuntimeError):
            ing.ingest("src", batch_size=10)
        session.rollback.assert_called()


def test_context_manager_protocol():
    ing = make_ingestor()
    with ing as x:
        assert x is ing


# ---------------------------------------------------------------------------
# CSV encoding pre-flight (validate_data)
# ---------------------------------------------------------------------------

def test_check_csv_encoding_rejects_non_utf8(tmp_path):
    # A Latin-1 export (German umlauts) used to surface as a misleading
    # "No data found"; now it fails fast with a clear UTF-8 message.
    bad = tmp_path / "umlaut.csv"
    bad.write_bytes("Größe,label\n1,a\n".encode("latin-1"))
    with pytest.raises(ValueError, match="UTF-8"):
        BaseIngestor._check_csv_encoding(str(bad))


def test_check_csv_encoding_accepts_utf8(tmp_path):
    good = tmp_path / "ok.csv"
    good.write_text("Größe,label\n1,a\n", encoding="utf-8")
    BaseIngestor._check_csv_encoding(str(good))  # must not raise


def test_check_csv_encoding_skips_non_csv_sources(tmp_path):
    # Non-CSV / non-path / missing sources are left to the validators.
    BaseIngestor._check_csv_encoding(str(tmp_path))                   # a directory
    BaseIngestor._check_csv_encoding(None)                            # not a path
    BaseIngestor._check_csv_encoding(str(tmp_path / "missing.csv"))   # nonexistent
