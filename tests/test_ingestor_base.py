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


def test_process_record_bool_not_stringified():
    """Python True / False must NOT be stringified — they must reach the DB
    binder as native bool so mysql-connector writes TINYINT 1/0.

    Regression: the cleaning dict applied `str(v).strip()` to every
    non-null value, turning True / False into the four-character strings
    "True" / "False". MySQL rejects those against a BOOL column with
    `Incorrect integer value: 'True' for column 'active' at row 1` —
    16/20 rows of an end-to-end JSON ingest against v0.3.5-rc3 failed for
    exactly this reason (the 4 rows with explicit `null` succeeded
    because they round-tripped to SQL NULL per #176; the rest had
    true/false). The bug was hidden until rc3 because earlier rc's
    rejected JSON before any record reached the INSERT (#173 read path,
    #176 validator widening).

    Pass bools through unchanged; non-bool, non-null values still get
    str()-and-strip semantics so existing INT/FLOAT/VARCHAR contracts
    are unchanged.
    """
    ing = make_ingestor(schema={"a": "BOOL", "b": "BOOL", "n": "INT"}, category=None)
    rec = ing.process_record({"a": True, "b": False, "n": 42, "filename": "f"})
    assert rec["a"] is True, f"expected True, got {rec['a']!r}"
    assert rec["b"] is False, f"expected False, got {rec['b']!r}"
    # Non-bool unchanged from the prior contract.
    assert rec["n"] == "42"


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


def test_process_record_preserves_mask_id_for_semantic_segmentation():
    """Regression: semantic_segmentation onboarding was broken end-to-end.

    With the documented 8-line schema-less example yaml + the shipped CSV
    (`filename, mask_id, image_label`), the cleaned_record comprehension's
    `k in self.schema` filter drops every CSV column. The next stage
    (file_transfer.py:401) does `record.get("mask_id")` and aborts with
    "No mask_id found in record" — every record skipped, 0 rows ingested
    even though #207's FilePairingValidator passes.

    mask_id must round-trip from the raw record onto the cleaned dict
    for SEMANTIC_SEGMENTATION (scoped narrowly because there's no mask_id
    DB column — #212 bugbot — and _process_batch strips it before insert).
    """
    from tracebloc_ingestor.utils.constants import TaskCategory
    ing = make_ingestor(
        schema={}, category=TaskCategory.SEMANTIC_SEGMENTATION, label_column=None
    )
    rec = ing.process_record(
        {"filename": "image_001", "mask_id": "image_001_mask", "image_label": "road"}
    )
    assert rec is not None
    assert rec["mask_id"] == "image_001_mask"
    assert rec["filename"] == "image_001"


def test_process_record_omits_mask_id_for_non_semseg_categories():
    """mask_id is a SEMANTIC_SEGMENTATION-only runtime indirection. Other
    categories must NOT carry it on the cleaned record — there's no
    mask_id column on the standard tracebloc table, so passing it
    through would make SQLAlchemy treat it as an unconsumed column at
    insert time (#212 bugbot)."""
    from tracebloc_ingestor.utils.constants import TaskCategory
    ing = make_ingestor(
        schema={}, category=TaskCategory.IMAGE_CLASSIFICATION, label_column=None
    )
    rec = ing.process_record({"filename": "image_001", "mask_id": "stray"})
    assert rec is not None
    assert "mask_id" not in rec, (
        f"non-semseg category should NOT carry mask_id; got {rec}"
    )


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


def test_process_batch_strips_mask_id_before_insert():
    # #212 bugbot: mask_id is a SEMANTIC_SEGMENTATION-only runtime
    # indirection consumed by file_transfer.map_file_transfer; it has no
    # corresponding DB column, so leaving it on the dict at insert time
    # makes SQLAlchemy reject the row as an unconsumed column. By the time
    # we reach insert, file_transfer has already used the value — pop it.
    ing = make_ingestor()
    session = MagicMock()
    batch = [
        {"data_id": "a", "mask_id": "image_001_mask"},
        {"data_id": "b", "mask_id": "image_002_mask"},
    ]
    ing._process_batch(batch, session)
    # The dicts that reached insert_batch must not carry mask_id.
    passed_batch = ing.database.insert_batch.call_args[0][1]
    assert all("mask_id" not in r for r in passed_batch), (
        f"mask_id leaked to insert: {passed_batch}"
    )


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


@pytest.mark.parametrize("failing_step", [
    "send_generate_edge_label_meta",
    "send_global_meta_meta",
    "prepare_dataset",
])
def test_ingest_fails_loud_when_backend_registration_step_fails(failing_step):
    # REGRESSION GUARD: a False return from ANY backend registration step leaves
    # the dataset half-created — rows are committed to MySQL but the dataset is
    # not registered. The old nested-if cascade silently skipped the remaining
    # steps (including create_dataset) and the run STILL returned cleanly (exit
    # 0), so the user saw a "success" over an unregistered dataset. Each step
    # must now RAISE so the run exits non-zero and the failure is visible.
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(records=records, category=None)
    getattr(ing.api_client, failing_step).return_value = False
    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(RuntimeError, match="NOT registered"):
            ing.ingest("src", batch_size=10)
    # The chain must stop — create_dataset is never reached on a failed step.
    ing.api_client.create_dataset.assert_not_called()


def test_ingest_skips_edge_label_call_for_self_supervised_categories():
    """Issue #213: self-supervised categories (MLM, …) have no `label` column,
    so the backend's edge-label endpoint returns a misleading HTTP 400
    ('No data found') even though the table has rows. Gate the call so it
    only runs for label-carrying categories. The remaining registration
    steps (send_global_meta_meta, prepare_dataset, create_dataset) still run."""
    from tracebloc_ingestor.utils.constants import TaskCategory
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(
        records=records,
        category=TaskCategory.MASKED_LANGUAGE_MODELING,
        label_column=None,
    )
    # Patch validate_data + map_file_transfer to skip real-filesystem checks;
    # the gate we're testing lives at the registration block AFTER ingest.
    with patch.object(base_mod, "Session") as Sess, \
         patch.object(ing, "validate_data", return_value=True), \
         patch.object(base_mod, "map_file_transfer", side_effect=lambda c, r, o: r):
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    ing.api_client.send_generate_edge_label_meta.assert_not_called()
    ing.api_client.send_global_meta_meta.assert_called_once()
    ing.api_client.prepare_dataset.assert_called_once()
    ing.api_client.create_dataset.assert_called_once()


def test_ingest_still_calls_edge_label_for_label_carrying_categories():
    """Regression guard for the gate above: a non-self-supervised category
    still calls the edge-label endpoint."""
    from tracebloc_ingestor.utils.constants import TaskCategory
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(
        records=records,
        category=TaskCategory.IMAGE_CLASSIFICATION,
        label_column="a",
    )
    with patch.object(base_mod, "Session") as Sess, \
         patch.object(ing, "validate_data", return_value=True), \
         patch.object(base_mod, "map_file_transfer", side_effect=lambda c, r, o: r):
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    ing.api_client.send_generate_edge_label_meta.assert_called_once()


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


def test_check_csv_encoding_rejects_nul_byte(tmp_path):
    # A NUL byte (0x00) is valid UTF-8 so it slips past the decode check, but
    # pandas' C parser silently TRUNCATES the field at it ("a\x00b" -> "a").
    # Reject it up front with a clear message (#238).
    bad = tmp_path / "nul.csv"
    bad.write_bytes(b"id,name\n1,a\x00b\n2,ok\n")
    with pytest.raises(ValueError, match="NUL byte"):
        BaseIngestor._check_csv_encoding(str(bad))


# ---------------------------------------------------------------------------
# Concurrent-ingest table lock — backend/#772 P2
# ---------------------------------------------------------------------------

def test_acquire_table_lock_creates_lock_file(tmp_path):
    """Lock file is created at STORAGE_PATH/.tracebloc-ingest-<table>.lock
    with metadata (ingestor_id, pid, hostname, started_at) so a holder
    can be identified on conflict."""
    import json
    from tracebloc_ingestor.config import Config as CfgCls
    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing = make_ingestor(table_name="dataset_a", category=None)
        lock_path = ing._acquire_table_lock()
        assert lock_path is not None
        assert lock_path.endswith(".tracebloc-ingest-dataset_a.lock")
        meta = json.loads(open(lock_path).read())
        assert meta["table_name"] == "dataset_a"
        assert meta["ingestor_id"] == ing.ingestor_id
        ing._release_table_lock(lock_path)
        assert not __import__("os").path.exists(lock_path)


def test_acquire_table_lock_rejects_concurrent_ingest(tmp_path):
    """A second ingest targeting the same table while a lock is held
    fails fast with a message naming the holder. Without this guard,
    two ingests would race create_table / interleave upserts (#772 P2)."""
    from tracebloc_ingestor.config import Config as CfgCls
    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing_a = make_ingestor(table_name="dataset_a", category=None)
        path_a = ing_a._acquire_table_lock()
        try:
            ing_b = make_ingestor(table_name="dataset_a", category=None)
            with pytest.raises(RuntimeError, match="already running"):
                ing_b._acquire_table_lock()
        finally:
            ing_a._release_table_lock(path_a)


def test_acquire_table_lock_different_tables_dont_conflict(tmp_path):
    """The lock is keyed by table_name — two different datasets can
    ingest concurrently without blocking each other."""
    from tracebloc_ingestor.config import Config as CfgCls
    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing_a = make_ingestor(table_name="dataset_a", category=None)
        ing_b = make_ingestor(table_name="dataset_b", category=None)
        path_a = ing_a._acquire_table_lock()
        path_b = ing_b._acquire_table_lock()
        assert path_a != path_b
        ing_a._release_table_lock(path_a)
        ing_b._release_table_lock(path_b)


def test_acquire_table_lock_reclaims_stale_lock(tmp_path):
    """A crashed ingest's lock auto-expires after the stale-cutoff so a
    customer isn't blocked indefinitely. We simulate by writing a lock
    file with an old timestamp."""
    import json
    from datetime import datetime, timedelta
    from tracebloc_ingestor.config import Config as CfgCls
    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing = make_ingestor(table_name="dataset_stale", category=None)
        lock_path = ing._table_lock_path()
        old = (datetime.utcnow() - timedelta(days=2)).isoformat() + "Z"
        with open(lock_path, "w") as f:
            json.dump(
                {"ingestor_id": "crashed-ingest", "started_at": old}, f
            )
        # Stale lock detected -> removed -> reacquired with the new holder.
        path = ing._acquire_table_lock()
        assert path == lock_path
        meta = json.loads(open(lock_path).read())
        assert meta["ingestor_id"] == ing.ingestor_id
        ing._release_table_lock(lock_path)


def test_acquire_table_lock_noop_when_storage_path_missing(tmp_path):
    """No STORAGE_PATH (e.g. unit tests, local dev) -> the lock is
    skipped. Returns None, _release_table_lock(None) is a no-op."""
    from tracebloc_ingestor.config import Config as CfgCls
    missing = str(tmp_path / "never_exists")
    with patch.object(CfgCls, "STORAGE_PATH", missing):
        ing = make_ingestor(table_name="dataset_a", category=None)
        assert ing._acquire_table_lock() is None
        ing._release_table_lock(None)  # must not raise


def test_release_table_lock_idempotent(tmp_path):
    """Double-release (e.g. exception path + finally path both call it)
    must not raise."""
    from tracebloc_ingestor.config import Config as CfgCls
    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing = make_ingestor(table_name="dataset_a", category=None)
        path = ing._acquire_table_lock()
        ing._release_table_lock(path)
        ing._release_table_lock(path)  # idempotent, no raise


# ---------------------------------------------------------------------------
# #221 bugbot — lock release on every exit + mtime fallback
# ---------------------------------------------------------------------------

def test_lock_released_when_validate_data_raises(tmp_path):
    """#221 bugbot HIGH: the original code only released the lock on
    validation errors / inner Session except. An exception escaping the
    pre-Session region (e.g. an unexpected error during validate_data
    that wasn't caught by the surrounding except) used to leak the lock
    until the stale-cutoff. try/finally now releases on every exit."""
    from tracebloc_ingestor.config import Config as CfgCls
    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing = make_ingestor(records=[], category=None)
        with patch.object(ing, "validate_data", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError):
                ing.ingest("src")
        lock_path = ing._table_lock_path()
        assert lock_path is not None
        import os as _os
        assert not _os.path.exists(lock_path), (
            f"lock leaked at {lock_path} after validate_data raised"
        )


def test_lock_released_when_count_records_raises(tmp_path):
    """#221 bugbot HIGH-severity scenario: a failure in
    ``self._count_records`` (between validation and the Session block)
    used to escape without releasing the lock — neither the validation
    except nor the Session except covered it. try/finally fixes it."""
    from tracebloc_ingestor.config import Config as CfgCls
    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing = make_ingestor(records=[], category=None)
        with patch.object(ing, "validate_data", return_value=True), \
             patch.object(ing, "_count_records", side_effect=RuntimeError("ouch")):
            with pytest.raises(RuntimeError):
                ing.ingest("src")
        import os as _os
        assert not _os.path.exists(ing._table_lock_path()), "lock leaked"


def test_acquire_table_lock_recovers_from_corrupt_lock_via_mtime(tmp_path):
    """#221 bugbot MED: when the lock metadata is unparseable (empty
    file, invalid JSON, missing started_at), staleness used to skip the
    cleanup — age stayed None and the lock blocked indefinitely. The
    file's mtime now serves as a fallback age signal."""
    import os as _os
    import json
    from tracebloc_ingestor.config import Config as CfgCls
    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing = make_ingestor(table_name="dataset_corrupt", category=None)
        lock_path = ing._table_lock_path()
        with open(lock_path, "w"):
            pass  # empty file -> JSON parse fails
        old = _os.path.getmtime(lock_path) - (13 * 3600)  # 13h ago
        _os.utime(lock_path, (old, old))
        path = ing._acquire_table_lock()
        assert path == lock_path
        meta = json.loads(open(lock_path).read())
        assert meta["ingestor_id"] == ing.ingestor_id
        ing._release_table_lock(lock_path)


def test_acquire_table_lock_corrupt_but_fresh_blocks(tmp_path):
    """A corrupt lock that's RECENT (not stale by mtime) still blocks
    the second ingest — we don't auto-clear; the user has to remove it
    manually. Boundary test against the mtime fallback."""
    from tracebloc_ingestor.config import Config as CfgCls
    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing = make_ingestor(table_name="dataset_corrupt", category=None)
        lock_path = ing._table_lock_path()
        with open(lock_path, "w"):
            pass  # empty, JSON parse fails, mtime is now (fresh)
        with pytest.raises(RuntimeError, match="already running"):
            ing._acquire_table_lock()


# ---------------------------------------------------------------------------
# SRC_PATH pre-flight (validate_data) — #772 P2 / PR #218 (already on develop)
# ---------------------------------------------------------------------------

def test_check_src_path_empty_raises(clean_env):
    # SRC_PATH unset / blank -> N copies of "Source image not found" with no
    # actionable cause. Fail fast with the real reason.
    clean_env.setenv("SRC_PATH", "")
    with pytest.raises(RuntimeError, match="SRC_PATH is empty"):
        BaseIngestor._check_src_path()


def test_check_src_path_unset_raises(clean_env):
    # SRC_PATH not in env at all -> same outcome.
    clean_env.delenv("SRC_PATH", raising=False)
    with pytest.raises(RuntimeError, match="SRC_PATH is empty"):
        BaseIngestor._check_src_path()


def test_check_src_path_relative_raises(clean_env):
    # A relative SRC_PATH silently joins to a relative path at file-lookup
    # time; the validator surfaces the misconfiguration before that point.
    clean_env.setenv("SRC_PATH", "data/shared")  # not absolute
    with pytest.raises(RuntimeError, match="not an absolute path"):
        BaseIngestor._check_src_path()


def test_check_src_path_nonexistent_raises(clean_env, tmp_path):
    missing = tmp_path / "never_staged"
    clean_env.setenv("SRC_PATH", str(missing))
    with pytest.raises(RuntimeError, match="does not exist"):
        BaseIngestor._check_src_path()


def test_check_src_path_accepts_real_directory(clean_env, tmp_path):
    # A properly-staged absolute directory passes — no raise.
    clean_env.setenv("SRC_PATH", str(tmp_path))
    BaseIngestor._check_src_path()  # must not raise


def test_check_src_path_only_runs_for_file_bearing_categories():
    """The guard is gated on category — tabular / time-series have no
    sidecar dirs under SRC_PATH, so the preflight isn't applied (their
    CSV path is checked separately). This keeps tabular-only ingests
    working even when SRC_PATH isn't set."""
    from tracebloc_ingestor.utils.constants import TaskCategory
    from tracebloc_ingestor.ingestors.base import _SRC_PATH_REQUIRED_CATEGORIES
    for cat in (
        TaskCategory.TABULAR_CLASSIFICATION,
        TaskCategory.TABULAR_REGRESSION,
        TaskCategory.TIME_SERIES_FORECASTING,
        TaskCategory.TIME_TO_EVENT_PREDICTION,
    ):
        assert cat not in _SRC_PATH_REQUIRED_CATEGORIES
    # Image / text / segmentation / MLM all need a staged SRC_PATH.
    for cat in (
        TaskCategory.IMAGE_CLASSIFICATION,
        TaskCategory.OBJECT_DETECTION,
        TaskCategory.KEYPOINT_DETECTION,
        TaskCategory.SEMANTIC_SEGMENTATION,
        TaskCategory.TEXT_CLASSIFICATION,
        TaskCategory.MASKED_LANGUAGE_MODELING,
    ):
        assert cat in _SRC_PATH_REQUIRED_CATEGORIES


def test_check_src_path_required_for_token_classification():
    """token_classification reads per-row .txt sidecars from texts/ under
    SRC_PATH (same layout as text_classification), so it must get the
    early staging preflight instead of N file-transfer 'not found' errors."""
    from tracebloc_ingestor.utils.constants import TaskCategory
    from tracebloc_ingestor.ingestors.base import _SRC_PATH_REQUIRED_CATEGORIES
    assert TaskCategory.TOKEN_CLASSIFICATION in _SRC_PATH_REQUIRED_CATEGORIES
