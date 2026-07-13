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
from tracebloc_ingestor.ingestors import preflight
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
    db.get_label_counts.return_value = {"cat": 2}
    db.get_samples.return_value = []
    api = MagicMock(name="APIClient")
    api.send_ingest_summary.return_value = {"dataset_id": 1, "dataset_key": "key"}

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


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(failed_records=1),
        dict(file_transfer_failures=1),
        dict(skipped_records=1),  # a record dropped during processing (#234)
        dict(inserted_records=9),  # < total
        dict(api_sent_records=9),  # < inserted
    ],
)
def test_summary_has_failures(kwargs):
    base = dict(
        ingestor_id="id",
        total_records=10,
        processed_records=10,
        inserted_records=10,
        api_sent_records=10,
        failed_records=0,
        skipped_records=0,
        file_transfer_failures=0,
    )
    base.update(kwargs)
    assert IngestionSummary(**base).has_failures is True


# ---------------------------------------------------------------------------
# __init__ schema cleaning
# ---------------------------------------------------------------------------


def test_init_strips_label_annotation_unique_from_schema():
    ing = make_ingestor(
        schema={"a": "INT", "lbl": "VARCHAR", "ann": "TEXT", "uid": "VARCHAR"},
        label_column="lbl",
        annotation_column="ann",
        unique_id_column="uid",
        category=None,
    )
    # Table creation is deferred until validation passes (#260), so inspect
    # the cleaned schema stashed for later create_table() instead of asserting
    # against a call that hasn't happened yet.
    table_schema = ing._table_schema
    assert (
        "lbl" not in table_schema
        and "ann" not in table_schema
        and "uid" not in table_schema
    )
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


def test_process_record_reads_label_by_configured_key():
    """Baseline for #340: RecordProcessor reads the label by EXACT key. With
    a mismatched key it reads None — this is why the ingestor must resolve the
    label column to the real header before processing (see the
    _resolve_label_column + end-to-end tests below)."""
    ing = make_ingestor(label_column="label", schema={"a": "INT", "label": "VARCHAR(10)"}, category=None)
    # exact key -> label present
    assert ing.process_record({"a": "1", "label": "cat", "filename": "f"})["label"] == "cat"
    # mismatched-case key with the SAME configured name -> None (the bug)
    assert ing.process_record({"a": "1", "Label": "cat", "filename": "f"})["label"] is None


# ---------------------------------------------------------------------------
# _resolve_label_column (#340)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "columns,expected",
    [
        (["a", "Label"], "Label"),      # case mismatch -> real header
        (["a", " label "], " label "),  # whitespace mismatch -> raw header
        (["a", "label"], "label"),       # exact -> unchanged
        (["a", "b"], "label"),           # absent -> configured name untouched
    ],
)
def test_resolve_label_column(columns, expected):
    ing = make_ingestor(label_column="label", schema={"a": "INT", "label": "VARCHAR(10)"})
    ing._resolve_label_column(columns)
    assert ing.label_column == expected


def test_resolve_label_column_noop_when_unset():
    ing = make_ingestor(label_column=None)
    ing._resolve_label_column(["a", "b"])
    assert ing.label_column is None


def test_ingest_label_case_mismatch_survives_to_db():
    """#340 end-to-end: config ``label: label`` against a header ``Label``.
    The label column is validated case-insensitively, so the manifest passes
    preflight; the ingest loop must resolve it to the real header or every
    label reaches the DB as None (silent all-NULL labels)."""
    records = [
        {"a": "1", "Label": "cat", "filename": "f1"},
        {"a": "2", "Label": "dog", "filename": "f2"},
    ]
    ing = make_ingestor(
        records=records,
        category=None,
        label_column="label",
        schema={"a": "INT", "label": "VARCHAR(10)"},
    )
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    ing.database.insert_batch.assert_called()
    _table, batch = ing.database.insert_batch.call_args.args
    assert [r.get("label") for r in batch] == ["cat", "dog"], (
        f"labels nulled by case mismatch: {[r.get('label') for r in batch]}"
    )


def test_ingest_label_resolves_on_later_record_for_sparse_json():
    """#340 JSON edge: records may be sparse — a leading object can omit the
    label. Resolution must retry on later records, not give up after the first,
    or a mis-cased label on every subsequent row would still null. The first
    (label-less) record legitimately gets None; the second resolves 'Label'."""
    records = [
        {"a": "1", "filename": "f1"},                   # no label key at all
        {"a": "2", "Label": "dog", "filename": "f2"},   # mis-cased label
    ]
    ing = make_ingestor(
        records=records,
        category=None,
        label_column="label",
        schema={"a": "INT", "label": "VARCHAR(10)"},
    )
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    _table, batch = ing.database.insert_batch.call_args.args
    assert [r.get("label") for r in batch] == [None, "dog"], (
        f"expected [None, 'dog']; got {[r.get('label') for r in batch]}"
    )


def test_process_record_strips_whitespace_from_string_label():
    """Issue #261: a raw label value like ``"  A  "`` must be stripped
    before the label policy runs, so MySQL stores ``"A"`` and a CSV
    with ``"  A  "`` mixed with ``"A"`` doesn't land as two distinct
    classes (silent label-set corruption).

    The strip mirrors what the framework does for ``data_id`` (line
    below in process_record) and for column headers (csv_ingestor).
    """
    ing = make_ingestor(label_column="lbl", category=None)
    rec = ing.process_record({"lbl": "  A  ", "filename": "f"})
    # PASSTHROUGH policy: label lands verbatim, but stripped.
    assert rec["label"] == "A", f"expected stripped 'A', got {rec['label']!r}"


def test_process_record_label_strip_makes_whitespace_variants_equivalent():
    """End-to-end check that two records with ``"  A  "`` and ``"A"``
    produce the SAME cleaned label — the contract the corruption fix
    establishes."""
    ing = make_ingestor(label_column="lbl", category=None)
    rec1 = ing.process_record({"lbl": "  A  ", "filename": "f1"})
    rec2 = ing.process_record({"lbl": "A", "filename": "f2"})
    assert rec1["label"] == rec2["label"] == "A"


def test_process_record_label_strip_preserves_non_string_labels():
    """INT class IDs and other non-string labels (which have no
    whitespace to strip) must pass through unchanged."""
    ing = make_ingestor(label_column="lbl", category=None)
    rec = ing.process_record({"lbl": 42, "filename": "f"})
    # Numeric labels pass through the policy unchanged.
    assert rec["label"] == 42


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

    ing = make_ingestor(
        schema={"a": "VARCHAR(10)", "b": "INT", "c": "VARCHAR(50)"}, category=None
    )
    rec = ing.process_record(
        {
            "a": None,  # explicit Python None
            "b": np.nan,  # float NaN (e.g. from pd.read_csv)
            "c": pd.NA,  # pd.NA (e.g. from pandas StringDtype)
            "filename": "f",
        }
    )
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


def test_process_record_does_not_carry_mask_id_when_not_in_schema():
    """When ``mask_id`` is NOT a declared schema column it is not a DB column,
    so process_record keeps it off the cleaned record (the schema filter drops
    it; the old semseg re-add is gone). ``map_file_transfer`` then lends it from
    the RAW source record for the copy (see
    test_map_file_transfer_lends_then_strips_mask_id). The complementary case —
    ``mask_id`` DECLARED in the schema (the semseg template) → kept + stored —
    is test_process_record_stores_mask_id_when_declared_in_schema.
    """
    from tracebloc_ingestor.utils.constants import TaskCategory

    ing = make_ingestor(
        schema={}, category=TaskCategory.SEMANTIC_SEGMENTATION, label_column=None
    )
    rec = ing.process_record(
        {"filename": "image_001", "mask_id": "image_001_mask", "image_label": "road"}
    )
    assert rec is not None
    assert rec["filename"] == "image_001"
    assert (
        "mask_id" not in rec
    ), f"mask_id (not in schema) must not be a column; got {rec}"


def test_process_record_stores_mask_id_when_declared_in_schema():
    """Contract with the training client (backend#816): for semantic_segmentation
    the client SELECTs the dataset row and does ``str(row["mask_id"])`` to locate
    each mask file — it raises FileNotFoundError if mask_id is missing/NULL. So
    when the semseg TEMPLATE declares ``mask_id`` in its schema, the ingestor
    MUST keep it on the cleaned record so it lands in MySQL. (develop popped it →
    NULL → semseg training crashed; this pins the stored contract that the P5
    mask_id work restores.)"""
    from tracebloc_ingestor.utils.constants import TaskCategory

    ing = make_ingestor(
        schema={"mask_id": "VARCHAR(255)"},
        category=TaskCategory.SEMANTIC_SEGMENTATION,
        label_column=None,
    )
    rec = ing.process_record({"mask_id": "image_001_mask", "filename": "image_001"})
    assert rec is not None
    assert rec["mask_id"] == "image_001_mask", (
        "mask_id declared in schema must be kept on the cleaned record so it "
        f"reaches MySQL (the client reads it to locate masks, backend#816); got {rec}"
    )


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
    assert (
        "mask_id" not in rec
    ), f"non-semseg category should NOT carry mask_id; got {rec}"


# ---------------------------------------------------------------------------
# _process_batch
# ---------------------------------------------------------------------------


def test_process_batch_success():
    ing = make_ingestor()
    session = MagicMock()
    ids, db_failures = ing._batch_writer._process(
        [{"data_id": "a"}], session
    )
    assert ids == [1, 2]
    assert db_failures == []


def test_process_batch_no_ids_skips_api():
    ing = make_ingestor()
    ing.database.insert_batch.return_value = ([], [{"err": "x"}])
    session = MagicMock()
    ids, db_failures = ing._batch_writer._process(
        [{"data_id": "a"}], session
    )
    assert ids == []
    assert db_failures == [{"err": "x"}]


def test_process_batch_reraises_on_insert_error():
    ing = make_ingestor()
    err = RuntimeError("db down")
    err.response = MagicMock(text="detail")
    ing.database.insert_batch.side_effect = err
    with pytest.raises(RuntimeError):
        ing._batch_writer._process([{"data_id": "a"}], MagicMock())


def test_map_file_transfer_lends_then_strips_mask_id(monkeypatch):
    """P5 (replaces the old _process_batch mask_id pop): the sidecar pointer is
    now owned by map_file_transfer. It LENDS mask_id from the RAW source record
    to the transfer (so the copy can locate the mask file) and STRIPS it before
    return — so it never reaches the DB insert. BatchWriter no longer pops it
    because the cleaned record never carries it (#212)."""
    from tracebloc_ingestor import file_transfer
    from tracebloc_ingestor.modalities.spec import ModalitySpec

    seen = {}

    def spy_transfer(record, options, cfg=None):
        seen["mask_id_during_transfer"] = record.get("mask_id")
        return record  # transfer succeeds

    fake_spec = ModalitySpec(
        category="seg",
        is_file_bearing=True,
        is_tabular_family=False,
        is_self_supervised=False,
        data_format="image",
        build_validators=lambda opts: [],
        transfer=spy_transfer,
    )
    monkeypatch.setattr(
        "tracebloc_ingestor.modalities.registry.REGISTRY", {"seg": fake_spec}
    )

    cleaned = {"data_id": "a", "filename": "img1"}  # DB-bound: no mask_id
    result = file_transfer.map_file_transfer(
        "seg", cleaned, {}, source_record={"filename": "img1", "mask_id": "img1_mask"}
    )

    # The transfer SAW the lent pointer...
    assert seen["mask_id_during_transfer"] == "img1_mask"
    # ...but it's stripped before return (same dict, mutated in place), so it
    # never reaches insert_batch.
    assert "mask_id" not in result
    assert "mask_id" not in cleaned


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


def test_init_does_not_create_table_until_validation_passes():
    # REGRESSION GUARD (#260): create_table used to fire inside __init__, so a
    # validator-rejected ingest left an empty orphaned table that the next run's
    # stale-table guard tripped on, forcing the user to manually DROP. Table
    # creation must be deferred until validation has accepted the input.
    ing = make_ingestor(category=None)
    ing.database.create_table.assert_not_called()
    assert ing.table is None


def test_validation_failure_leaves_no_table_created():
    # REGRESSION GUARD (#260): when validate_data rejects the input, the
    # ingestor must not have created the destination table — so a corrected
    # re-run starts from a clean slate without manual DB intervention.
    ing = make_ingestor(category=None)
    bad = MagicMock()
    bad.name = "Bad"
    bad.validate.return_value = ValidationResult(False, ["nope"], [], {})
    with patch.object(base_mod, "map_validators", return_value=[bad]):
        with pytest.raises(ValueError):
            ing.ingest("src", batch_size=10)
    ing.database.create_table.assert_not_called()
    assert ing.table is None


def test_ingest_creates_table_after_validation_passes():
    # The table is created exactly once, after validation accepts the input.
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(records=records, category=None)
    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    # index_columns=None: only grouped categories (ModalitySpec.grouping)
    # request the composite secondary index (backend#1054 WS1).
    ing.database.create_table.assert_called_once_with(
        "tbl", {"a": "INT"}, index_columns=None
    )
    assert ing.table is not None


def test_ingest_happy_path():
    records = [{"a": "1", "filename": "f1"}, {"a": "2", "filename": "f2"}]
    ing = make_ingestor(records=records, category=None)
    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest("src", batch_size=10)
    assert failed == []
    ing.database.insert_batch.assert_called()
    ing.api_client.send_ingest_summary.assert_called_once()


def test_ingest_fails_loud_when_backend_registration_fails():
    # REGRESSION GUARD: if send_ingest_summary raises, rows are committed to
    # MySQL but the dataset is not registered. The exception must propagate so
    # the run exits non-zero and the failure is visible.
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(records=records, category=None)
    ing.api_client.send_ingest_summary.side_effect = RuntimeError("backend rejected")
    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(RuntimeError):
            ing.ingest("src", batch_size=10)


def test_ingest_calls_summary_for_self_supervised_categories():
    """Self-supervised categories (MLM, …) MUST call send_ingest_summary like
    every other category — the single-call flow handles all categories uniformly."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(
        records=records,
        category=TaskCategory.MASKED_LANGUAGE_MODELING,
        label_column=None,
    )
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    ing.api_client.send_ingest_summary.assert_called_once()


def test_ingest_fails_loud_when_summary_fails_for_self_supervised():
    """If send_ingest_summary raises for a self-supervised category, the ingest
    must fail loudly — not silently exit 0 with an unregistered dataset."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(
        records=records,
        category=TaskCategory.MASKED_LANGUAGE_MODELING,
        label_column=None,
    )
    ing.api_client.send_ingest_summary.side_effect = RuntimeError("backend rejected")
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(RuntimeError):
            ing.ingest("src", batch_size=10)


def test_ingest_calls_summary_for_label_carrying_categories():
    """Non-self-supervised categories also call send_ingest_summary."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(
        records=records,
        category=TaskCategory.IMAGE_CLASSIFICATION,
        label_column="a",
    )
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    ing.api_client.send_ingest_summary.assert_called_once()


def test_ingest_fails_fast_on_invalid_intent():
    # A bad intent is a run-wide config error, not a per-row skip: it must
    # abort loudly before any DB work (#234), not silently skip every record
    # and exit 0 with an empty dataset and a Job marked Succeeded.
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(records=records, category=None, intent="bogus")
    with pytest.raises(ValueError, match="intent"):
        ing.ingest("src", batch_size=10)
    ing.database.insert_batch.assert_not_called()


def test_ingest_counts_dropped_record_as_failure():
    # A record dropped during processing (here: a missing unique_id when
    # unique_id_column is set) must be surfaced as a failed record so the run
    # exits non-zero — not silently skipped with a clean exit 0 (#234). The
    # dropped record never reaches the DB.
    records = [{"a": "1", "filename": "f1"}]  # no 'uid' -> _map_unique_id drops it
    ing = make_ingestor(
        records=records, category=None, intent="train", unique_id_column="uid"
    )
    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest("src", batch_size=10)
    assert len(failed) == 1
    assert failed[0]["error"] == "record_dropped_in_processing"
    ing.database.insert_batch.assert_not_called()


def test_ingest_keeps_good_records_and_counts_dropped():
    # The canonical #234 scenario: a mixed run. The good record ingests; the
    # dropped one (blank unique_id) is surfaced as a failure, the summary
    # records the drop and trips has_failures — so a partial run can NOT be
    # reported as a clean success (the "0 failures / most records failed"
    # contradiction).
    records = [{"a": "1", "uid": "x1"}, {"a": "2", "uid": "  "}]  # 2nd: blank uid
    ing = make_ingestor(
        records=records, category=None, intent="train", unique_id_column="uid"
    )
    captured = {}
    real_log = BaseIngestor._log_summary

    def spy(self, summary):
        captured["summary"] = summary
        return real_log(self, summary)

    with patch.object(base_mod, "Session") as Sess, patch.object(
        BaseIngestor, "_log_summary", spy
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest("src", batch_size=10)

    summary = captured["summary"]
    assert summary.skipped_records == 1
    assert summary.has_failures is True
    assert any(f["error"] == "record_dropped_in_processing" for f in failed)
    ing.database.insert_batch.assert_called()  # the good record reached the DB


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
        preflight.check_csv_encoding(str(bad))


def test_check_csv_encoding_accepts_utf8(tmp_path):
    good = tmp_path / "ok.csv"
    good.write_text("Größe,label\n1,a\n", encoding="utf-8")
    preflight.check_csv_encoding(str(good))  # must not raise


def test_check_csv_encoding_skips_non_csv_sources(tmp_path):
    # Non-CSV / non-path / missing sources are left to the validators.
    preflight.check_csv_encoding(str(tmp_path))  # a directory
    preflight.check_csv_encoding(None)  # not a path
    preflight.check_csv_encoding(str(tmp_path / "missing.csv"))  # nonexistent


def test_check_csv_encoding_rejects_nul_byte(tmp_path):
    # A NUL byte (0x00) is valid UTF-8 so it slips past the decode check, but
    # pandas' C parser silently TRUNCATES the field at it ("a\x00b" -> "a").
    # Reject it up front with a clear message (#238).
    bad = tmp_path / "nul.csv"
    bad.write_bytes(b"id,name\n1,a\x00b\n2,ok\n")
    with pytest.raises(ValueError, match="NUL byte"):
        preflight.check_csv_encoding(str(bad))


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
        lock_path = ing._table_lock.acquire()
        assert lock_path is not None
        assert lock_path.endswith(".tracebloc-ingest-dataset_a.lock")
        meta = json.loads(open(lock_path).read())
        assert meta["table_name"] == "dataset_a"
        assert meta["ingestor_id"] == ing.ingestor_id
        ing._table_lock.release(lock_path)
        assert not __import__("os").path.exists(lock_path)


def test_acquire_table_lock_rejects_concurrent_ingest(tmp_path):
    """A second ingest targeting the same table while a lock is held
    fails fast with a message naming the holder. Without this guard,
    two ingests would race create_table / interleave upserts (#772 P2)."""
    from tracebloc_ingestor.config import Config as CfgCls

    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing_a = make_ingestor(table_name="dataset_a", category=None)
        path_a = ing_a._table_lock.acquire()
        try:
            ing_b = make_ingestor(table_name="dataset_a", category=None)
            with pytest.raises(RuntimeError, match="already running"):
                ing_b._table_lock.acquire()
        finally:
            ing_a._table_lock.release(path_a)


def test_acquire_table_lock_different_tables_dont_conflict(tmp_path):
    """The lock is keyed by table_name — two different datasets can
    ingest concurrently without blocking each other."""
    from tracebloc_ingestor.config import Config as CfgCls

    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing_a = make_ingestor(table_name="dataset_a", category=None)
        ing_b = make_ingestor(table_name="dataset_b", category=None)
        path_a = ing_a._table_lock.acquire()
        path_b = ing_b._table_lock.acquire()
        assert path_a != path_b
        ing_a._table_lock.release(path_a)
        ing_b._table_lock.release(path_b)


def test_acquire_table_lock_reclaims_stale_lock(tmp_path):
    """A crashed ingest's lock auto-expires after the stale-cutoff so a
    customer isn't blocked indefinitely. We simulate by writing a lock
    file with an old timestamp."""
    import json
    from datetime import datetime, timedelta
    from tracebloc_ingestor.config import Config as CfgCls

    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing = make_ingestor(table_name="dataset_stale", category=None)
        lock_path = ing._table_lock.path()
        old = (datetime.utcnow() - timedelta(days=2)).isoformat() + "Z"
        with open(lock_path, "w") as f:
            json.dump({"ingestor_id": "crashed-ingest", "started_at": old}, f)
        # Stale lock detected -> removed -> reacquired with the new holder.
        path = ing._table_lock.acquire()
        assert path == lock_path
        meta = json.loads(open(lock_path).read())
        assert meta["ingestor_id"] == ing.ingestor_id
        ing._table_lock.release(lock_path)


def test_acquire_table_lock_noop_when_storage_path_missing(tmp_path):
    """No STORAGE_PATH (e.g. unit tests, local dev) -> the lock is
    skipped. Returns None, _release_table_lock(None) is a no-op."""
    from tracebloc_ingestor.config import Config as CfgCls

    missing = str(tmp_path / "never_exists")
    with patch.object(CfgCls, "STORAGE_PATH", missing):
        ing = make_ingestor(table_name="dataset_a", category=None)
        assert ing._table_lock.acquire() is None
        ing._table_lock.release(None)  # must not raise


def test_release_table_lock_idempotent(tmp_path):
    """Double-release (e.g. exception path + finally path both call it)
    must not raise."""
    from tracebloc_ingestor.config import Config as CfgCls

    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing = make_ingestor(table_name="dataset_a", category=None)
        path = ing._table_lock.acquire()
        ing._table_lock.release(path)
        ing._table_lock.release(path)  # idempotent, no raise


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
        lock_path = ing._table_lock.path()
        assert lock_path is not None
        import os as _os

        assert not _os.path.exists(
            lock_path
        ), f"lock leaked at {lock_path} after validate_data raised"


def test_lock_released_when_count_records_raises(tmp_path):
    """#221 bugbot HIGH-severity scenario: a failure in
    ``self._count_records`` (between validation and the Session block)
    used to escape without releasing the lock — neither the validation
    except nor the Session except covered it. try/finally fixes it."""
    from tracebloc_ingestor.config import Config as CfgCls

    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing = make_ingestor(records=[], category=None)
        with patch.object(ing, "validate_data", return_value=True), patch.object(
            ing, "_count_records", side_effect=RuntimeError("ouch")
        ):
            with pytest.raises(RuntimeError):
                ing.ingest("src")
        import os as _os

        assert not _os.path.exists(ing._table_lock.path()), "lock leaked"


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
        lock_path = ing._table_lock.path()
        with open(lock_path, "w"):
            pass  # empty file -> JSON parse fails
        old = _os.path.getmtime(lock_path) - (13 * 3600)  # 13h ago
        _os.utime(lock_path, (old, old))
        path = ing._table_lock.acquire()
        assert path == lock_path
        meta = json.loads(open(lock_path).read())
        assert meta["ingestor_id"] == ing.ingestor_id
        ing._table_lock.release(lock_path)


def test_acquire_table_lock_corrupt_but_fresh_blocks(tmp_path):
    """A corrupt lock that's RECENT (not stale by mtime) still blocks
    the second ingest — we don't auto-clear; the user has to remove it
    manually. Boundary test against the mtime fallback."""
    from tracebloc_ingestor.config import Config as CfgCls

    with patch.object(CfgCls, "STORAGE_PATH", str(tmp_path)):
        ing = make_ingestor(table_name="dataset_corrupt", category=None)
        lock_path = ing._table_lock.path()
        with open(lock_path, "w"):
            pass  # empty, JSON parse fails, mtime is now (fresh)
        with pytest.raises(RuntimeError, match="already running"):
            ing._table_lock.acquire()


# ---------------------------------------------------------------------------
# SRC_PATH pre-flight (validate_data) — #772 P2 / PR #218 (already on develop)
# ---------------------------------------------------------------------------


def test_check_src_path_empty_raises(clean_env):
    # SRC_PATH unset / blank -> N copies of "Source image not found" with no
    # actionable cause. Fail fast with the real reason.
    clean_env.setenv("SRC_PATH", "")
    with pytest.raises(RuntimeError, match="SRC_PATH is empty"):
        preflight.check_src_path()


def test_check_src_path_unset_raises(clean_env):
    # SRC_PATH not in env at all -> same outcome.
    clean_env.delenv("SRC_PATH", raising=False)
    with pytest.raises(RuntimeError, match="SRC_PATH is empty"):
        preflight.check_src_path()


def test_check_src_path_relative_raises(clean_env):
    # A relative SRC_PATH silently joins to a relative path at file-lookup
    # time; the validator surfaces the misconfiguration before that point.
    clean_env.setenv("SRC_PATH", "data/shared")  # not absolute
    with pytest.raises(RuntimeError, match="not an absolute path"):
        preflight.check_src_path()


def test_check_src_path_nonexistent_raises(clean_env, tmp_path):
    missing = tmp_path / "never_staged"
    clean_env.setenv("SRC_PATH", str(missing))
    with pytest.raises(RuntimeError, match="does not exist"):
        preflight.check_src_path()


def test_check_src_path_accepts_real_directory(clean_env, tmp_path):
    # A properly-staged absolute directory passes — no raise.
    clean_env.setenv("SRC_PATH", str(tmp_path))
    preflight.check_src_path()  # must not raise


def test_check_src_path_only_runs_for_file_bearing_categories():
    """The guard is gated on category — tabular / time-series have no
    sidecar dirs under SRC_PATH, so the preflight isn't applied (their
    CSV path is checked separately). This keeps tabular-only ingests
    working even when SRC_PATH isn't set."""
    from tracebloc_ingestor.utils.constants import TaskCategory
    from tracebloc_ingestor.ingestors.base import _FILE_BEARING_CATEGORIES

    for cat in (
        TaskCategory.TABULAR_CLASSIFICATION,
        TaskCategory.TABULAR_REGRESSION,
        TaskCategory.TIME_SERIES_FORECASTING,
        TaskCategory.TIME_TO_EVENT_PREDICTION,
    ):
        assert cat not in _FILE_BEARING_CATEGORIES
    # Image / text / segmentation / MLM / causal LM / seq2seq all need a staged
    # SRC_PATH.
    for cat in (
        TaskCategory.IMAGE_CLASSIFICATION,
        TaskCategory.OBJECT_DETECTION,
        TaskCategory.KEYPOINT_DETECTION,
        TaskCategory.SEMANTIC_SEGMENTATION,
        TaskCategory.TEXT_CLASSIFICATION,
        TaskCategory.MASKED_LANGUAGE_MODELING,
        TaskCategory.CAUSAL_LANGUAGE_MODELING,
        TaskCategory.SEQ2SEQ,
    ):
        assert cat in _FILE_BEARING_CATEGORIES


def test_check_src_path_required_for_token_classification():
    """token_classification reads per-row .txt sidecars from texts/ under
    SRC_PATH (same layout as text_classification), so it must get the
    early staging preflight instead of N file-transfer 'not found' errors."""
    from tracebloc_ingestor.utils.constants import TaskCategory
    from tracebloc_ingestor.ingestors.base import _FILE_BEARING_CATEGORIES

    assert TaskCategory.TOKEN_CLASSIFICATION in _FILE_BEARING_CATEGORIES


# ---------------------------------------------------------------------------
# #805: data-derived text profile on the global-metadata channel
# ---------------------------------------------------------------------------

_TEXT_PROFILE = {
    "schema_version": 1,
    "docs_sampled": 3,
    "scripts": {"Latin": 1.0},
}


@pytest.mark.parametrize(
    "category",
    [
        "MASKED_LANGUAGE_MODELING",
        "CAUSAL_LANGUAGE_MODELING",
        "SEQ2SEQ",
        "TEXT_CLASSIFICATION",
        "TOKEN_CLASSIFICATION",
    ],
)
def test_ingest_attaches_text_profile_for_nlp(category):
    """For every NLP category, the data-derived text profile is attached to
    file_options and sent as meta_data in the ingest summary."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    cat = getattr(TaskCategory, category)
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(records=records, category=cat, label_column=None)
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ), patch.object(
        base_mod, "compute_text_profile", return_value=_TEXT_PROFILE
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    call_kwargs = ing.api_client.send_ingest_summary.call_args[1]
    assert call_kwargs["meta_data"].get("text_profile") == _TEXT_PROFILE


def test_ingest_omits_text_profile_when_none_for_nlp():
    """No readable staged text -> profiler returns None -> field omitted; the
    ingest still registers cleanly."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(
        records=records,
        category=TaskCategory.TEXT_CLASSIFICATION,
        label_column="a",
    )
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ), patch.object(
        base_mod, "compute_text_profile", return_value=None
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    call_kwargs = ing.api_client.send_ingest_summary.call_args[1]
    assert "text_profile" not in call_kwargs["meta_data"]


def test_ingest_does_not_profile_non_nlp():
    """Non-NLP categories never touch the text-profile path."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(
        records=records,
        category=TaskCategory.IMAGE_CLASSIFICATION,
        label_column="a",
    )
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ), patch.object(
        base_mod, "compute_text_profile"
    ) as profile:
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    profile.assert_not_called()
    call_kwargs = ing.api_client.send_ingest_summary.call_args[1]
    assert "text_profile" not in call_kwargs["meta_data"]


# --- meta_data forwarding: file_options reach send_ingest_summary -----------


def test_ingest_forwards_file_options_as_meta_data():
    """Caller-supplied file_options (e.g. extension, target_size) must appear
    verbatim in the meta_data kwarg of send_ingest_summary."""
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(
        records=records,
        category=None,
        file_options={"extension": ".jpeg", "target_size": [256, 256]},
    )
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    call_kwargs = ing.api_client.send_ingest_summary.call_args[1]
    assert call_kwargs["meta_data"]["extension"] == ".jpeg"
    assert call_kwargs["meta_data"]["target_size"] == [256, 256]


def test_ingest_includes_number_of_columns_in_meta_data_for_tabular():
    """For tabular-family categories, number_of_columns is injected into
    file_options and must therefore appear in meta_data."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    records = [{"a": "1", "b": "2", "filename": "f1"}]
    ing = make_ingestor(
        records=records,
        category=TaskCategory.TABULAR_CLASSIFICATION,
        schema={"a": "INT", "b": "FLOAT"},
    )
    ing.database.get_table_schema.return_value = {"a": "INT", "b": "FLOAT"}
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    call_kwargs = ing.api_client.send_ingest_summary.call_args[1]
    assert call_kwargs["meta_data"]["number_of_columns"] == 2


def test_ingest_does_not_inject_number_of_columns_for_non_tabular():
    """Non-tabular categories (e.g. image_classification) must NOT get
    number_of_columns in meta_data — the count would be meaningless there."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(
        records=records,
        category=TaskCategory.IMAGE_CLASSIFICATION,
        schema={"a": "INT"},
    )
    ing.database.get_table_schema.return_value = {"a": "INT"}
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    call_kwargs = ing.api_client.send_ingest_summary.call_args[1]
    assert "number_of_columns" not in call_kwargs["meta_data"]


# --- Truthful registration-failure message about row state (0-record case) ---


def test_rows_state_clause_phrasing():
    """The parenthetical must never claim phantom rows: zero ingested -> says so;
    nonzero -> warns the rows are persisted."""
    assert (
        base_mod._rows_state_clause(0)
        == "no rows were ingested, so nothing was left in the database"
    )
    assert "3 already-ingested row(s)" in base_mod._rows_state_clause(3)


def test_zero_inserted_skips_registration():
    """stats['inserted_records'] == 0 → send_ingest_summary is skipped and
    ingest completes without error. The guard is on inserted_records, not on
    the return value of get_label_counts."""
    ing = make_ingestor(records=[{"a": "1", "filename": "f1"}], category=None)
    ing.database.insert_batch.return_value = ([], [])  # nothing committed
    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)  # must NOT raise
    ing.api_client.send_ingest_summary.assert_not_called()


def test_label_counts_empty_despite_inserts_raises():
    """If rows were inserted (inserted_records > 0) but get_label_counts returns
    {} — a DB accounting mismatch — ingest must raise RuntimeError rather than
    silently skip registration and leave committed MySQL rows unregistered."""
    ing = make_ingestor(records=[{"a": "1", "filename": "f1"}], category=None)
    # insert_batch reports 1 row committed, but the DB query returns nothing.
    ing.database.insert_batch.return_value = ([42], [])
    ing.database.get_label_counts.return_value = {}
    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(RuntimeError, match="get_label_counts returned nothing"):
            ing.ingest("src", batch_size=10)
    ing.api_client.send_ingest_summary.assert_not_called()


def test_mlm_header_only_csv_fails_before_table_creation(clean_env, tmp_path):
    """End-to-end fail-fast: a header-only MLM manifest is rejected during
    validation, BEFORE the destination table is created — so no orphan empty
    table is left behind (the #260 failure mode at the zero-records gate)."""
    from tracebloc_ingestor.config import Config
    from tracebloc_ingestor.utils.constants import TaskCategory

    src = tmp_path / "src"
    (src / "sequences").mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))
    clean_env.setenv("TABLE_NAME", "mlm_train")
    clean_env.setenv("DEST_PATH", str(tmp_path / "dest" / "mlm_train"))

    csv = tmp_path / "manifest.csv"
    pd.DataFrame(columns=["filename"]).to_csv(csv, index=False)  # header only

    ing = make_ingestor(
        records=[],
        category=TaskCategory.MASKED_LANGUAGE_MODELING,
        label_column=None,
    )
    ing.database.config = Config()  # real config so path-reading validators work

    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(ValueError, match="No data rows found"):
            ing.ingest(str(csv), batch_size=10)

    ing.database.create_table.assert_not_called()


def test_mlm_all_files_missing_fails_before_table_creation(clean_env, tmp_path):
    """The other zero-record path: a populated CSV whose every referenced file
    is missing is rejected before table creation."""
    from tracebloc_ingestor.config import Config
    from tracebloc_ingestor.utils.constants import TaskCategory

    src = tmp_path / "src"
    (src / "sequences").mkdir(parents=True)  # empty -> referenced files missing
    clean_env.setenv("SRC_PATH", str(src))
    clean_env.setenv("TABLE_NAME", "mlm_train")
    clean_env.setenv("DEST_PATH", str(tmp_path / "dest" / "mlm_train"))

    csv = tmp_path / "manifest.csv"
    pd.DataFrame({"filename": ["doc1", "doc2"]}).to_csv(csv, index=False)

    ing = make_ingestor(
        records=[],
        category=TaskCategory.MASKED_LANGUAGE_MODELING,
        label_column=None,
    )
    ing.database.config = Config()

    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(ValueError, match="No referenced data files"):
            ing.ingest(str(csv), batch_size=10)

    ing.database.create_table.assert_not_called()


# ── #227: compensating delete on failed registration ────────────────────────
# Rows commit per batch during the loop; when the run fails before its dataset
# registers, the failure path must delete exactly this run's rows (identified
# by ingestor_id) so the table is clean for a re-run — instead of leaving
# orphans that trip the stale-table guard (#336).


def _ingest_expecting(ing, exc_type):
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(exc_type):
            ing.ingest("src", batch_size=10)


def test_failed_registration_triggers_compensating_delete():
    """send_ingest_summary fails terminally → the run's rows are deleted by
    ingestor_id and the original error still propagates."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    ing = make_ingestor(
        records=[{"a": "1", "filename": "f1"}],
        category=TaskCategory.IMAGE_CLASSIFICATION,
        label_column="a",
    )
    ing.api_client.send_ingest_summary.side_effect = RuntimeError("backend rejected")
    _ingest_expecting(ing, RuntimeError)
    ing.database.delete_by_ingestor_id.assert_called_once_with(
        ing.table_name, ing.ingestor_id
    )


def test_successful_registration_never_deletes():
    """Happy path: registration succeeds → the compensating delete must not run."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    ing = make_ingestor(
        records=[{"a": "1", "filename": "f1"}],
        category=TaskCategory.IMAGE_CLASSIFICATION,
        label_column="a",
    )
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)
    ing.database.delete_by_ingestor_id.assert_not_called()


def test_cleanup_failure_preserves_original_error(caplog):
    """The delete itself failing must not mask the registration error — the
    original exception propagates and the orphan state is logged CRITICAL."""
    import logging
    from tracebloc_ingestor.utils.constants import TaskCategory

    ing = make_ingestor(
        records=[{"a": "1", "filename": "f1"}],
        category=TaskCategory.IMAGE_CLASSIFICATION,
        label_column="a",
    )
    ing.api_client.send_ingest_summary.side_effect = RuntimeError("backend rejected")
    ing.database.delete_by_ingestor_id.side_effect = Exception("mysql went away")
    with caplog.at_level(logging.CRITICAL):
        _ingest_expecting(ing, RuntimeError)  # ORIGINAL error, not the cleanup one
    assert any("Compensating delete FAILED" in r.message for r in caplog.records)


def test_no_rows_inserted_no_delete():
    """A failure before any row was inserted must not attempt a delete —
    the table may not even exist yet."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    ing = make_ingestor(
        records=[],
        category=TaskCategory.IMAGE_CLASSIFICATION,
        label_column="a",
    )
    # Force a failure after the commit point but before registration, with
    # zero inserted rows: get_label_counts blows up on an empty run.
    ing.database.get_label_counts.side_effect = RuntimeError("boom")
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(RuntimeError):
            ing.ingest("src", batch_size=10)
    ing.database.delete_by_ingestor_id.assert_not_called()


def test_late_failure_after_registration_never_deletes():
    """The dataset_registered guard: an exception AFTER send_ingest_summary
    succeeded (e.g. in summary rendering) must propagate WITHOUT deleting the
    now-registered dataset's rows. Mutation check: dropping the
    `and not dataset_registered` condition fails this test."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    ing = make_ingestor(
        records=[{"a": "1", "filename": "f1"}],
        category=TaskCategory.IMAGE_CLASSIFICATION,
        label_column="a",
    )
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        ing, "_log_summary", side_effect=RuntimeError("render blew up")
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(RuntimeError):
            ing.ingest("src", batch_size=10)
    ing.api_client.send_ingest_summary.assert_called_once()
    ing.database.delete_by_ingestor_id.assert_not_called()


# ── backend#1028 item 2: orphan-row reconciliation on start ──────────────────
# The #227 compensating delete only runs on a CAUGHT failure. A hard kill
# (OOMKilled / SIGKILL mid-ingest) bypasses it, leaving the dead run's rows in
# the table with its dataset never registered — and the k8s Job retry then
# duplicates them. Every ingest therefore (1) reclaims rows of
# journaled-started-but-never-registered prior runs BEFORE processing,
# (2) journals its own start BEFORE its first insert, and (3) journals its
# registration right after send_ingest_summary returns.


def _run_happy_ingest(ing):
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        return ing.ingest("src", batch_size=10)


def test_reconcile_and_start_journal_run_before_first_insert():
    """Order contract: create_table → reclaim orphans → journal own start →
    first insert_batch. Reconciling after inserting would misread the run's
    own rows; journaling after inserting would let a kill in between leave
    rows the journal never heard about (undetectable orphans)."""
    ing = make_ingestor(records=[{"a": "1"}], label_column="a")
    _run_happy_ingest(ing)

    ing.database.reclaim_dead_run_rows.assert_called_once_with(
        ing.table_name, ing.ingestor_id
    )
    ing.database.record_ingest_started.assert_called_once_with(
        ing.table_name, ing.ingestor_id
    )
    names = [name for name, _, _ in ing.database.mock_calls]
    assert names.index("create_table") < names.index("reclaim_dead_run_rows")
    assert names.index("reclaim_dead_run_rows") < names.index(
        "record_ingest_started"
    )
    assert names.index("record_ingest_started") < names.index("insert_batch")


def test_successful_registration_marks_journal_registered():
    """After send_ingest_summary returns, the run must be journaled as
    REGISTERED — the durable marker that stops any future reconcile pass from
    reclaiming this run's rows."""
    ing = make_ingestor(records=[{"a": "1"}], label_column="a")
    _run_happy_ingest(ing)
    ing.database.mark_ingest_registered.assert_called_once_with(
        ing.table_name, ing.ingestor_id
    )


def test_failed_registration_never_marks_journal_registered():
    """A run whose registration failed must stay started-but-unregistered in
    the journal (its rows are removed by the #227 delete anyway, which still
    fires — asserted here so the two cleanups are known to coexist)."""
    ing = make_ingestor(records=[{"a": "1"}], label_column="a")
    ing.api_client.send_ingest_summary.side_effect = RuntimeError(
        "backend rejected"
    )
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(RuntimeError):
            ing.ingest("src", batch_size=10)
    ing.database.mark_ingest_registered.assert_not_called()
    ing.database.delete_by_ingestor_id.assert_called_once_with(
        ing.table_name, ing.ingestor_id
    )


def test_mark_registered_failure_never_fails_a_registered_run(caplog):
    """The journal UPDATE failing AFTER successful registration must not fail
    the run (the dataset exists!) nor trigger the #227 delete — raising here
    would hand the retry a journal entry telling it to purge a registered
    dataset's rows. Logged CRITICAL instead."""
    import logging

    ing = make_ingestor(records=[{"a": "1"}], label_column="a")
    ing.database.mark_ingest_registered.side_effect = Exception("journal down")
    with caplog.at_level(logging.CRITICAL):
        _run_happy_ingest(ing)  # must NOT raise
    ing.database.delete_by_ingestor_id.assert_not_called()
    assert any(
        "Failed to journal registration" in r.message for r in caplog.records
    )


def test_reclaim_failure_never_blocks_the_ingest(caplog):
    """Reconciliation failing (journal table unreachable, unexpected SQL
    error) must degrade to today's status quo — orphans stay, the ingest
    itself proceeds — never brick every ingest into that table. Logged
    CRITICAL."""
    import logging

    ing = make_ingestor(records=[{"a": "1"}], label_column="a")
    ing.database.reclaim_dead_run_rows.side_effect = Exception("mysql sick")
    with caplog.at_level(logging.CRITICAL):
        _run_happy_ingest(ing)  # must NOT raise
    ing.database.insert_batch.assert_called()
    ing.database.record_ingest_started.assert_called_once()
    assert any(
        "Orphan-row reconciliation failed" in r.message for r in caplog.records
    )


def test_zero_record_run_journals_start_but_not_registered():
    """A run that inserts nothing sends no summary, so it must journal its
    start but never its registration — its (row-less) journal entry is inert
    for future reconcile passes, which only reclaim ids that still own rows."""
    ing = make_ingestor(records=[], label_column="a")
    ing.database.get_label_counts.return_value = {}
    _run_happy_ingest(ing)
    ing.database.record_ingest_started.assert_called_once()
    ing.database.mark_ingest_registered.assert_not_called()
