"""Gate tests: BaseIngestor reclaims its staged source only on a verified,
clean load (#346).

``reclaim_source`` itself (and all its safety guards) is unit-tested in
test_file_transfer_reclaim.py. Here we patch it out and assert only the GATE
in ``_ingest_with_lock``: it fires after a fully-registered, failure-free run
and stays quiet on any partial/failed run — matching the compensating-delete
branch, which deliberately leaves staged files in place for retry/inspection.
"""

from __future__ import annotations

from typing import Any, Dict, Generator
from unittest.mock import MagicMock, patch

import pytest

from tracebloc_ingestor.ingestors import base as base_mod
from tracebloc_ingestor.ingestors.base import BaseIngestor
from tracebloc_ingestor.validators.base import ValidationResult


class _FakeIngestor(BaseIngestor):
    """Concrete BaseIngestor whose read_data yields preset records and whose
    record processing is a pass-through — we exercise the reclaim GATE, not the
    (separately tested) content-hash / record-cleaning path."""

    def __init__(self, records, **kwargs):
        self._records = records
        super().__init__(**kwargs)

    def read_data(self, source: Any) -> Generator[Dict[str, Any], None, None]:
        yield from self._records

    def process_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return dict(record)


def _make_ingestor(records=None, insert_return=([1], []), **overrides):
    db = MagicMock(name="Database")
    db.create_table.return_value = MagicMock(name="table")
    db.insert_batch.return_value = insert_return  # (inserted_ids, db_failures)
    db.get_table_schema.return_value = {"a": "INT"}
    db.get_label_counts.return_value = {"x": 1}  # truthy -> summary is sent
    db.iter_label_counts.return_value = [("x", 1)]
    api = MagicMock(name="APIClient")
    kwargs = dict(
        database=db,
        api_client=api,
        table_name="tbl",
        schema={"a": "INT"},
        intent="train",
        category=None,
    )
    kwargs.update(overrides)
    return _FakeIngestor(
        records if records is not None else [{"a": "1", "filename": "f"}], **kwargs
    )


def _run(ing):
    """Drive a full ingest with validation stubbed to pass and the DB Session
    mocked, returning the patched ``reclaim_source`` mock for assertions."""
    ok = MagicMock()
    ok.name = "OK"
    ok.validate.return_value = ValidationResult(True, [], [], {})
    with patch.object(base_mod, "map_validators", return_value=[ok]), patch.object(
        base_mod, "Session"
    ) as Sess, patch.object(base_mod, "reclaim_source") as reclaim:
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest("src", batch_size=10)
    return reclaim, failed


def test_reclaim_called_on_clean_success():
    ing = _make_ingestor()
    reclaim, failed = _run(ing)
    assert failed == []
    reclaim.assert_called_once_with(ing.database.config)


def test_reclaim_not_called_when_a_record_fails_but_dataset_registers():
    # A DB failure on part of the batch: the dataset still registers (some
    # rows inserted, summary sent) but failed_records is non-empty -> the run
    # is "partial", so the source must be KEPT for retry/inspection.
    ing = _make_ingestor(
        insert_return=([1], [{"record": {"a": "2"}, "error": "boom"}]),
    )
    reclaim, failed = _run(ing)
    assert failed  # non-empty
    reclaim.assert_not_called()


def test_reclaim_not_called_when_summary_send_fails():
    # send_ingest_summary raising drops into the compensating-delete branch:
    # dataset_registered stays False and the run re-raises -> no reclaim.
    ing = _make_ingestor()
    ing.api_client.send_ingest_summary.side_effect = RuntimeError("5xx")
    ok = MagicMock()
    ok.name = "OK"
    ok.validate.return_value = ValidationResult(True, [], [], {})
    with patch.object(base_mod, "map_validators", return_value=[ok]), patch.object(
        base_mod, "Session"
    ) as Sess, patch.object(base_mod, "reclaim_source") as reclaim:
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(RuntimeError):
            ing.ingest("src", batch_size=10)
    reclaim.assert_not_called()
