"""Regression tests for ingest atomicity w.r.t. validation (#260).

#260: the dataset table used to be created in ``BaseIngestor.__init__`` —
BEFORE ``validate_data`` ran. A validator-rejected ingest (e.g. duplicate
column names) then left an orphaned empty table that blocked the *next* ingest
with the "table already exists … drop it or use a new name" guard, so a
rejected ingest was not idempotent (the operator had to manually drop the
table). The fix defers ``create_table`` until AFTER ``validate_data`` passes,
so a rejection leaves nothing behind.

These pin that ordering: validation rejection must create no table, while a
clean run still does (so the deferral didn't break the happy path).
"""

from __future__ import annotations

from typing import Any, Dict, Generator
from unittest.mock import MagicMock, patch

import pytest

from tracebloc_ingestor.ingestors import base as base_mod
from tracebloc_ingestor.ingestors.base import BaseIngestor
from tracebloc_ingestor.validators.base import ValidationResult


class _FakeIngestor(BaseIngestor):
    """Concrete BaseIngestor whose read_data yields preset records."""

    def __init__(self, records, **kwargs):
        self._records = records
        super().__init__(**kwargs)

    def read_data(self, source: Any) -> Generator[Dict[str, Any], None, None]:
        yield from self._records


def _make_ingestor(**overrides):
    db = MagicMock(name="Database")
    db.create_table.return_value = MagicMock(name="table")
    db.insert_batch.return_value = ([1], [])  # ids, db_failures
    db.get_table_schema.return_value = {"a": "INT"}
    api = MagicMock(name="APIClient")
    for m in (
        "send_batch",
        "send_generate_edge_label_meta",
        "send_global_meta_meta",
        "prepare_dataset",
    ):
        getattr(api, m).return_value = True
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
    return _FakeIngestor([{"a": "1", "filename": "f"}], **kwargs)


def test_validator_rejection_creates_no_table():
    """A validator-rejected ingest must leave NO table behind (#260): the
    table is created only after ``validate_data`` passes, so the rejection
    never creates the orphaning table that blocked the next ingest."""
    ing = _make_ingestor()
    rejecting = MagicMock()
    rejecting.name = "Rejecting"
    rejecting.validate.return_value = ValidationResult(
        False, ["Invalid table names found: duplicate column names"], [], {}
    )
    with patch.object(base_mod, "map_validators", return_value=[rejecting]):
        with pytest.raises(ValueError):
            ing.ingest("src")

    ing.database.create_table.assert_not_called()


def test_clean_validation_still_creates_table():
    """Control: a passing validation DOES create the table — the #260 deferral
    must not break the happy path."""
    ing = _make_ingestor()
    ok = MagicMock()
    ok.name = "OK"
    ok.validate.return_value = ValidationResult(True, [], [], {})
    with patch.object(base_mod, "map_validators", return_value=[ok]), patch.object(
        base_mod, "Session"
    ) as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        ing.ingest("src", batch_size=10)

    ing.database.create_table.assert_called_once()
