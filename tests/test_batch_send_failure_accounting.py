"""Batch API-send failures must surface, not vanish.

Regression guards for a real silent-failure incident: every batch POST to
``/global_meta/<table>/`` was rejected with HTTP 400, yet the run finished
with "All records processed successfully", a summary claiming a 100% success
rate, and exit code 0. Files were copied and MySQL rows inserted, but the
backend had zero records — the next platform call failed with "No data found
for table name".

Three swallow points, three guard groups below:

1. ``APIClient.send_batch`` logged ``str(e)[:100]`` — the manually-raised
   HTTPError carried no ``.response``, and 100 chars truncated the message
   right after "HTTP 400: ", hiding the DRF field error.
2. ``BaseIngestor`` only skipped the ``api_sent_records`` increment when
   ``api_success`` was False; the records never reached ``failed_records``,
   so ``ingest()`` returned ``[]`` and callers exited 0. Exceptions from
   ``_process_batch`` were likewise logged and dropped from every counter.
3. The template scripts never exited non-zero on failed records.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, Generator
from unittest.mock import MagicMock, patch

import pytest
import requests

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.api.client import APIClient
from tracebloc_ingestor.ingestors import base as base_mod
from tracebloc_ingestor.ingestors.base import BaseIngestor, IngestionSummary


# ---------------------------------------------------------------------------
# helpers (mirror test_api_client_methods.py / test_ingestor_base.py)
# ---------------------------------------------------------------------------

def _client(**overrides):
    defaults = dict(BACKEND_TOKEN="tok", CLIENT_USERNAME=None,
                    CLIENT_PASSWORD=None, EDGE_ENV="prod", TITLE=None)
    defaults.update(overrides)
    return APIClient(Config(**defaults))


def _resp(status=200, json_body=None, text=""):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_body if json_body is not None else {}
    r.text = text
    return r


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
# 1. send_batch logs the actual backend error, not a 100-char stub
# ---------------------------------------------------------------------------

# A realistic DRF batch-rejection body: well past the old 100-char cutoff
# (str(e) started with "HTTP 400: ", leaving ~90 chars of body).
_DRF_400_BODY = (
    '[{"data_id": ["This field may not be blank."], '
    '"label": ["Object with label=tok_cls_O does not exist. '
    'Padding padding padding padding padding to push the field error '
    'well past the first hundred characters of the message."]}]'
)


def test_send_batch_400_logs_status_and_full_field_error(caplog):
    client = _client()
    with patch.object(client.session, "post", return_value=_resp(400, text=_DRF_400_BODY)):
        with caplog.at_level(logging.ERROR, logger="tracebloc_ingestor.api.client"):
            assert client.send_batch([(1, {"data_id": "a"})], "tbl", "ing") is False
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "Error sending batch to API" in joined
    assert "HTTP 400" in joined
    # The tail of the DRF body — beyond the old [:100] truncation — must be
    # visible so the operator can see WHY the backend rejected the batch.
    assert "well past the first hundred characters" in joined


def test_send_batch_400_body_capped_at_2000_chars(caplog):
    client = _client()
    huge = "x" * 5000
    with patch.object(client.session, "post", return_value=_resp(400, text=huge)):
        with caplog.at_level(logging.ERROR, logger="tracebloc_ingestor.api.client"):
            client.send_batch([(1, {"data_id": "a"})], "tbl", "ing")
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "x" * 2000 in joined
    assert "x" * 2001 not in joined


def test_send_batch_connection_error_still_logged(caplog):
    client = _client()
    with patch.object(
        client.session, "post",
        side_effect=requests.exceptions.ConnectionError("conn refused"),
    ):
        with caplog.at_level(logging.ERROR, logger="tracebloc_ingestor.api.client"):
            assert client.send_batch([(1, {"data_id": "a"})], "tbl", "ing") is False
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "Error sending batch to API" in joined
    assert "conn refused" in joined


# ---------------------------------------------------------------------------
# 2. ingest() surfaces API-send failures: returned, counted, summarized
# ---------------------------------------------------------------------------

def _run_ingest(ing, batch_size=10):
    """Run ingest with Session patched out; capture the logged summary."""
    captured = {}
    real_log = BaseIngestor._log_summary

    def spy(self, summary):
        captured["summary"] = summary
        return real_log(self, summary)

    with patch.object(base_mod, "Session") as Sess, \
         patch.object(BaseIngestor, "_log_summary", spy):
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest("src", batch_size=batch_size)
    return failed, captured.get("summary")


def test_ingest_api_send_failure_returns_failed_records():
    records = [{"a": "1", "filename": "f1"}, {"a": "2", "filename": "f2"}]
    ing = make_ingestor(records=records, category=None)
    ing.api_client.send_batch.return_value = False  # every batch POST rejected

    failed, summary = _run_ingest(ing)

    # Every inserted-but-unsent record comes back as a failed record, so
    # cli.run.main returns 1 and the template scripts sys.exit(1).
    assert len(failed) == 2
    assert all(f["error"] == "api_send_failed" for f in failed)

    # The summary must not claim the records shipped.
    assert summary.inserted_records == 2
    assert summary.api_sent_records == 0
    assert summary.has_failures is True


def test_ingest_api_send_failure_on_final_partial_batch():
    # 3 records with batch_size=2 exercises BOTH flush sites (in-loop and
    # final partial batch).
    records = [{"a": str(i), "filename": f"f{i}"} for i in range(3)]
    ing = make_ingestor(records=records, category=None)
    ing.api_client.send_batch.return_value = False
    # insert_batch must echo the actual batch size, not the fixture's [1, 2]
    ing.database.insert_batch.side_effect = lambda t, b: (list(range(len(b))), [])

    failed, summary = _run_ingest(ing, batch_size=2)

    assert len(failed) == 3
    assert summary.inserted_records == 3
    assert summary.api_sent_records == 0


def test_ingest_api_success_keeps_failed_records_empty():
    # Control: a clean run is unchanged by the new accounting.
    records = [{"a": "1", "filename": "f1"}, {"a": "2", "filename": "f2"}]
    ing = make_ingestor(records=records, category=None)

    failed, summary = _run_ingest(ing)

    assert failed == []
    assert summary.api_sent_records == summary.inserted_records == 2
    assert summary.has_failures is False


def test_ingest_batch_exception_counts_whole_batch_as_failed():
    # An exception escaping _process_batch (e.g. a DB connection drop mid
    # insert) used to be logged and dropped from every counter.
    records = [{"a": "1", "filename": "f1"}, {"a": "2", "filename": "f2"}]
    ing = make_ingestor(records=records, category=None)
    ing.database.insert_batch.side_effect = RuntimeError("db gone")

    failed, summary = _run_ingest(ing)

    assert len(failed) == 2
    assert all("db gone" in f["error"] for f in failed)
    assert summary.failed_records == 2
    assert summary.inserted_records == 0
    assert summary.has_failures is True


def test_ingest_partial_db_failure_not_double_counted():
    # 1 of 2 rows inserts; the API send for the inserted row succeeds. Only
    # the DB failure is returned — the inserted+sent row is not.
    records = [{"a": "1", "filename": "f1"}, {"a": "2", "filename": "f2"}]
    ing = make_ingestor(records=records, category=None)
    db_failure = {"record": {"a": "2"}, "error": "dup key"}
    ing.database.insert_batch.return_value = ([1], [db_failure])

    failed, summary = _run_ingest(ing)

    assert failed == [db_failure]
    assert summary.inserted_records == 1
    assert summary.api_sent_records == 1
    assert summary.failed_records == 1


def test_ingest_mid_batch_db_failure_plus_api_failure_tags_right_records():
    # Bugbot regression guard: insert_batch's per-record fallback appends
    # successes in scan order, so after a MID-batch DB failure the inserted
    # records are NOT the first len(ids) entries. The api_send_failed tag
    # must land on the records that actually inserted (here: #0 and #2),
    # not on a batch[:len(ids)] prefix that would include the DB-failed #1
    # and omit #2.
    records = [{"a": str(i), "filename": f"f{i}"} for i in range(3)]
    ing = make_ingestor(records=records, category=None)
    ing.api_client.send_batch.return_value = False

    def fake_insert(table_name, batch):
        # Middle record fails; failure carries a COPY of the record (the
        # real insert_batch builds processed_record = {**record, ...}).
        failed_copy = {**batch[1], "updated_at": "now"}
        return [10, 12], [{"record": failed_copy, "error": "dup key"}]

    ing.database.insert_batch.side_effect = fake_insert

    failed, summary = _run_ingest(ing)

    api_failed = [f for f in failed if f["error"] == "api_send_failed"]
    assert {f["record"]["a"] for f in api_failed} == {"0", "2"}
    assert [f["error"] for f in failed if f["error"] != "api_send_failed"] == ["dup key"]
    assert summary.inserted_records == 2
    assert summary.api_sent_records == 0
    assert summary.failed_records == 1


# ---------------------------------------------------------------------------
# 3. templates exit non-zero when ingest() returns failed records
# ---------------------------------------------------------------------------

_TEMPLATES = sorted(Path(__file__).parent.parent.glob("templates/*/*.py"))


@pytest.mark.parametrize("template", _TEMPLATES, ids=lambda p: p.parent.name)
def test_template_exits_nonzero_on_failed_records(template):
    """Each template must route its run through the shared
    ``run_ingestion`` helper, which owns the exit contract this test used
    to pin per-template: log every failed record, ``sys.exit(1)`` on
    failed records, re-raise hard errors. The eleven inlined copies of
    that block drifted (five swallowed exceptions and exited 0 — #230),
    so the duplication was extracted; the behavioral guarantees are now
    covered directly in tests/test_template_runner.py and this check pins
    the call-site instead of the inlined pattern."""
    src = template.read_text(encoding="utf-8")
    # Imported from the package root (not a local re-implementation) …
    m = re.search(
        r"^from tracebloc_ingestor import \((?P<names>[^)]*)\)", src, re.M | re.S
    )
    assert m and "run_ingestion" in m.group("names"), (
        f"{template}: does not import run_ingestion from tracebloc_ingestor"
    )
    # … and actually called with the constructed ingestor.
    assert re.search(r"run_ingestion\(\s*ingestor,", src), (
        f"{template}: main() does not call run_ingestion(ingestor, ...)"
    )
