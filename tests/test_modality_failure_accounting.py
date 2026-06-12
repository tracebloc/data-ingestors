"""Per-modality guards for the batch-send failure accounting fixed in #223.

#223 was verified end-to-end with a real ingest for token_classification
only. These tests close the per-modality gap in three directions:

1. The ``api_send_failed`` accounting in ``BaseIngestor._flush_batch`` is
   exercised for EVERY template category (the #223 tests ran it with
   ``category=None`` only, which skips the file-transfer branch the file-
   bearing categories take).
2. The template scripts' ``except Exception`` handler must re-raise.
   Five templates (image_classification, tabular_classification,
   tabular_regression, time_series_forecasting, time_to_event_prediction)
   used to log-and-swallow, so a hard failure raised by ``ingest()`` —
   validation error, DB error, or the fail-loud backend-registration
   RuntimeErrors from base.py — ended with exit code 0 and a K8s Job
   marked Succeeded. (#223's structural test only covered the
   ``failed_records`` branch, which exits via SystemExit and bypasses the
   handler.)
3. After a MID-batch DB failure, ``_process_batch`` must send the API the
   records that actually inserted. ``zip(ids, batch)`` paired positionally
   and truncated to ``len(ids)``, sending the DB-failed record (a phantom
   backend entry with no MySQL row) and dropping the last inserted one (a
   committed row the platform never sees).

Plus the #223 diagnostics fix (log ``HTTP <status>: <body>`` instead of a
100-char stub) mirrored onto the remaining API-client methods.
"""

from __future__ import annotations

import ast
import logging
from pathlib import Path
from typing import Any, Dict, Generator
from unittest.mock import MagicMock, patch

import pytest
import requests

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.api.client import APIClient
from tracebloc_ingestor.ingestors import base as base_mod
from tracebloc_ingestor.ingestors.base import BaseIngestor
from tracebloc_ingestor.utils.constants import TaskCategory


# ---------------------------------------------------------------------------
# helpers (mirror test_batch_send_failure_accounting.py)
# ---------------------------------------------------------------------------

# One entry per template directory — the 11 supported modalities.
_TEMPLATE_CATEGORIES = [
    TaskCategory.IMAGE_CLASSIFICATION,
    TaskCategory.KEYPOINT_DETECTION,
    TaskCategory.MASKED_LANGUAGE_MODELING,
    TaskCategory.OBJECT_DETECTION,
    TaskCategory.SEMANTIC_SEGMENTATION,
    TaskCategory.TABULAR_CLASSIFICATION,
    TaskCategory.TABULAR_REGRESSION,
    TaskCategory.TEXT_CLASSIFICATION,
    TaskCategory.TIME_SERIES_FORECASTING,
    TaskCategory.TIME_TO_EVENT_PREDICTION,
    TaskCategory.TOKEN_CLASSIFICATION,
]


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


def _run_ingest(ing, batch_size=10):
    """Run ingest with Session / validation / file-transfer patched out so
    every category — including the file-bearing ones — runs without a real
    filesystem. Captures the logged summary."""
    captured = {}
    real_log = BaseIngestor._log_summary

    def spy(self, summary):
        captured["summary"] = summary
        return real_log(self, summary)

    with patch.object(base_mod, "Session") as Sess, \
         patch.object(BaseIngestor, "_log_summary", spy), \
         patch.object(ing, "validate_data", return_value=True), \
         patch.object(base_mod, "map_file_transfer",
                      side_effect=lambda c, r, o: r):
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest("src", batch_size=batch_size)
    return failed, captured.get("summary")


# ---------------------------------------------------------------------------
# 1. api_send_failed accounting holds for every template category
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("category", _TEMPLATE_CATEGORIES)
def test_api_send_failure_counted_for_every_category(category):
    """Every batch POST rejected -> every record comes back as a failed
    record for EVERY modality, so the caller exits non-zero. Covers the
    file-transfer branch in _ingest_with_lock that ``category=None``
    (the #223 tests) skips."""
    records = [{"a": "1", "filename": "f1"}, {"a": "2", "filename": "f2"}]
    ing = make_ingestor(records=records, category=category)
    ing.api_client.send_batch.return_value = False  # every batch POST rejected

    failed, summary = _run_ingest(ing)

    assert len(failed) == 2, f"{category}: api-send failures not surfaced"
    assert all(f["error"] == "api_send_failed" for f in failed)
    assert summary.inserted_records == 2
    assert summary.api_sent_records == 0
    assert summary.has_failures is True


@pytest.mark.parametrize("category", _TEMPLATE_CATEGORIES)
def test_clean_run_has_no_failures_for_every_category(category):
    """Control: with the API accepting every batch, no category reports
    failures (guards against the file-transfer patch masking a skip)."""
    records = [{"a": "1", "filename": "f1"}, {"a": "2", "filename": "f2"}]
    ing = make_ingestor(records=records, category=category)

    failed, summary = _run_ingest(ing)

    assert failed == []
    assert summary.api_sent_records == summary.inserted_records == 2
    assert summary.has_failures is False


# ---------------------------------------------------------------------------
# 2. templates must re-raise from their except-Exception handler
# ---------------------------------------------------------------------------

_TEMPLATES = sorted(Path(__file__).parent.parent.glob("templates/*/*.py"))


def _main_except_handlers(template: Path):
    """Yield every ``except Exception`` handler inside the template's
    ``main()`` function."""
    tree = ast.parse(template.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            for sub in ast.walk(node):
                if isinstance(sub, ast.ExceptHandler):
                    if isinstance(sub.type, ast.Name) and sub.type.id == "Exception":
                        yield sub


@pytest.mark.parametrize("template", _TEMPLATES, ids=lambda p: p.parent.name)
def test_template_except_handler_reraises(template):
    """A template's ``except Exception`` must re-raise. Log-and-swallow
    turned any exception escaping ``ingest()`` — validation failure, DB
    error, the backend-registration RuntimeErrors from base.py — into
    exit code 0 and a K8s Job marked Succeeded, the same silent-success
    class #223 fixed for batch POST failures."""
    handlers = list(_main_except_handlers(template))
    assert handlers, f"{template}: no except-Exception handler in main()"
    for handler in handlers:
        has_raise = any(
            isinstance(stmt, ast.Raise) for stmt in ast.walk(handler)
        )
        assert has_raise, (
            f"{template}: except-Exception handler in main() does not "
            f"re-raise — a hard ingest failure would exit 0"
        )


# ---------------------------------------------------------------------------
# 3. mid-batch DB failure: only the inserted records are sent to the API
# ---------------------------------------------------------------------------

def test_mid_batch_db_failure_sends_only_inserted_records():
    """3-record batch, middle record fails DB insert. The API send must
    carry exactly the two records that inserted (#0 and #2) — not a
    positional ``batch[:len(ids)]`` prefix that would send the DB-failed
    #1 (phantom backend record with no MySQL row) and drop the inserted
    #2 (committed row the platform never sees)."""
    records = [{"a": str(i), "filename": f"f{i}"} for i in range(3)]
    ing = make_ingestor(records=records, category=None)
    seen = {}

    def fake_insert(table_name, batch):
        seen["batch"] = list(batch)
        # Middle record fails; failure carries a COPY of the record (the
        # real insert_batch builds processed_record = {**record, ...}).
        failed_copy = {**batch[1], "updated_at": "now"}
        return [10, 12], [{"record": failed_copy, "error": "dup key"}]

    ing.database.insert_batch.side_effect = fake_insert

    failed, summary = _run_ingest(ing)

    sent = ing.api_client.send_batch.call_args[0][0]
    sent_data_ids = {record["data_id"] for _, record in sent}
    batch = seen["batch"]
    assert sent_data_ids == {batch[0]["data_id"], batch[2]["data_id"]}, (
        "API send must carry the records that actually inserted"
    )
    assert len(sent) == 2
    # Accounting unchanged: the DB failure is the only failed record.
    assert [f["error"] for f in failed] == ["dup key"]
    assert summary.inserted_records == 2
    assert summary.api_sent_records == 2
    assert summary.failed_records == 1


# ---------------------------------------------------------------------------
# 4. registration-step diagnostics: full backend error logged, not a stub
#    (mirrors the #223 send_batch fix on the remaining client methods)
# ---------------------------------------------------------------------------

# Well past the old str(e)[:100] cutoff ("HTTP 400: " left ~90 visible chars).
_DRF_400_BODY = (
    '{"error": ["No data found for table name padding padding padding '
    'padding padding padding padding to push the explanation well past '
    'the first hundred characters of the message."]}'
)


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(
            lambda c: c.send_global_meta_meta("tbl", {"a": "INT"}, {}),
            id="send_global_meta_meta",
        ),
        pytest.param(
            lambda c: c.send_generate_edge_label_meta("tbl", "ing", "train"),
            id="send_generate_edge_label_meta",
        ),
        pytest.param(
            lambda c: c.prepare_dataset(
                TaskCategory.IMAGE_CLASSIFICATION, "ing", "image", "train"
            ),
            id="prepare_dataset",
        ),
    ],
)
def test_registration_call_400_logs_status_and_full_error(call, caplog):
    client = _client()
    with patch.object(client.session, "post",
                      return_value=_resp(400, text=_DRF_400_BODY)), \
         patch.object(client.session, "get",
                      return_value=_resp(400, text=_DRF_400_BODY)):
        with caplog.at_level(logging.ERROR, logger="tracebloc_ingestor.api.client"):
            assert call(client) is False
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "HTTP 400" in joined
    # The tail of the body — beyond the old [:100] truncation — must be
    # visible so the operator can see WHY the backend rejected the call.
    assert "well past the first hundred characters" in joined


def test_create_dataset_400_logs_full_error_and_raises(caplog):
    client = _client()
    with patch.object(client.session, "post",
                      return_value=_resp(400, text=_DRF_400_BODY)):
        with caplog.at_level(logging.ERROR, logger="tracebloc_ingestor.api.client"):
            with pytest.raises(requests.exceptions.HTTPError):
                client.create_dataset(
                    ingestor_id="ing", category=TaskCategory.IMAGE_CLASSIFICATION
                )
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "HTTP 400" in joined
    assert "well past the first hundred characters" in joined


def test_create_dataset_exception_propagates_out_of_ingest():
    """create_dataset is the one registration call whose return value
    isn't checked in _ingest_with_lock — it signals failure by raising.
    Guard that the raise actually escapes ingest() (with the template
    re-raise fix, that now fails the run in every modality)."""
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(records=records, category=None)
    ing.api_client.create_dataset.side_effect = requests.exceptions.HTTPError(
        "HTTP 401: token expired"
    )

    with patch.object(base_mod, "Session") as Sess, \
         patch.object(ing, "validate_data", return_value=True):
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(requests.exceptions.HTTPError):
            ing.ingest("src", batch_size=10)
