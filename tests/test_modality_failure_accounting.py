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

# One entry per template directory — the 15 supported modalities.
_TEMPLATE_CATEGORIES = [
    TaskCategory.IMAGE_CLASSIFICATION,
    TaskCategory.KEYPOINT_DETECTION,
    TaskCategory.MASKED_LANGUAGE_MODELING,
    TaskCategory.CAUSAL_LANGUAGE_MODELING,
    TaskCategory.SEQ2SEQ,
    TaskCategory.EMBEDDINGS,
    TaskCategory.OBJECT_DETECTION,
    TaskCategory.SEMANTIC_SEGMENTATION,
    TaskCategory.TABULAR_CLASSIFICATION,
    TaskCategory.TABULAR_REGRESSION,
    TaskCategory.TEXT_CLASSIFICATION,
    TaskCategory.SENTENCE_PAIR_CLASSIFICATION,
    TaskCategory.TIME_SERIES_FORECASTING,
    TaskCategory.TIME_TO_EVENT_PREDICTION,
    TaskCategory.TOKEN_CLASSIFICATION,
]


def _client(**overrides):
    defaults = dict(
        BACKEND_TOKEN="tok",
        CLIENT_USERNAME=None,
        CLIENT_PASSWORD=None,
        EDGE_ENV="prod",
        TITLE=None,
    )
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
    # #350: content_hash is the default id strategy; the mock DB must return a
    # real salt string (a MagicMock salt poisons the hash and drops every row).
    db.get_or_create_table_salt.return_value = "0" * 64
    db.create_table.return_value = MagicMock(name="table")
    db.insert_batch.return_value = ([1, 2], [])  # ids, db_failures
    db.get_table_schema.return_value = {"a": "INT"}
    db.get_label_counts.return_value = {"cat": 2}
    db.iter_label_counts.return_value = [("cat", 2)]
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


def _run_ingest(ing, batch_size=10):
    """Run ingest with Session / validation / file-transfer patched out so
    every category — including the file-bearing ones — runs without a real
    filesystem. Captures the logged summary."""
    captured = {}
    real_log = BaseIngestor._log_summary

    def spy(self, summary):
        captured["summary"] = summary
        return real_log(self, summary)

    with patch.object(base_mod, "Session") as Sess, patch.object(
        BaseIngestor, "_log_summary", spy
    ), patch.object(ing, "validate_data", return_value=True), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest("src", batch_size=batch_size)
    return failed, captured.get("summary")


# ---------------------------------------------------------------------------
# 1. api_send_failed accounting holds for every template category
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("category", _TEMPLATE_CATEGORIES)
def test_api_send_failure_propagates_for_every_category(category):
    """send_ingest_summary failing raises out of ingest() for EVERY modality,
    so the caller exits non-zero. Covers the file-transfer branch that
    ``category=None`` skips."""
    records = [{"a": "1", "filename": "f1"}, {"a": "2", "filename": "f2"}]
    ing = make_ingestor(records=records, category=category)
    ing.api_client.send_ingest_summary.side_effect = RuntimeError("backend rejected")

    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(RuntimeError, match="backend rejected"):
            ing.ingest("src", batch_size=10)


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


def _main_node(template: Path):
    tree = ast.parse(template.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            return node
    raise AssertionError(f"{template}: no main() function")


def _swallowing_handlers(func: ast.FunctionDef):
    """Yield every exception handler inside ``func`` that could swallow an
    Exception: ``except Exception``, a tuple containing Exception, or a
    bare ``except:``."""
    for sub in ast.walk(func):
        if not isinstance(sub, ast.ExceptHandler):
            continue
        catches_exception = (
            sub.type is None  # bare except
            or (isinstance(sub.type, ast.Name) and sub.type.id == "Exception")
            or (
                isinstance(sub.type, ast.Tuple)
                and any(
                    isinstance(el, ast.Name) and el.id == "Exception"
                    for el in sub.type.elts
                )
            )
        )
        if catches_exception:
            yield sub


@pytest.mark.parametrize("template", _TEMPLATES, ids=lambda p: p.parent.name)
def test_template_except_handler_reraises(template):
    """main() must not swallow exceptions. Log-and-swallow turned any
    exception escaping ``ingest()`` — validation failure, DB error, the
    backend-registration RuntimeErrors from base.py — into exit code 0
    and a K8s Job marked Succeeded, the same silent-success class #223
    fixed for batch POST failures (#230).

    Since the duplication extraction, the templates delegate the whole
    run/report/exit contract to ``run_ingestion`` (which logs and
    re-raises — covered in tests/test_template_runner.py) and carry no
    handler of their own. This guard remains so a reintroduced
    ``except Exception`` (or bare ``except``) around the run can't bring
    the swallow back: if a handler exists, it must re-raise, and main()
    must still route through run_ingestion either way."""
    main_fn = _main_node(template)
    for handler in _swallowing_handlers(main_fn):
        has_raise = any(isinstance(stmt, ast.Raise) for stmt in ast.walk(handler))
        assert has_raise, (
            f"{template}: exception handler in main() does not re-raise "
            f"— a hard ingest failure would exit 0"
        )
    calls_run_ingestion = any(
        isinstance(sub, ast.Call)
        and isinstance(sub.func, ast.Name)
        and sub.func.id == "run_ingestion"
        for sub in ast.walk(main_fn)
    )
    assert calls_run_ingestion, (
        f"{template}: main() does not call run_ingestion — the exit "
        f"contract (sys.exit(1) on failed records, re-raise on hard "
        f"errors) would not apply"
    )


# ---------------------------------------------------------------------------
# 3. mid-batch DB failure: only the inserted records are sent to the API
# ---------------------------------------------------------------------------


def test_mid_batch_db_failure_summary_still_called():
    """3-record batch, middle record fails DB insert. send_ingest_summary is
    still called (2 records DID insert), and only the DB failure appears in
    failed — no phantom api_send_failed entries."""
    records = [{"a": str(i), "filename": f"f{i}"} for i in range(3)]
    ing = make_ingestor(records=records, category=None)
    ing.database.get_label_counts.return_value = {"a": 2}

    ing.database.iter_label_counts.return_value = [("a", 2)]

    def fake_insert(table_name, batch):
        failed_copy = {**batch[1], "updated_at": "now"}
        return [10, 12], [{"record": failed_copy, "error": "dup key"}]

    ing.database.insert_batch.side_effect = fake_insert

    failed, summary = _run_ingest(ing)

    ing.api_client.send_ingest_summary.assert_called_once()
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
    "padding padding padding padding to push the explanation well past "
    'the first hundred characters of the message."]}'
)


def test_send_ingest_summary_400_logs_status_and_full_error(caplog):
    """send_ingest_summary rejection (400) must log the full body — not a
    100-char stub — so operators can see WHY the backend rejected the call."""
    client = _client()
    with patch.object(
        client.session, "post", return_value=_resp(400, text=_DRF_400_BODY)
    ):
        with caplog.at_level(logging.ERROR, logger="tracebloc_ingestor.api.client"):
            with pytest.raises(requests.exceptions.HTTPError):
                client.send_ingest_summary(
                    table_name="tbl",
                    ingestor_id="ing",
                    labels={"cat": 1},
                    dataset_title="T",
                    data_format="image",
                    data_intent="train",
                    category=TaskCategory.IMAGE_CLASSIFICATION,
                    schema={},
                    samples=[],
                )
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "HTTP 400" in joined
    assert "well past the first hundred characters" in joined


def test_send_ingest_summary_exception_propagates_out_of_ingest():
    """send_ingest_summary is the one registration call that signals failure by
    raising. Guard that the raise escapes ingest() so the run exits non-zero."""
    records = [{"a": "1", "filename": "f1"}]
    ing = make_ingestor(records=records, category=None)
    ing.api_client.send_ingest_summary.side_effect = requests.exceptions.HTTPError(
        "HTTP 401: token expired"
    )

    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(requests.exceptions.HTTPError):
            ing.ingest("src", batch_size=10)
