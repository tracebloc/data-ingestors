"""Behavioral contract of ``run_ingestion`` — the shared run/report/exit
wrapper the template scripts delegate to.

The eleven templates used to inline this block and the copies drifted:
five swallowed exceptions and exited 0 on hard failures (#230), and the
failed-record log line was wrong in three different ways (wrapper-level
``.get("filename")`` logged None; ``.get("name")`` always logged
Unknown). The structural template tests pin that every template calls
this helper; these tests pin what the helper actually guarantees.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from tracebloc_ingestor import run_ingestion as run_ingestion_from_root
from tracebloc_ingestor.utils.template_runner import run_ingestion


def _ingestor(failed_records=None, ingest_side_effect=None):
    ing = MagicMock(name="ingestor")
    ing.__enter__ = MagicMock(return_value=ing)
    ing.__exit__ = MagicMock(return_value=False)
    if ingest_side_effect is not None:
        ing.ingest.side_effect = ingest_side_effect
    else:
        ing.ingest.return_value = failed_records or []
    return ing


_LOGGER = logging.getLogger("test_template_runner_template")


def test_exported_from_package_root():
    # Templates import it via `from tracebloc_ingestor import run_ingestion`.
    assert run_ingestion_from_root is run_ingestion


def test_clean_run_logs_success_and_returns(caplog):
    ing = _ingestor(failed_records=[])
    with caplog.at_level(logging.INFO, logger=_LOGGER.name):
        assert run_ingestion(ing, "labels.csv", batch_size=7, logger=_LOGGER) is None
    ing.ingest.assert_called_once_with("labels.csv", batch_size=7)
    assert "All records processed successfully" in caplog.text


def test_failed_records_exit_nonzero_and_log_each(caplog):
    failures = [
        {"record": {"filename": "f1", "data_id": "d1"}, "error": "api_send_failed"},
        {"record": {"data_id": "d2"}, "error": "dup key"},  # tabular: no filename
        {"record": {}, "error": "boom"},
        {"error": "no record key at all"},
    ]
    ing = _ingestor(failed_records=failures)
    with caplog.at_level(logging.WARNING, logger=_LOGGER.name):
        with pytest.raises(SystemExit) as excinfo:
            run_ingestion(ing, "labels.csv", batch_size=7, logger=_LOGGER)
    assert excinfo.value.code == 1
    assert "Failed to process 4 records" in caplog.text
    # Identifier resolution: filename where present, else data_id, else Unknown.
    assert "Failed record: f1" in caplog.text
    assert "Failed record: d2" in caplog.text
    assert "Failed record: Unknown" in caplog.text
    assert "Error details: api_send_failed" in caplog.text
    assert "Error details: dup key" in caplog.text
    # SystemExit must pass through the except-Exception handler untouched —
    # it must NOT be re-logged as a hard failure.
    assert "Ingestion failed" not in caplog.text


def test_exception_from_ingest_is_logged_and_reraised(caplog):
    err = RuntimeError("Backend rejected edge-label metadata")
    ing = _ingestor(ingest_side_effect=err)
    with caplog.at_level(logging.ERROR, logger=_LOGGER.name):
        with pytest.raises(RuntimeError) as excinfo:
            run_ingestion(ing, "labels.csv", batch_size=7, logger=_LOGGER)
    # Re-raised, not swallowed (the #230 silent-success class) — and it's
    # the ORIGINAL exception, not a replacement.
    assert excinfo.value is err
    assert "Ingestion failed: Backend rejected edge-label metadata" in caplog.text


def test_ingestor_used_as_context_manager():
    ing = _ingestor(failed_records=[])
    run_ingestion(ing, "labels.csv", batch_size=7, logger=_LOGGER)
    ing.__enter__.assert_called_once()
    ing.__exit__.assert_called_once()


def test_context_manager_exited_on_failure_paths():
    # SystemExit (failed records) and a hard error must both leave the
    # with-block normally unwound.
    ing = _ingestor(failed_records=[{"record": {}, "error": "x"}])
    with pytest.raises(SystemExit):
        run_ingestion(ing, "labels.csv", batch_size=7, logger=_LOGGER)
    ing.__exit__.assert_called_once()

    ing2 = _ingestor(ingest_side_effect=RuntimeError("db gone"))
    with pytest.raises(RuntimeError):
        run_ingestion(ing2, "labels.csv", batch_size=7, logger=_LOGGER)
    ing2.__exit__.assert_called_once()


def test_default_logger_used_when_none_passed(caplog):
    ing = _ingestor(failed_records=[])
    with caplog.at_level(
        logging.INFO, logger="tracebloc_ingestor.utils.template_runner"
    ):
        run_ingestion(ing, "labels.csv", batch_size=7)
    assert "All records processed successfully" in caplog.text
