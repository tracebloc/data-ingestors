"""Regression tests for issue #99 — the silent file_transfer failure
pattern.

Before the fix, ``image_transfer`` / ``annotation_transfer`` /
``text_transfer`` returned the record unchanged when their source file
was missing, so ``map_file_transfer`` returned a truthy record and
``BaseIngestor.ingest`` happily wrote the row to the DB and pushed it to
the API. The summary's "Inserted to Database" / "Sent to API" counters
both incremented, and the customer-facing banner reported
``🎉 Ingestion completed successfully!`` with 100% success — even though
zero image files reached the destination directory.

These tests pin three things:

1. The single-file transfer functions return ``None`` on missing source.
2. ``BaseIngestor.ingest`` increments ``file_transfer_failures`` (NOT
   ``skipped_records``) for those records and surfaces them in the
   ``failed_records`` list it returns to the CLI.
3. ``IngestionSummary.has_failures`` is True whenever any non-trivial
   failure occurred, so the "completed successfully" banner is gated
   correctly.
"""

from __future__ import annotations

import io
import logging
import os
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

import pytest

from tracebloc_ingestor import file_transfer
from tracebloc_ingestor.ingestors.base import BaseIngestor, IngestionSummary
from tracebloc_ingestor.utils.constants import TaskCategory


# ---------------------------------------------------------------------------
# Unit: single-file transfer functions return None on missing source
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_dirs(tmp_path, monkeypatch):
    """Point file_transfer's module-level Config at empty tmp dirs so any
    transfer attempt resolves to a non-existent source.

    Config exposes SRC_PATH as a lazy env-backed property and DEST_PATH
    as a derived property (``STORAGE_PATH / TABLE_NAME``), so the
    supported override path is env vars + a STORAGE_PATH override on
    the module-level Config instance."""
    src = tmp_path / "src"
    storage = tmp_path / "storage"
    src.mkdir()
    storage.mkdir()
    monkeypatch.setenv("SRC_PATH", str(src))
    monkeypatch.setenv("TABLE_NAME", "test_table")
    # STORAGE_PATH is a plain class attr (not env-driven), so override
    # the live module-level Config snapshot directly.
    monkeypatch.setattr(file_transfer.config, "STORAGE_PATH", str(storage))
    return src, storage


def test_image_transfer_returns_none_when_source_missing(isolated_dirs, caplog):
    """Pre-fix: this returned the record (truthy), letting the DB write
    proceed despite zero bytes landing on disk. Now: returns None and
    the caller in BaseIngestor.ingest treats it as a file-transfer skip."""
    record = {"filename": "does-not-exist", "data_id": "abc"}
    with caplog.at_level(logging.ERROR):
        result = file_transfer.image_transfer(record, {"extension": ".jpeg"})
    assert result is None
    assert any("Source image not found" in r.message for r in caplog.records)


def test_text_transfer_returns_none_when_source_missing(isolated_dirs, caplog):
    record = {"filename": "missing-doc", "data_id": "abc"}
    with caplog.at_level(logging.ERROR):
        result = file_transfer.text_transfer(record, {"extension": ".txt"})
    assert result is None
    assert any("Source text file not found" in r.message for r in caplog.records)


def test_annotation_transfer_returns_none_when_source_missing(isolated_dirs, caplog):
    record = {"filename": "missing", "data_id": "abc"}
    with caplog.at_level(logging.ERROR):
        result = file_transfer.annotation_transfer(record, {}, ".xml")
    assert result is None
    assert any("Source file not found" in r.message for r in caplog.records)


def test_image_transfer_returns_none_when_filename_missing(isolated_dirs):
    """A record arriving without a filename can never have its file
    transferred — return None so the caller drops it rather than writing
    a half-record to the DB."""
    assert file_transfer.image_transfer({}, {"extension": ".jpeg"}) is None


# ---------------------------------------------------------------------------
# Unit: IngestionSummary.has_failures + banner gating
# ---------------------------------------------------------------------------


def _summary(**overrides) -> IngestionSummary:
    base = dict(
        ingestor_id="t",
        total_records=10,
        processed_records=10,
        inserted_records=10,
        api_sent_records=10,
        failed_records=0,
        skipped_records=0,
        file_transfer_failures=0,
    )
    base.update(overrides)
    return IngestionSummary(**base)


def test_has_failures_false_on_clean_run():
    assert _summary().has_failures is False


def test_has_failures_true_when_file_transfer_failed():
    """The headline regression: even if DB+API both reported all 10
    records as successful, a single file-transfer failure must flip
    has_failures so the banner can't say 'completed successfully'."""
    s = _summary(file_transfer_failures=1, total_records=11, processed_records=10)
    assert s.has_failures is True


def test_has_failures_true_when_inserted_less_than_total():
    assert _summary(inserted_records=9).has_failures is True


def test_has_failures_true_when_api_short_of_inserted():
    assert _summary(api_sent_records=9).has_failures is True


def test_has_failures_true_when_db_failed():
    assert _summary(failed_records=1).has_failures is True


# ---------------------------------------------------------------------------
# Behavior: _log_summary swaps the banner when failures are present
# ---------------------------------------------------------------------------


def _capture_summary(summary: IngestionSummary) -> str:
    # _log_summary only reads `summary`, so we can call it on a bare
    # MagicMock that satisfies the bound-method signature.
    ingestor = MagicMock(spec=BaseIngestor)
    buf = io.StringIO()
    with redirect_stdout(buf):
        BaseIngestor._log_summary(ingestor, summary)
    return buf.getvalue()


def test_banner_drops_celebration_on_file_transfer_failure():
    s = _summary(
        total_records=576,
        processed_records=576,
        inserted_records=0,
        api_sent_records=0,
        file_transfer_failures=576,
    )
    out = _capture_summary(s)
    assert "🎉 Ingestion completed successfully!" not in out
    assert "completed with" in out
    assert "576" in out  # the failure count appears in the banner


def test_banner_total_does_not_double_count_file_transfer(capsys):
    """Bugbot caught this: when every record fails file_transfer, the
    failure count must be 576, NOT 1152. The bug was summing
    file_transfer_failures alongside (total_records - api_sent_records),
    but file-transfer failures never reach the API so they were counted
    in both terms.

    The failure channels are mutually exclusive per record:
        file_transfer_failures  — never reached DB
        failed_records          — DB failures; never reached API
        api_only_failures       — inserted to DB but didn't ship
    """
    s = _summary(
        total_records=576,
        processed_records=576,
        inserted_records=0,
        api_sent_records=0,
        file_transfer_failures=576,
    )
    out = _capture_summary(s)

    # The "Failed to Send to API" line previously read 576 here too —
    # double-counting the same records. Should be 0: nothing was
    # inserted, so nothing was eligible for the API ship step.
    assert "❌ Failed to Send to API:" in out
    api_line = next(
        line for line in out.splitlines() if "Failed to Send to API" in line
    )
    # ANSI color codes wrap the count, so check the digits don't include 576.
    assert "576" not in api_line
    assert ">0<" in api_line.replace("\x1b[91m", ">").replace("\x1b[0m", "<")

    # The banner total appears only once and equals the unique failures.
    assert "1,152" not in out
    assert "1152" not in out


def test_banner_total_correct_with_mixed_failure_modes():
    """Sanity check: the three failure channels add up cleanly when all
    three are non-zero."""
    s = _summary(
        total_records=100,
        processed_records=95,  # 5 generic skips (not failures)
        inserted_records=80,   # 15 DB failures
        api_sent_records=70,   # 10 API-only failures
        failed_records=15,
        file_transfer_failures=5,
    )
    # Expected unique failure count: 5 (file) + 15 (DB) + 10 (API) = 30.
    out = _capture_summary(s)
    assert "30 failure" in out


def test_banner_shows_file_transfer_failures_line():
    s = _summary(file_transfer_failures=3, total_records=13, processed_records=10)
    out = _capture_summary(s)
    assert "File Transfer Failures" in out
    assert "3" in out


def test_banner_celebrates_only_on_truly_clean_run():
    out = _capture_summary(_summary())
    assert "🎉 Ingestion completed successfully!" in out


# ---------------------------------------------------------------------------
# Integration-ish: BaseIngestor.ingest plumbs file_transfer failures
# through to the caller (so cli.run.main exits non-zero) and the summary
# attributes them to the new counter.
# ---------------------------------------------------------------------------


class _Recorder:
    """Captures the IngestionSummary that _log_summary is called with.

    Installed via ``monkeypatch.setattr(BaseIngestor, "_log_summary", ...)``,
    which stores the _Recorder instance as a class attribute. Because
    _Recorder doesn't implement ``__get__``, instance access returns the
    recorder itself (no descriptor binding), so ``self._log_summary(s)``
    inside ``ingest()`` reduces to ``recorder(s)`` — hence the single
    ``summary`` argument on ``__call__``.
    """

    def __init__(self):
        self.summary: IngestionSummary | None = None

    def __call__(self, summary):
        self.summary = summary


def test_ingest_counts_file_transfer_failures_separately(monkeypatch):
    """End-to-end mocked: image_transfer is forced to return None on every
    record. After the fix, the ingestor should:

      * NOT increment ``inserted_records`` / ``api_sent_records`` for the
        failed records,
      * increment ``file_transfer_failures`` (NOT ``skipped_records``),
      * append the failures to the returned list so cli.run.main exits 1,
      * tick the tqdm progress bar for each failed record so an
        all-transfer-failure run doesn't leave the bar stuck at 0/N
        (bugbot regression — the `continue` previously skipped
        ``pbar.update``).
    """
    records = [{"filename": f"row{i}", "extension": ".jpeg"} for i in range(5)]

    # Force every map_file_transfer call to fail.
    monkeypatch.setattr(
        "tracebloc_ingestor.ingestors.base.map_file_transfer",
        lambda category, record, options: None,
    )

    # Replace tqdm with a stub that records update() calls so we can
    # assert the progress bar actually advances.
    pbar_updates: list[int] = []

    class _FakePbar:
        def __init__(self, *args, **kwargs):
            pass

        def update(self, n):
            pbar_updates.append(n)

        def close(self):
            pass

    monkeypatch.setattr("tracebloc_ingestor.ingestors.base.tqdm", _FakePbar)

    captured = _Recorder()
    monkeypatch.setattr(BaseIngestor, "_log_summary", captured)

    # Patch out the API meta calls so we reach the summary block.
    mock_api = MagicMock()
    mock_api.send_generate_edge_label_meta.return_value = True
    mock_api.send_global_meta_meta.return_value = True
    mock_api.prepare_dataset.return_value = True

    mock_db = MagicMock()
    mock_db.create_table.return_value = MagicMock()
    mock_db.get_table_schema.return_value = {}
    mock_db.insert_batch.return_value = ([], [])

    with patch.object(BaseIngestor, "__abstractmethods__", set()):
        ingestor = BaseIngestor(
            database=mock_db,
            api_client=mock_api,
            table_name="t",
            schema={"filename": "VARCHAR(255)"},
            category=TaskCategory.IMAGE_CLASSIFICATION,
            intent="train",  # required for process_record to succeed
        )
        ingestor.read_data = lambda source: iter(records)
        ingestor._count_records = lambda source: len(records)
        ingestor.validate_data = lambda source: True

        failed = ingestor.ingest("fake-source", batch_size=10)

    assert captured.summary is not None, "summary was never logged"
    assert captured.summary.file_transfer_failures == 5
    assert captured.summary.skipped_records == 0  # NOT counted as generic skip
    assert captured.summary.inserted_records == 0
    assert captured.summary.api_sent_records == 0
    assert captured.summary.has_failures is True

    # The CLI exit-code path: cli.run.main returns 1 when this list is
    # non-empty. Without the fix, a 100%-failed run returned [] and the
    # K8s job marker was Succeeded.
    assert len(failed) == 5
    assert all(f["error"] == "file_transfer_failed" for f in failed)

    # Bugbot regression: the file-transfer skip branch must advance the
    # progress bar. Sum of ticks must reach total_records (5).
    assert sum(pbar_updates) == 5
