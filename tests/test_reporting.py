"""Tests for ConsoleRenderer — the extracted ingestion-summary presentation.

Pulling the summary box out of ``BaseIngestor._log_summary`` into
``reporting.ConsoleRenderer`` (structural refactor — backend#796, P2) means
the presentation is now testable in isolation, without a database or a full
ingest. These lock the rendered output's contract.
"""

import contextlib
import io

import pytest

from tracebloc_ingestor.ingestors.base import IngestionSummary
from tracebloc_ingestor.reporting import ConsoleRenderer


def _render(summary):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        ConsoleRenderer().render_summary(summary)
    return buf.getvalue()


def test_clean_run_renders_success_banner():
    out = _render(IngestionSummary("ing-1", 10, 10, 10, 10, 0, 0, 0))
    assert "📊 INGESTION SUMMARY" in out
    assert "ing-1" in out
    assert "100.0%" in out
    assert "🎉 Ingestion completed successfully!" in out


def test_dropped_records_disqualify_success_and_are_counted():
    # 4 of 10 dropped during processing (skipped_records). Per #234 the banner
    # must NOT claim success, and the failure count must include the drops.
    out = _render(IngestionSummary("ing-2", 10, 6, 6, 6, 0, 4, 0))
    assert "🎉" not in out
    assert "with 4 failure(s)" in out


def test_file_transfer_failures_disqualify_success():
    out = _render(IngestionSummary("ing-3", 10, 10, 10, 10, 0, 0, 2))
    assert "🎉" not in out
    assert "with 2 failure(s)" in out


def test_api_send_gap_counted_without_double_counting():
    # 10 inserted, only 7 shipped to the API -> 3 api-only failures, and no
    # other channel failed, so the total is exactly 3 (not double-counted
    # against total_records).
    out = _render(IngestionSummary("ing-4", 10, 10, 10, 7, 0, 0, 0))
    assert "🎉" not in out
    assert "with 3 failure(s)" in out


@pytest.mark.parametrize(
    "summary,needle",
    [
        # success_rate >= 80 -> mild banner
        (IngestionSummary("a", 10, 9, 9, 9, 1, 0, 0), "see logs."),
        # 60 <= rate < 80 -> "many records failed"
        (IngestionSummary("b", 10, 7, 7, 7, 3, 0, 0), "many records failed"),
        # rate < 60 -> "Critical"
        (IngestionSummary("c", 10, 3, 3, 3, 7, 0, 0), "❌ Critical!"),
    ],
)
def test_severity_thresholds(summary, needle):
    assert needle in _render(summary)


def test_zero_total_records_does_not_divide_by_zero():
    # An empty source must render without raising (no success-rate bar).
    out = _render(IngestionSummary("empty", 0, 0, 0, 0, 0, 0, 0))
    assert "📊 INGESTION SUMMARY" in out


# ---------------------------------------------------------------------------
#  backend#2895 — the banner must name the table that was actually written.
#
#  Under PER_INGESTION_TABLES the row store is `ds_<uuid4().hex>` while the
#  run's `table_name` stays the user-facing LABEL. This banner is the ONLY
#  channel the CLI parses, and it carried the label — so `tracebloc data
#  ingest --name X` reported `"table": "X"` for a table that does not exist,
#  and `data delete X` then failed on a dataset that had just been created.
#  Both signals agreed and both were wrong, which is what made it silent.
# ---------------------------------------------------------------------------


def test_destination_table_is_rendered_when_it_differs_from_the_label():
    handle = "ds_" + "b" * 32
    out = _render(IngestionSummary("ing-5", 10, 10, 10, 10, 0, 0, 0, handle))
    assert "Destination table:" in out
    # The HANDLE, not the label — this is the whole finding.
    assert handle in out


def test_destination_table_line_is_absent_when_unset():
    # Flag-off / legacy callers construct the tuple without the field. The
    # banner must stay byte-identical for them, so the line is omitted rather
    # than printed empty — an empty "Destination table:" would parse as a real
    # (blank) answer on the CLI side, which is worse than no line at all.
    out = _render(IngestionSummary("ing-6", 10, 10, 10, 10, 0, 0, 0))
    assert "Destination table:" not in out
    assert "ing-6" in out


def test_legacy_positional_construction_still_works():
    # The field is trailing + defaulted precisely so every existing call site
    # stays valid. If this breaks, the change is not additive.
    s = IngestionSummary("ing-7", 5, 5, 5, 5, 0, 0, 0)
    assert s.destination_table == ""
