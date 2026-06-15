"""Console rendering for ingestion output.

The single home for human-facing presentation of an ingestion run — the ANSI
colours, the emoji, and the summary box. Business logic builds a pure
``IngestionSummary`` (data only); this renderer turns it into the terminal
output a customer sees.

Keeping presentation here, out of ``BaseIngestor``, means the engine can run
headless, the summary format can change without touching ingestion logic, and
there is exactly one place that imports the ANSI constants for this path
(structural refactor — backend#796, phase P2). Behaviour is unchanged: the
rendered output is byte-for-byte what ``BaseIngestor._log_summary`` produced.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .utils.constants import BLUE, BOLD, CYAN, GREEN, RED, RESET, YELLOW

if TYPE_CHECKING:  # import only for typing — avoids a runtime cycle, since
    # base.py imports this module at load time.
    from .ingestors.base import IngestionSummary


class ConsoleRenderer:
    """Renders ingestion results to stdout.

    The only place the summary's ANSI / emoji formatting lives. Stateless for
    now; a class (rather than a bare function) so later presentation concerns
    — a no-colour mode, rendering validation errors — have a natural home.
    """

    def render_summary(self, summary: "IngestionSummary") -> None:
        """Print the ingestion summary box (success rate, per-channel counts,
        status banner). Moved verbatim from ``BaseIngestor._log_summary`` — the
        success-rate / failure-count arithmetic stays with the rendering it
        feeds, so the customer-facing output is identical."""
        # A successful record requires DB insert AND API send AND, where
        # applicable, a successful file transfer. File-transfer failures
        # short-circuit before the DB write (they're skipped from the
        # batch), so subtracting them from total_records gives the
        # denominator's effective ceiling. Use inserted_records (the
        # actual durable outcome) as the numerator.
        success_rate = 0
        if summary.total_records > 0:
            success_rate = (summary.inserted_records / summary.total_records) * 100

        # Determine overall status color
        status_color = (
            GREEN
            if success_rate >= 90 and not summary.has_failures
            else YELLOW if success_rate >= 70 else RED
        )

        print(f"\n{CYAN}{'═'*60}{RESET}")
        print(f"{BOLD}{CYAN}📊 INGESTION SUMMARY 📊{RESET}")
        print(f"{CYAN}{'═'*60}{RESET}")
        print(
            f"{BOLD}Ingestor ID:{RESET}                {BLUE}{summary.ingestor_id}{RESET}"
        )
        # Main statistics with icons and colors
        print(
            f"{BOLD}📈 Total Records Found:{RESET}     {BLUE}{summary.total_records:,}{RESET}"
        )
        print(
            f"{BOLD}✅ Successfully Processed:{RESET}  {GREEN}{summary.processed_records:,}{RESET}"
        )
        print(
            f"{BOLD}💾 Inserted to Database:{RESET}    {GREEN}{summary.inserted_records:,}{RESET}"
        )
        print(
            f"{BOLD}🚀 Sent to API:{RESET}             {GREEN}{summary.api_sent_records:,}{RESET}"
        )
        print(
            f"{BOLD}⏭️  Skipped Records:{RESET}        {YELLOW}{summary.skipped_records:,}{RESET}"
        )
        file_transfer_color = RED if summary.file_transfer_failures > 0 else GREEN
        print(
            f"{BOLD}📁 File Transfer Failures:{RESET}  {file_transfer_color}{summary.file_transfer_failures:,}{RESET}"
        )
        print(
            f"{BOLD}❌ Failed DB Insertion:{RESET}     {RED}{summary.failed_records:,}{RESET}"
        )
        # Only count records that made it to a DB insert but didn't ship
        # to the API. Using `total_records - api_sent_records` would also
        # include file-transfer failures and DB failures (which never had
        # a chance to ship), giving an inflated, double-counted total.
        api_only_failures = max(0, summary.inserted_records - summary.api_sent_records)
        print(
            f"{BOLD}❌ Failed to Send to API:{RESET}   {RED}{api_only_failures:,}{RESET}"
        )
        print(f"{CYAN}{'─'*60}{RESET}")

        # Success rate with visual indicator
        if summary.total_records > 0:
            # Progress bar
            bar_length = 30
            filled_length = int(bar_length * success_rate / 100)
            bar = "█" * filled_length + "░" * (bar_length - filled_length)
            print(
                f"{BOLD}📊 Success Rate:{RESET} [{status_color}{bar}{RESET}] {status_color}{success_rate:.1f}%{RESET}"
            )

        # Status banner. Any non-trivial failure (DB, API, file-transfer, or a
        # record dropped during processing) disqualifies the "completed
        # successfully" message — a customer seeing 🎉 should be able to trust
        # that no record was silently dropped. The four failure channels are
        # mutually exclusive per record (a dropped record never reaches
        # file-transfer; file-transfer failures never reach DB; DB failures
        # never reach API; api_only_failures are records that hit DB but didn't
        # ship), so summing them gives a clean unique count instead of the
        # double-count `total_records - api_sent_records` would produce.
        # ``skipped_records`` MUST be included or the count contradicts the
        # severity text — a run that drops most rows printed "0 failure(s)"
        # while success_rate said "most records failed" (#234).
        total_failures = (
            summary.failed_records
            + summary.file_transfer_failures
            + summary.skipped_records
            + api_only_failures
        )
        if not summary.has_failures:
            status_msg = "🎉 Ingestion completed successfully!"
        elif success_rate >= 80:
            status_msg = (
                f"⚠️  Ingestion completed with {total_failures:,} failure(s), "
                "see logs."
            )
        elif success_rate >= 60:
            status_msg = (
                f"⚠️  Ingestion completed with {total_failures:,} failure(s); "
                "many records failed to process — see logs."
            )
        else:
            status_msg = (
                f"❌ Critical! Ingestion completed with {total_failures:,} "
                "failure(s); most records failed — see logs."
            )

        print(f"{CYAN}{'─'*60}{RESET}")
        print(f"{BOLD}{status_color}{status_msg}{RESET}")
        print(f"{CYAN}{'═'*60}{RESET}\n")
