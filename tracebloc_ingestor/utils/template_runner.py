"""Shared run-and-report wrapper for the template scripts.

Every ``templates/<category>/`` script used to inline the same ~20-line
block: run ``ingest()``, log each failed record, ``sys.exit(1)`` on
failures, re-raise hard errors. Eleven hand-maintained copies drifted —
five templates swallowed exceptions and exited 0 on hard failures until
#230 — so the contract now lives here once and the templates just call
:func:`run_ingestion`.
"""

import logging
import sys
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import cycle guard, typing only
    from ..ingestors.base import BaseIngestor

# Underscored to stay distinct from run_ingestion's `logger` parameter.
_logger = logging.getLogger(__name__)

__all__ = ["run_ingestion"]


def run_ingestion(
    ingestor: "BaseIngestor",
    source: Any,
    batch_size: int,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Run ``ingestor.ingest(source)`` and enforce the template exit
    contract:

    - failed records -> log each one, then ``sys.exit(1)`` so the K8s Job
      is marked failed instead of reporting silent success
    - an exception escaping ``ingest()`` (validation error, DB error,
      backend registration rejection) -> log and re-raise, so the process
      exits non-zero. Swallowing here is what let five templates end a
      hard failure with exit code 0 and a Job marked Succeeded (#230).
    - clean run -> success log, normal return

    Args:
        ingestor: The constructed ingestor; used as a context manager,
            mirroring the templates' previous ``with ingestor:`` usage.
        source: Passed through to ``ingest()`` (the labels CSV / JSON path).
        batch_size: Passed through to ``ingest()``.
        logger: The template's logger, so log lines keep the template's
            logger name. Falls back to this module's logger.
    """
    log = logger if logger is not None else _logger
    try:
        with ingestor:
            failed_records = ingestor.ingest(source, batch_size=batch_size)
            if failed_records:
                log.warning(f"Failed to process {len(failed_records)} records")
                for failure in failed_records:
                    # ingest() returns {"record": <processed record>,
                    # "error": <reason>} entries. Identify the record by
                    # filename where the category has one (file-bearing
                    # categories), else by data_id (tabular family) — the
                    # old per-template copies got this wrong in three
                    # different ways (wrapper-level .get("filename") logged
                    # None; .get("name") always logged Unknown).
                    record = failure.get("record") or {}
                    identifier = (
                        record.get("filename") or record.get("data_id") or "Unknown"
                    )
                    log.warning(f"Failed record: {identifier}")
                    log.warning(
                        f"Error details: {failure.get('error', 'Unknown error')}"
                    )
                # Failed records (file transfer, DB insert, API send, or
                # processing) must fail the run — exit non-zero so the K8s
                # Job is marked failed instead of reporting silent success
                # (SystemExit bypasses the except Exception below).
                sys.exit(1)
            log.info("All records processed successfully")
    except Exception as e:
        # Re-raise so the process exits non-zero — swallowing here let a
        # hard failure (validation error, DB error, backend registration
        # rejection raised by ingest()) end with exit code 0 and a K8s
        # Job marked Succeeded (#230).
        log.error(f"Ingestion failed: {str(e)}")
        raise
