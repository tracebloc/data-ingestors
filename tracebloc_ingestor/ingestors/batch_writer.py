"""Batch write path — DB insert + failure accounting
(structural refactor — backend#796, P5d).

Owns what happens to one batch of cleaned records: insert to MySQL and fold the
outcome (inserted / failed) into the run's ``stats`` and ``failed_records``.
Extracted from ``BaseIngestor._process_batch``. ``BaseIngestor`` composes it
via the ``_batch_writer`` property.

The per-batch API publish step has been removed: a single ``send_ingest_summary``
call is made after all records are committed (see ``BaseIngestor._ingest_with_lock``).

``session`` is threaded through unchanged for signature/behaviour parity even
though the insert goes through ``database.insert_batch`` (which manages its own
connection) — removing the dead parameter is left to a later cleanup so this
slice stays a pure relocation.
"""

import logging
from ..utils import redaction
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from ..utils.constants import RED, RESET

logger = logging.getLogger(__name__)


class BatchWriter:
    """Inserts a batch to MySQL and records the outcome. One instance per
    ingest (built by ``BaseIngestor._batch_writer``).

    The per-batch API publish step is gone: a single ``send_ingest_summary``
    call is made after all records are committed (see
    ``BaseIngestor._ingest_with_lock``).
    """

    def __init__(
        self, database: Any, api_client: Any, table_name: str, ingestor_id: str
    ):
        self.database = database
        self.api_client = api_client
        self.table_name = table_name
        self.ingestor_id = ingestor_id

    def flush(
        self,
        batch: List[Dict[str, Any]],
        session: Session,
        stats: Dict[str, int],
        failed_records: List[Dict[str, Any]],
    ) -> None:
        """Insert one batch to MySQL and fold its outcome into ``stats`` /
        ``failed_records``. Shared by the in-loop and final-batch flush sites
        in ``_ingest_with_lock``.
        """
        try:
            inserted_ids, db_failures = self._process(batch, session)
            if inserted_ids:
                stats["inserted_records"] += len(inserted_ids)
            if db_failures:
                stats["failed_records"] += len(db_failures)
                failed_records.extend(db_failures)
        except Exception as e:
            safe = redaction.safe_db_error(e)
            logger.error(f"Batch processing failed: {safe}")
            stats["failed_records"] += len(batch)
            failed_records.extend(
                {"record": record, "error": safe} for record in batch
            )

    def _process(
        self, batch: List[Dict[str, Any]], session: Session
    ) -> tuple:
        """Insert a batch of records into the database.

        Args:
            batch: List of records to insert
            session: Database session (unused — inserts commit internally)

        Returns:
            Tuple of (inserted_ids, db_failures)

        Raises:
            Exception: If batch processing fails
        """
        try:
            # The batch carries the schema-declared columns + framework columns.
            # mask_id, when the schema declares it (the semseg template), is a
            # real column and is inserted — the training client reads it from
            # MySQL to locate masks (backend#816). When NOT declared it never
            # reaches here (RecordProcessor drops it; map_file_transfer strips
            # its lend). So the former blanket ``mask_id`` pop (#212) is gone
            # with its cause; there's nothing framework-internal to drop.
            ids, db_failures = self.database.insert_batch(self.table_name, batch)
            return ids if ids else [], db_failures

        except Exception as e:
            logger.error(
                f"{RED}Error processing batch: {redaction.safe_db_error(e)}{RESET}"
            )
            # Guard the attribute chain: a non-HTTP exception (e.g. a DB
            # error) has no .response at all, and the old
            # hasattr(e.response, "text") raised AttributeError INSIDE the
            # handler — replacing the real error with "'RuntimeError'
            # object has no attribute 'response'".
            response = getattr(e, "response", None)
            if response is not None and hasattr(response, "text"):
                logger.error(f"{RED}Error response: {response.text}{RESET}")
            raise
