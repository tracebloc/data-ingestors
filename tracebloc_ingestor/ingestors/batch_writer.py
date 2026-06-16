"""Batch write path — DB insert + API publish + failure accounting
(structural refactor — backend#796, P5d).

Owns what happens to one batch of cleaned records: insert to MySQL, publish the
inserted rows to the backend, and fold the outcome (inserted / api-sent /
failed) into the run's ``stats`` and ``failed_records``. Extracted verbatim from ``BaseIngestor._flush_batch`` /
``_process_batch`` — the subtle mid-batch-DB-failure accounting (match by
``data_id``, not position) and the #99 / api_send_failed surfacing are
byte-for-byte unchanged. ``BaseIngestor`` composes it via the ``_batch_writer``
property; the attribute names match the ingestor's so the bodies are identical.

``session`` is threaded through unchanged for signature/behaviour parity even
though the insert goes through ``database.insert_batch`` (which manages its own
connection) — removing the dead parameter is left to a later cleanup so this
slice stays a pure relocation.
"""

import logging
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from ..utils.constants import RED, RESET

logger = logging.getLogger(__name__)


class BatchWriter:
    """Inserts a batch to MySQL, publishes it to the backend, and records the
    outcome. One instance per ingest (built by ``BaseIngestor._batch_writer``)."""

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
        """Process one batch and fold its outcome into ``stats`` /
        ``failed_records``. Shared by the in-loop and final-batch flush
        sites in ``_ingest_with_lock``.

        Failure accounting is the point of this helper. A run where every
        batch POST was rejected with HTTP 400 used to finish with
        "All records processed successfully" and exit 0 — the rows were
        in MySQL but the backend had zero records, and the next platform
        call failed with "No data found for table name". Two swallow
        points caused that:

        - ``api_success=False`` only skipped the ``api_sent_records``
          increment; the records never reached ``failed_records``, so
          ``ingest()`` returned ``[]`` and the caller exited 0. Now each
          inserted-but-unsent record is returned as a failed record with
          ``error="api_send_failed"`` (the rows stay committed — they're
          in MySQL but invisible to the platform until re-sent).
        - an exception from ``_process`` was logged and dropped,
          leaving the whole batch out of every counter. Now the batch is
          counted and returned as failed.

        The summary needs no extra field: "Failed to Send to API" is
        derived from ``inserted_records - api_sent_records``, and
        ``IngestionSummary.has_failures`` already trips on that gap.
        """
        try:
            inserted_ids, api_success, db_failures = self._process(batch, session)
            # Only count records that were successfully inserted
            if inserted_ids:
                stats["inserted_records"] += len(inserted_ids)
                if api_success:
                    stats["api_sent_records"] += len(inserted_ids)
                else:
                    # The inserted-but-unsent records are the batch minus
                    # the DB failures. Don't assume they're the first
                    # len(ids) entries: insert_batch's per-record fallback
                    # appends successes in scan order, so a mid-batch DB
                    # failure shifts which records were inserted. Failure
                    # entries carry a *copy* of the record (processed_record
                    # adds updated_at), so match by data_id — set on every
                    # processed record by _map_unique_id — not by identity.
                    db_failed_data_ids = {
                        f.get("record", {}).get("data_id") for f in db_failures
                    }
                    failed_records.extend(
                        {"record": record, "error": "api_send_failed"}
                        for record in batch
                        if record.get("data_id") not in db_failed_data_ids
                    )
            if db_failures:
                stats["failed_records"] += len(db_failures)
                failed_records.extend(db_failures)
        except Exception as e:
            logger.error(f"Batch processing failed: {str(e)}")
            stats["failed_records"] += len(batch)
            failed_records.extend(
                {"record": record, "error": str(e)} for record in batch
            )

    def _process(self, batch: List[Dict[str, Any]], session: Session) -> List[int]:
        """
        Process and insert a batch of records

        Args:
            batch: List of records to process
            session: Database session

        Returns:
            List of record IDs

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
            # Insert batch and get IDs
            ids, db_failures = self.database.insert_batch(self.table_name, batch)
            api_success = False
            # Send to API with ingestor_id
            if ids:  # Only send to API if we have valid IDs
                # Send only the records that actually inserted.
                # ``zip(ids, batch)`` pairs positionally and truncates to
                # ``len(ids)``; after a MID-batch DB failure (insert_batch's
                # per-record fallback appends successes in scan order) that
                # would send the DB-failed record to the API and drop the
                # last inserted one — a phantom backend record pointing at
                # no MySQL row, plus a committed row the platform never
                # sees. Match by data_id, same as flush.
                if db_failures:
                    db_failed_data_ids = {
                        f.get("record", {}).get("data_id") for f in db_failures
                    }
                    records_to_send = [
                        record
                        for record in batch
                        if record.get("data_id") not in db_failed_data_ids
                    ]
                else:
                    records_to_send = batch
                api_success = self.api_client.send_batch(
                    [(id, record) for id, record in zip(ids, records_to_send)],
                    self.table_name,
                    ingestor_id=self.ingestor_id,  # Include ingestor_id in API requests
                )
            return (
                ids if ids else [],
                api_success,
                db_failures,
            )  # Ensure we always return a list

        except Exception as e:
            logger.error(f"{RED}Error processing batch: {str(e)}{RESET}")
            # Guard the attribute chain: a non-HTTP exception (e.g. a DB
            # error) has no .response at all, and the old
            # hasattr(e.response, "text") raised AttributeError INSIDE the
            # handler — replacing the real error with "'RuntimeError'
            # object has no attribute 'response'".
            response = getattr(e, "response", None)
            if response is not None and hasattr(response, "text"):
                logger.error(f"{RED}Error response: {response.text}{RESET}")
            raise
