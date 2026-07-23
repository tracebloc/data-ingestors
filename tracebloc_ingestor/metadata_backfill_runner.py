"""Pre-cutover metadata backfill runner (backend#1166 / #1198 — the send side
of #378).

Ties the backfill pieces together into a one-time, per-client sweep of datasets
ingested BEFORE the federated-alignment cutover. For each dataset it:

  1. **GETs the backend record** by ``ingestor_id``
     (:meth:`APIClient.get_dataset_metadata`). The authoritative ``category`` and
     ``data_format`` drive the recompute and are not recoverable from the table
     alone, so they are read from the backend first.
  2. **Recomputes** the enriched ``{schema, meta_data}`` from the persisted rows
     via :func:`build_dataset_metadata` — SQL aggregates, no row re-ingest —
     keyed on that category.
  3. **POSTs** it back (:meth:`APIClient.send_metadata_backfill`), which upserts
     ``GlobalMetaData`` and re-folds any competition built from the table.

The set of datasets to sweep is the client's REGISTERED ingest journal
(:meth:`Database.list_registered_runs`); explicit ``ingestor_id``\\ s can be
passed instead.

**Rollout, not always-on.** New ingests already emit the new-shape metadata
(#361), so this runs once per client during rollout. It is idempotent: a dataset
already carrying new-shape metadata is skipped, and the backend upsert is itself
a safe overwrite, so a re-run converges.

**Scope.** Only plain/source datasets are recomputed. Competition/merged
datasets have no client-side raw table to read — their fold is rebuilt
backend-side by the re-fold this POST triggers — so they are skipped here
(detected via the backend record's ``is_competition`` / ``source_dataset_ids``).
``label_column`` and the uploader-declared scalar attributes are not persisted
backend-side, so they are not passed; the SQL-derivable enriched schema +
``feature_stats`` (the backfill's primary value) are recomputed regardless. See
:func:`build_dataset_metadata` for the per-fact limitations.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .api.client import APIClient
from .config import Config
from .database import Database
from .metadata_backfill import build_dataset_metadata
from .utils.logging import setup_logging

logger = logging.getLogger(__name__)


# Outcome statuses for a single dataset's backfill attempt.
STATUS_OK = "ok"
STATUS_NOT_FOUND = "not_found"
STATUS_SKIPPED_COMPETITION = "skipped_competition"
STATUS_SKIPPED_CURRENT = "skipped_current"
STATUS_ERROR = "error"


@dataclass
class BackfillResult:
    """The outcome of backfilling one dataset (see the ``STATUS_*`` constants)."""

    ingestor_id: str
    table_name: Optional[str] = None
    status: str = STATUS_OK
    created: Optional[bool] = None
    competitions_refolded: Optional[int] = None
    error: Optional[str] = None


def _already_new_shape(record: Dict[str, Any]) -> bool:
    """Whether the backend record already carries new-shape metadata, so the
    recompute + POST can be skipped.

    Marker: a non-empty ``meta_data.attributes`` — the typed alignment contract
    that only the post-cutover ingest (#361) / a prior backfill writes. A
    pre-cutover row has no ``attributes``, so it is not skipped. Conservative: if
    a category legitimately has empty attributes we simply re-POST (idempotent).
    """
    meta_data = record.get("meta_data") or {}
    return bool(meta_data.get("attributes"))


def backfill_dataset(
    database: Database,
    api_client: APIClient,
    ingestor_id: str,
    *,
    skip_if_current: bool = True,
) -> BackfillResult:
    """GET → recompute → POST for a single dataset. Never raises for an expected
    skip/not-found; those are returned as a :class:`BackfillResult` status.
    Unexpected failures (SQL / network) propagate to the caller.
    """
    record = api_client.get_dataset_metadata(ingestor_id)
    if record is None:
        return BackfillResult(ingestor_id, status=STATUS_NOT_FOUND)

    table_name = record.get("table_name")

    # Competition/merged datasets have no client-side raw table for the SQL
    # recompute; their reconciled fold is rebuilt backend-side (the re-fold this
    # backfill triggers on source POSTs), so never recompute them here.
    if record.get("is_competition") or record.get("source_dataset_ids"):
        return BackfillResult(
            ingestor_id, table_name, status=STATUS_SKIPPED_COMPETITION
        )

    if skip_if_current and _already_new_shape(record):
        return BackfillResult(ingestor_id, table_name, status=STATUS_SKIPPED_CURRENT)

    category = record.get("category")
    if not category or not table_name:
        return BackfillResult(
            ingestor_id,
            table_name,
            status=STATUS_ERROR,
            error="backend record missing category/table_name",
        )

    payload = build_dataset_metadata(
        database,
        table_name,
        category=category,
        data_format=record.get("data_format"),
    )
    result = api_client.send_metadata_backfill(
        table_name, payload["schema"], payload.get("meta_data")
    )
    return BackfillResult(
        ingestor_id,
        table_name,
        status=STATUS_OK,
        created=result.get("created"),
        competitions_refolded=result.get("competitions_refolded"),
    )


def backfill_datasets(
    database: Database,
    api_client: APIClient,
    ingestor_ids: Optional[List[str]] = None,
    *,
    skip_if_current: bool = True,
) -> List[BackfillResult]:
    """Backfill every dataset in ``ingestor_ids``, or — when ``None`` — every
    REGISTERED run in this client's ingest journal.

    Per-dataset guarded: an unexpected failure on one dataset is logged and
    recorded as an ``error`` result, and the sweep continues to the next (a
    single bad table can't abort the rollout). Returns one
    :class:`BackfillResult` per dataset.
    """
    if ingestor_ids is None:
        ingestor_ids = [run["ingestor_id"] for run in database.list_registered_runs()]

    logger.info("Metadata backfill: %d dataset(s) to process.", len(ingestor_ids))

    results: List[BackfillResult] = []
    for ingestor_id in ingestor_ids:
        try:
            result = backfill_dataset(
                database, api_client, ingestor_id, skip_if_current=skip_if_current
            )
        except Exception as exc:  # noqa: BLE001 — one bad table must not abort the sweep
            # Record/log the exception TYPE only — never str(exc) or a traceback.
            # A SQL driver error or backend response body can embed customer cell
            # values (e.g. categorical vocab), and this runs in a Helm-hook Job
            # whose output lands in install logs. (bugbot)
            error_type = type(exc).__name__
            logger.error(
                "Metadata backfill failed for ingestor_id=%s (%s)",
                ingestor_id,
                error_type,
            )
            result = BackfillResult(
                ingestor_id, status=STATUS_ERROR, error=error_type
            )
        results.append(result)
        _log_result(result)

    _log_summary(results)
    return results


def _log_result(result: BackfillResult) -> None:
    if result.status == STATUS_OK:
        logger.info(
            "  ✓ %s (%s): created=%s, competitions_refolded=%s",
            result.table_name,
            result.ingestor_id,
            result.created,
            result.competitions_refolded,
        )
    elif result.status == STATUS_ERROR:
        logger.error(
            "  ✗ %s (%s): %s", result.table_name, result.ingestor_id, result.error
        )
    else:
        logger.info(
            "  – %s (%s): %s", result.table_name, result.ingestor_id, result.status
        )


def _log_summary(results: List[BackfillResult]) -> None:
    counts: Dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    logger.info(
        "Metadata backfill complete: %s",
        ", ".join(f"{status}={count}" for status, count in sorted(counts.items()))
        or "nothing to do",
    )


def main(argv: Optional[List[str]] = None) -> int:  # pragma: no cover - thin shell
    """Console-script entrypoint (``tracebloc-backfill``).

    Reads DB + backend credentials from the environment via :class:`Config` (the
    same env the ingestor uses — ``MYSQL_HOST``, ``BACKEND_TOKEN``, ``EDGE_ENV``,
    …); no ``ingest.yaml`` is needed. Returns a non-zero exit code iff any
    dataset errored, so a rollout job surfaces failures.
    """
    config = Config()
    setup_logging(config)

    database = Database(config)
    api_client = APIClient(config)  # triggers config.validate()

    results = backfill_datasets(database, api_client)
    errored = sum(1 for result in results if result.status == STATUS_ERROR)
    return 1 if errored else 0
