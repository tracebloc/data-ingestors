"""Metadata backfill for pre-cutover datasets (di#360 / backend#1037).

Recompute the enriched ``schema`` + ``feature_stats`` metadata payload for an
ALREADY-INGESTED table **without re-ingesting rows**. Numeric sufficient
statistics come from SQL aggregates (``COUNT`` / ``SUM`` / ``SUM(x*x)`` / ``MIN``
/ ``MAX``); categorical vocabularies from a bounded ``GROUP BY``.

Why: datasets ingested before the federated-alignment cutover have no per-column
``feature_stats`` and no enriched schema on the global-metadata channel, so the
backend#1037 combine-time checks can't reconcile them. This rebuilds the exact
``{schema, meta_data}`` shape a live ingest ships — it reuses
``CSVIngestor._schema_payload`` / ``_collect_run_metadata`` rather than
reimplementing the shapes, so a backfilled dataset's metadata is
indistinguishable from a freshly-ingested one (guarded by a parity test).

The API send is deliberately **not** done here: :func:`build_dataset_metadata`
returns the payload; a caller wires it to the backend metadata endpoint
separately (that surface is intentionally out of scope for now).

Limitations (v1), all WARN-only alignment facts rather than block-causing ones:

* The time-series ``timezone`` / ``sampling_frequency`` facts are not recomputed
  (they need the ordered timestamp series, not a scalar aggregate).
* The uploader-declared scalar attributes (``color_mode``, ``bit_depth``,
  ``language``, ``normalization``, ``time_unit``, ``event_indicator``,
  ``positive_definition``) live only in the original ingest config, not in the
  table. Pass them through ``file_options`` when the caller has them (e.g. from
  the stored dataset config); otherwise they are omitted.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any, Dict, Optional

from sqlalchemy import Float, MetaData, Table, cast, func, select

from .cli.conventions import REGRESSION_CLASS_CATEGORIES
from .database import Database
from .ingestors.csv_ingestor import _MAX_CATEGORICAL_CARDINALITY, CSVIngestor
from .modalities.registry import spec_for
from .schema_inference import canonical_dtype

logger = logging.getLogger(__name__)

# Framework/bookkeeping columns every dataset table carries
# (``Database.create_table``). They are never dataset features, so they never
# contribute ``feature_stats`` or categorical vocab. ``label`` is handled
# separately: it holds the prediction target, whose numeric stats ARE wanted for
# regression-class tasks (emitted under the ``label`` key, matching the live
# ingest's target re-key and the enriched schema's ``role: "target"``).
_FRAMEWORK_COLUMNS = frozenset(
    {
        "id",
        "created_at",
        "updated_at",
        "status",
        "data_intent",
        "data_id",
        "filename",
        "extension",
        "annotation",
        "ingestor_id",
    }
)

# The framework target column (mirrors ``ingestors.base._TARGET_COLUMN``).
_TARGET_COLUMN = "label"

# Canonical dtypes the live ingest accumulates numeric feature_stats for — the
# INT / FLOAT cast branches. ``bool`` / ``datetime`` / ``string`` are not folded.
_NUMERIC_DTYPES = frozenset({"int", "float"})

# The distinct-value cap for a categorical column (past it: free text / an id,
# not a feature) is imported from CSVIngestor so the two can't drift.


def build_dataset_metadata(
    database: Database,
    table_name: str,
    *,
    category: str,
    label_column: Optional[str] = None,
    data_format: Optional[str] = None,
    file_options: Optional[Dict[str, Any]] = None,
    api_client: Any = None,
) -> Dict[str, Any]:
    """Return the ``{"schema", "meta_data"}`` metadata payload for an
    already-ingested table, recomputed from the persisted rows via SQL.

    Args:
        database: A connected :class:`Database` for the client's MySQL.
        table_name: The dataset table to backfill.
        category: The dataset's task category (drives the target/role handling
            and the emitted scalar attributes). Required — it is not derivable
            from the table.
        label_column: The manifest label column name, if any. Used to mark the
            enriched schema's ``label`` column ``role: "target"`` and to keep the
            regression target's numeric stats (which live in the physical
            ``label`` column). Pass the value from the stored dataset config.
        data_format: The dataset's data format; defaults to the category's
            registry default. Governs the base scalar attributes (image
            ``resolution``, text ``encoding``).
        file_options: Uploader-declared facts to pass through unchanged
            (``column_descriptors`` for per-column ``unit``/``ordinal``, and the
            scalar alignment facts ``color_mode``/``language``/… when the caller
            has them). Nothing here is recomputed from the table.
        api_client: Unused while building (kept out of the metadata computation);
            accepted so a caller can hand its client through to a later send
            step. Defaults to ``None``.

    Returns:
        ``{"schema": <flat or enriched schema>, "meta_data": {"attributes": {...}}}``
        — the same shape ``send_ingest_summary`` ships, minus the row-level
        ``labels`` / ``samples``. ``meta_data`` is ``{}`` when there is nothing
        to contribute.
    """
    schema = database.get_table_schema(table_name)
    if data_format is None:
        data_format = spec_for(category).data_format

    # Reuse the live ingestor's shaping so the output is byte-identical to a fresh
    # ingest: we only feed it the accumulators (computed via SQL instead of a
    # chunked read) and call its existing metadata methods. api_client is only
    # stored by __init__, never used by the methods we call, so None is safe.
    ingestor = CSVIngestor(
        database=database,
        api_client=api_client,
        table_name=table_name,
        schema=schema,
        category=category,
        label_column=label_column,
        data_format=data_format,
        file_options=dict(file_options or {}),
    )

    # Same gate as the live cast pass (#385): alignment stats describe cell
    # values as FEATURES, which is only true for tabular-family categories.
    # Injecting the SQL-built accumulators directly would bypass that
    # accumulation-time gate and ship a manifest table's TEXT cell values as
    # vocab (bugbot High on #383). Honor the ingestor's own flag — derived
    # from the category in __init__ — and skip the table scans entirely,
    # mirroring what a fresh ingest of the same table would emit.
    if ingestor._emit_alignment_stats:
        table = Table(table_name, MetaData(), autoload_with=database.engine)
        with database.engine.connect() as conn:
            ingestor._feature_stats_acc = _numeric_feature_stats(
                conn, table, schema, category
            )
            ingestor._categorical_acc = _categorical_counts(conn, table, schema)

    enriched_schema = ingestor._schema_payload(schema)
    run_meta = ingestor._collect_run_metadata()

    meta_data: Dict[str, Any] = {}
    attributes = run_meta.get("attributes")
    if attributes:
        meta_data["attributes"] = attributes
    return {"schema": enriched_schema, "meta_data": meta_data}


def _numeric_feature_stats(conn, table, schema, category) -> Dict[str, Dict[str, Any]]:
    """Per-numeric-column sufficient statistics from SQL aggregates, in the exact
    ``{count, sum, sum_sq, min, max}`` shape the live ingest accumulates.

    ``count`` is the non-null count and every aggregate ignores nulls (``WHERE
    col IS NOT NULL``), matching the ingest path's ``dropna()``. An all-null
    column is omitted (min/max undefined), also matching. The ``label`` column is
    the target: its stats are kept for regression-class tasks (emitted under
    ``label``) and skipped otherwise (there it is the class label, not a feature).
    """
    stats: Dict[str, Dict[str, Any]] = {}
    for col, sql_type in schema.items():
        if col in _FRAMEWORK_COLUMNS:
            continue
        if col == _TARGET_COLUMN and category not in REGRESSION_CLASS_CATEGORIES:
            continue
        dtype = canonical_dtype(sql_type)
        if dtype not in _NUMERIC_DTYPES:
            continue
        column = table.c[col]
        # Sum and sum-of-squares in float (DOUBLE), matching the live path which
        # squares in float64 (``(fvals**2).sum()``). Without the cast, MySQL
        # squares an INT column in integer/DECIMAL — which can drift from the
        # float64 result (so sum_sq wouldn't stay byte-identical) and can overflow
        # BIGINT on large values. min/max keep the raw column so an INT column
        # still reports integer extremes (matches the ingest's ``.item()``).
        column_as_float = cast(column, Float)
        count, total, total_sq, low, high = conn.execute(
            select(
                func.count(column),
                func.sum(column_as_float),
                func.sum(column_as_float * column_as_float),
                func.min(column),
                func.max(column),
            ).where(column.isnot(None))
        ).one()
        if not count:
            continue
        is_int = dtype == "int"
        stats[col] = {
            "count": int(count),
            "sum": float(total),
            "sum_sq": float(total_sq),
            "min": _coerce_scalar(low, is_int),
            "max": _coerce_scalar(high, is_int),
        }
    return stats


def _categorical_counts(conn, table, schema) -> Dict[str, Counter]:
    """Per-categorical-column value counts (a ``Counter``) from a bounded ``GROUP
    BY``, matching the accumulator ``CSVIngestor.categorical_vocab`` consumes.

    String columns only; the framework columns and the ``label`` target/class are
    excluded (the label is never a categorical *feature*). A column with more
    distinct values than the cardinality cap is dropped (free text / id). The
    per-value ``CATEGORICAL_MIN_COUNT`` suppression is applied later by
    ``categorical_vocab`` — here we just carry the raw counts.
    """
    counts: Dict[str, Counter] = {}
    for col, sql_type in schema.items():
        if col in _FRAMEWORK_COLUMNS or col == _TARGET_COLUMN:
            continue
        if canonical_dtype(sql_type) != "string":
            continue
        column = table.c[col]
        # LIMIT cap+1 so a high-cardinality free-text/id column doesn't pull every
        # distinct value into memory just to be dropped: cap+1 rows is enough to
        # know it exceeds the cap.
        rows = conn.execute(
            select(column, func.count())
            .where(column.isnot(None))
            .group_by(column)
            .limit(_MAX_CATEGORICAL_CARDINALITY + 1)
        ).all()
        if len(rows) > _MAX_CATEGORICAL_CARDINALITY:
            continue
        counter: Counter = Counter()
        for value, cnt in rows:
            counter[str(value)] = int(cnt)
        if counter:
            counts[col] = counter
    return counts


def _coerce_scalar(value: Any, is_int: bool) -> Any:
    """Coerce a SQL aggregate scalar to a JSON-serialisable native number, keeping
    the column's real type (INT columns report integer min/max, like the live
    ingest's ``.item()``). ``SUM`` of an INT column comes back as ``Decimal``;
    ``int()`` / ``float()`` normalise it."""
    if value is None:
        return None
    return int(value) if is_int else float(value)
