"""Label-policy bucketing for regression-class tasks.

Per #44 (and the parent client#85): when the ``label`` column is a numeric
prediction target — regression, time-series forecasting, time-to-event
prediction — the raw value must NOT leak to the central backend. The
on-prem-data principle is that only metadata crosses the cluster boundary;
shipping the literal target value defeats it.

This module is the single point where raw labels become bucket IDs, and it runs
at the CLUSTER BOUNDARY — inside ``APIClient.send_ingest_summary``, on the
payload only. The row written to the on-prem MySQL table keeps the RAW target.

That distinction is the whole bug of #486: the policy used to run in
``RecordProcessor.clean_record``, i.e. on the record handed to the batch writer
for INSERT. The stored target became a bucket id, the raw value was persisted
nowhere, and the training client read hash buckets as labels — a hard failure
for ``time_to_event_prediction`` (the engine requires the event column ∈ {0,1})
and a silent one for ``tabular_regression`` / ``time_series_forecasting``. The
local data is what training consumes; only metadata is in scope for bucketing.

v1 strategy: stable hash-bucket. ``sha256(str(value))`` truncated to 8 bytes,
modulo ``NUM_BUCKETS``. Properties:

- **Stable**: same value always lands in the same bucket. The central
  backend can correlate identical labels across runs without seeing them.
- **Privacy-preserving**: raw value is not derivable from the bucket ID.
- **One-pass**: no need to scan the dataset twice to find min/max for
  equal-width bins. Plays nicely with the existing chunked CSV reader.
- **Lossy on ordinality**: close numeric values may land in distant
  buckets. That's a feature for privacy; analytic insights stay on-prem.

Equal-width or quantile bucketing is a v1.1 improvement if customers ask for
it; the schema can grow ``label.policy: equal_width`` / ``quantile`` without
breaking ``passthrough`` / ``bucket`` consumers.
"""

from __future__ import annotations

import hashlib
import math
from typing import Any, Dict, Iterable, List, Mapping, Tuple, Union

# Number of buckets. 64 is enough granularity for the central backend to
# reason about distribution without offering reconstruction power. Trade-off
# is documented; bumping this number requires no schema change.
NUM_BUCKETS = 64

# Sentinel used when the label is missing/empty under the ``bucket`` policy.
# ``-1`` is outside the ``[0, NUM_BUCKETS)`` range so it can't collide with
# a real bucket; central backend can render it as "no label" without a flag.
MISSING_LABEL_BUCKET = -1


# Policy name constants — mirror the schema enum. Importable so the
# entrypoint and tests don't string-literal these in three places.
PASSTHROUGH = "passthrough"
BUCKET = "bucket"


def apply(value: Any, policy: str) -> Any:
    """Apply the configured label policy to a single label value.

    Args:
        value: Raw label as read from the source CSV/JSON.
        policy: Either ``"passthrough"`` (classification — value sent
            unchanged) or ``"bucket"`` (regression-class — value replaced
            with a stable hash-derived bucket ID in ``[0, NUM_BUCKETS)``,
            or ``MISSING_LABEL_BUCKET`` if missing).

    Returns:
        The value unchanged for ``passthrough``; an int for ``bucket``.

    Raises:
        ValueError: if ``policy`` is unknown. Should be unreachable since
            the schema's enum constrains valid values.
    """
    if policy == PASSTHROUGH:
        return value
    if policy == BUCKET:
        return _bucket(value)
    raise ValueError(
        f"Unknown label policy: {policy!r}. " f"Valid: {PASSTHROUGH!r}, {BUCKET!r}."
    )


def _bucket(value: Any) -> int:
    """Stable hash-bucket of ``str(value)``.

    None / NaN / empty / whitespace-only values produce ``MISSING_LABEL_BUCKET``
    so the central backend can distinguish "no label" from "bucket 0"
    without an extra flag. pandas reads missing numeric cells as
    ``float('nan')``, which ``str()`` renders as ``"nan"`` — non-empty —
    so it needs an explicit float-NaN check before stringification.
    """
    if value is None:
        return MISSING_LABEL_BUCKET
    if isinstance(value, float) and math.isnan(value):
        return MISSING_LABEL_BUCKET
    text = str(value).strip()
    if not text:
        return MISSING_LABEL_BUCKET
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % NUM_BUCKETS


def apply_to_label_counts(
    counts: Union[Mapping[Any, int], Iterable[Tuple[Any, int]]], policy: str
) -> Dict[Any, int]:
    """Bucket the keys of a ``{label: row_count}`` map for the outbound payload.

    Args:
        counts: The per-label row counts read back from the cluster DB, keyed by
            the RAW label — either a mapping
            (:meth:`Database.get_label_counts`) or an iterable of
            ``(label, count)`` pairs (:meth:`Database.iter_label_counts`). The
            pair form is folded as it arrives, so the ungrouped counts of a raw
            continuous target are never materialised (#488): peak memory is the
            <= 64 buckets, not the distinct-value count.
        policy: ``"passthrough"`` (returned unchanged) or ``"bucket"``.

    Returns:
        Under ``bucket``, ``{"<bucket_id>": row_count}``. Counts of raw values
        that share a bucket are SUMMED — with 64 buckets, collisions are
        expected (a 300-label dataset averages ~5 raw values per bucket), and
        dropping one of the colliding entries would under-report the dataset's
        size to the backend.

        Keys are STRINGS, matching what the backend has always received: the
        bucket id used to be written to the VARCHAR ``label`` column and read
        back as a string, and JSON object keys are strings regardless. Same
        reason ``apply_to_samples`` stringifies.

    Raises:
        ValueError: if ``policy`` is unknown (via :func:`apply`).
    """
    pairs = counts.items() if isinstance(counts, Mapping) else counts
    if policy == PASSTHROUGH:
        # A mapping is copied rather than aliased; pairs are drained into a
        # dict. GROUP BY makes the keys unique either way, so no summing is
        # needed on this branch.
        return dict(pairs)
    bucketed: Dict[Any, int] = {}
    for label, count in pairs:
        key = str(apply(label, policy))
        bucketed[key] = bucketed.get(key, 0) + count
    return bucketed


def apply_to_samples(
    samples: Iterable[Mapping[str, Any]], policy: str
) -> List[Dict[str, Any]]:
    """Bucket the ``label`` of each outbound ``{data_id, label}`` sample.

    Args:
        samples: The preview records from ``Database.get_samples``, carrying
            the RAW label.
        policy: ``"passthrough"`` (labels returned unchanged) or ``"bucket"``.

    Returns:
        New sample dicts — the input mappings are never mutated. A sample
        without a ``label`` key is passed through untouched rather than gaining
        a bucketed ``None``: the absence is the backend's signal, and inventing
        ``MISSING_LABEL_BUCKET`` for a shape that never had a label would
        misreport it as a label the ingest saw.

        The bucketed label is a STRING, not the raw ``int``. Pre-#486 the
        bucket was stored in the VARCHAR ``label`` column and read back out by
        ``Database.get_samples``, so ``data_samples[].label`` has always been a
        JSON string — as it is for classification class names. The backend's
        summary serializer takes ``samples`` as an untyped ``ListField`` and
        would accept a number, but every consumer downstream of
        ``UserDataSet.data_samples`` has only ever seen strings, so this keeps
        the wire shape identical rather than betting on their tolerance
        (review on #487).

    Raises:
        ValueError: if ``policy`` is unknown (via :func:`apply`).
    """
    if policy == PASSTHROUGH:
        return [dict(sample) for sample in samples]
    out: List[Dict[str, Any]] = []
    for sample in samples:
        record = dict(sample)
        if "label" in record:
            record["label"] = str(apply(record["label"], policy))
        out.append(record)
    return out


__all__ = [
    "apply",
    "apply_to_label_counts",
    "apply_to_samples",
    "PASSTHROUGH",
    "BUCKET",
    "NUM_BUCKETS",
    "MISSING_LABEL_BUCKET",
]
