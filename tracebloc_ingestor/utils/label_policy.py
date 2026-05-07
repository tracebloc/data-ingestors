"""Label-policy bucketing for regression-class tasks.

Per #44 (and the parent client#85): when the ``label`` column is a numeric
prediction target — regression, time-series forecasting, time-to-event
prediction — the raw value must NOT leak to the central backend. The
on-prem-data principle is that only metadata crosses the cluster boundary;
shipping the literal target value defeats it.

This module is the single point where raw labels become bucket IDs before
``APIClient.send_batch`` builds its payload.

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
from typing import Any


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
        f"Unknown label policy: {policy!r}. "
        f"Valid: {PASSTHROUGH!r}, {BUCKET!r}."
    )


def _bucket(value: Any) -> int:
    """Stable hash-bucket of ``str(value)``.

    Empty / None / whitespace-only values produce ``MISSING_LABEL_BUCKET``
    so the central backend can distinguish "no label" from "bucket 0"
    without an extra flag.
    """
    if value is None:
        return MISSING_LABEL_BUCKET
    text = str(value).strip()
    if not text:
        return MISSING_LABEL_BUCKET
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % NUM_BUCKETS


__all__ = [
    "apply",
    "PASSTHROUGH",
    "BUCKET",
    "NUM_BUCKETS",
    "MISSING_LABEL_BUCKET",
]
