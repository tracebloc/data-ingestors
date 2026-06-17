"""Single source of truth for "Did you mean …?" suggestions on schema types.

Both the row-read layer (``Database._get_sqlalchemy_type``) and the preflight
``DataValidator`` reject unknown MySQL/SQLAlchemy types. Both should offer the
same suggestion when the user has clearly typo'd a real entry (``INTERGER`` →
``INTEGER``, ``BIGINTEGER`` → ``BIGINT``/``INTEGER``, ``NUMRIC`` → ``NUMERIC``).

Originally lived in :mod:`tracebloc_ingestor.database` as a private helper, but
the preflight validator rejects unknowns first (#266) — so the suggestion has
to be reachable from both call sites without circular imports. Extracted here
so each layer can wire its own error message identically.
"""

from typing import Iterable, Optional


def suggest_type(unknown: str, known: Iterable[str]) -> Optional[str]:
    """Return the closest match from ``known`` to ``unknown`` if any candidate
    is within edit distance 3 (Levenshtein), else ``None``.

    Distance 3 is the empirical sweet spot — close enough to catch every
    realistic typo seen in the wild (single-letter swaps, common
    prefix/suffix confusion like ``INT``↔``INTEGER``, missing or duplicated
    letters), wide enough to fail-silently on entries that are genuinely
    different vocabulary (no false "Did you mean DATE?" for ``GEOMETRY``).

    Returns the FIRST best match at the minimum distance — callers pass an
    ordered iterable (the type map keys) so the deterministic result is fine
    for tests.
    """
    if not unknown:
        return None

    target = unknown.upper()
    best: Optional[str] = None
    best_d = 99
    for candidate in known:
        d = _levenshtein(target, candidate)
        if d < best_d:
            best = candidate
            best_d = d
    return best if best_d <= 3 else None


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    # Two-row DP — O(len(a)*len(b)) time, O(min(a,b)) space.
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i] + [0] * len(b)
        for j, cb in enumerate(b, start=1):
            cost = 0 if ca == cb else 1
            curr[j] = min(
                prev[j] + 1,
                curr[j - 1] + 1,
                prev[j - 1] + cost,
            )
        prev = curr
    return prev[-1]
