"""Shared column-name resolution (#340).

The label / filename columns a dataset author declares in the manifest may not
match the CSV/JSON header exactly in case or surrounding whitespace. Both the
validators (which decide accept / reject) and the ingest read path (which pulls
the value out of each record) must resolve a declared name to the actual header
the SAME way — otherwise a manifest can pass preflight and then read ``None``
for every row.

That was the #340 divergence: the validators matched the label column
case-/whitespace-insensitively (``BaseValidator._match_column``), but
``RecordProcessor`` read it by exact key, so a config ``label: label`` against a
header ``Label`` passed both preflights and then silently nulled every label at
ingest.

This module is that single matching rule. ``BaseValidator._match_column``
delegates here, and ``BaseIngestor`` calls it to pin the resolved label column
once per run so the read path and the validators agree.
"""

from typing import Any, Optional


def resolve_column(columns: Any, name: str) -> Optional[str]:
    """Return the actual column in ``columns`` matching ``name``, case- AND
    whitespace-insensitively; ``None`` when nothing matches.

    ``columns`` is any iterable of column names (a DataFrame's ``.columns``, an
    Index, a plain list, a dict's keys). An exact match wins first; otherwise
    both sides are compared stripped + lower-cased. The ORIGINAL (un-lowercased)
    column spelling is returned so callers can index the raw frame / record with
    it. Surrounding whitespace is stripped on both sides because ``CSVIngestor``
    strips header whitespace on read (``chunk.columns.str.strip()``), so a header
    ``" label "`` is ingested as ``label`` and must resolve here too.
    """
    cols = list(columns)
    if name in cols:
        return name
    target = str(name).strip().lower()
    normalised = {str(c).strip().lower(): c for c in cols}
    return normalised.get(target)
