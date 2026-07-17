"""Shared CSV read-dialect passthrough for preflight validators.

A validator that reads the manifest itself must tokenize it BYTE-IDENTICALLY to
:class:`CSVIngestor`, or a non-comma / BOM / quoted manifest that ingests fine
gets falsely rejected — or passes for the wrong reason — at preflight, then
ingests differently at runtime. This centralises the whitelist of tokenizing
dialect keys plus the BOM-safe encoding default so every CSV-reading validator
resolves the same columns + cells the write path does.

Used by :class:`MaskIdColumnValidator` and the ``time_series_classification``
grouped validators (backend#1054 WS1); the single source of truth so the set
can't drift between them (review: #371 bugbot).
"""

from typing import Any, Dict, Optional

# The ONLY keys forwarded to a validator's read: those that control how bytes
# are TOKENIZED into columns + cells. A whitelist, not a blacklist —
# frame-RESTRUCTURING keys (index_col, header, names, usecols, skiprows, nrows,
# ...) are excluded, because a validator pins its own usecols/nrows/dtype and
# forwarding a restructuring key would collide with those (a swallowed error
# that fail-opens the gate) or change which column is which. Unknown/new
# csv_options are dropped by default.
READ_DIALECT_KEYS = frozenset(
    {
        "sep",
        "delimiter",
        "quotechar",
        "doublequote",
        "escapechar",
        "quoting",
        "skipinitialspace",
        "encoding",
        "encoding_errors",
        "lineterminator",
        "comment",
        "engine",
    }
)


def read_dialect_kwargs(csv_options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """pandas ``read_csv`` kwargs matching CSVIngestor's tokenizer for
    *csv_options*.

    Forwards only the dialect keys (see :data:`READ_DIALECT_KEYS`), upgrades the
    encoding to its BOM-safe form (Excel's "CSV UTF-8" export, #338), and
    defaults the separator to ``","`` — so a validator's read splits fields
    exactly as the ingest write path does. Callers merge their own read-shape
    kwargs (``nrows`` / ``usecols`` / ``on_bad_lines`` / ``dtype`` / ...) on top.
    """
    # Lazy import: csv_ingestor -> base -> validators_mapping ->
    # modalities.validators -> the grouped validators -> this module, so a
    # top-level import would cycle.
    from ..ingestors.csv_ingestor import _bom_safe_encoding

    opts = {
        k: v for k, v in (csv_options or {}).items() if k in READ_DIALECT_KEYS
    }
    opts["encoding"] = _bom_safe_encoding(opts.get("encoding"))
    if "sep" not in opts and "delimiter" not in opts:
        opts["sep"] = ","
    return opts
