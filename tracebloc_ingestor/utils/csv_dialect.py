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


# Dialect keys whose value must be a string when present — the char / encoding
# options (a subset of READ_DIALECT_KEYS). ``quoting`` is an int (csv.QUOTE_*),
# checked separately; the boolean/flag keys (doublequote, skipinitialspace) are
# left for pandas to coerce.
_STR_DIALECT_KEYS = (
    "sep",
    "delimiter",
    "quotechar",
    "escapechar",
    "encoding",
    "encoding_errors",
    "lineterminator",
    "comment",
    "engine",
)


def validate_csv_options(csv_options: Optional[Dict[str, Any]]) -> None:
    """Fail fast on a malformed *csv_options* dialect value, raising
    ``ValueError`` at construction rather than letting it surface as a generic
    parse failure deep inside a validator's read (validate config at
    construction, not mid-scan). Checks only the dialect keys the validators
    forward to pandas (see :data:`READ_DIALECT_KEYS`)."""
    opts = csv_options or {}
    for key in _STR_DIALECT_KEYS:
        val = opts.get(key)
        if val is not None and not isinstance(val, str):
            raise ValueError(
                f"csv_options['{key}'] must be a string, got "
                f"{type(val).__name__} — check the ingest config."
            )
    quoting = opts.get("quoting")
    if quoting is not None and not isinstance(quoting, int):
        raise ValueError(
            f"csv_options['quoting'] must be an int (csv.QUOTE_*), got "
            f"{type(quoting).__name__} — check the ingest config."
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
