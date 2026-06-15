"""Single source of truth for numeric coercion + the missing-value policy.

Three layers used to each decide *independently* what a declared MySQL type
permits and what tokens count as missing:

  - the ``DataValidator`` gate (validators/data_validator.py),
  - the CSV type-cast (``CSVIngestor._validate_csv``),
  - the JSON per-record check (``JSONIngestor._validate_value_against_dtype``).

Because the rules were duplicated, the layers drifted (#189, #204) and
disagreed: a file passed the validator gate and then crashed mid-ingest
(#236 out-of-int64 values, #237 ``NA``/``null`` sentinels in non-tabular
CSVs). Centralising both decisions here means the gate and the ingest read
the file the same way and reach the same verdict on the same value — by
construction, not by a "keep this in lockstep" comment.

Robustness note (#236): pandas reads an out-of-int64 column as ``object``
dtype holding *strings*, and ``pd.to_numeric(..., errors="raise")`` on that
raises a cryptic ``Integer out of range``; ``np.isinf`` / ``>`` on the raw
object series throw outright. Every check here therefore coerces with
``errors="coerce"`` first and inspects the resulting float — it never
compares or ``isinf``-checks a raw object series.
"""

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

__all__ = [
    "INT64_MIN",
    "INT64_MAX",
    "NA_SENTINELS",
    "build_csv_na_values",
    "int_range_error",
    "int_value_overflows",
]

# Signed 64-bit integer bounds — the widest MySQL integer (BIGINT) and the
# pandas ``Int64`` the INT cast targets. Values beyond this are what made
# the old overflow guard throw a numpy error instead of a clean message.
INT64_MIN = -9223372036854775808
INT64_MAX = 9223372036854775807

# Tokens treated as MISSING (-> SQL NULL) in declared *schema* columns.
# Spelled out explicitly (rather than relying on pandas' ``keep_default_na``
# set, which has shifted across versions) so the validator and the ingestor
# apply byte-identical rules. Applied only to schema columns by
# ``build_csv_na_values`` — never to the framework's own ``filename`` /
# ``mask_id`` columns — so a file legitimately named ``NA.jpg`` survives.
NA_SENTINELS: List[str] = [
    "",
    "NA",
    "N/A",
    "n/a",
    "NULL",
    "null",
    "None",
    "none",
    "NaN",
    "nan",
    "<NA>",
    "#N/A",
]

# Integer base types — used to decide whether the int64-range check applies
# (a 1e26 value is a valid FLOAT but an out-of-range INT/BIGINT).
_INT_TYPES = frozenset(
    {
        "INT",
        "INTEGER",
        "TINYINT",
        "SMALLINT",
        "MEDIUMINT",
        "BIGINT",
    }
)


def _base_type(mysql_type: str) -> str:
    """First word, sans parenthesised args — ``DECIMAL(10,2)`` -> ``DECIMAL``,
    ``INT UNSIGNED`` -> ``INT``."""
    return str(mysql_type).strip().upper().split("(")[0].split()[0]


def build_csv_na_values(schema: Dict[str, str]) -> Dict[str, List[str]]:
    """Per-column ``na_values`` for ``pd.read_csv``, shared by the validator
    gate and ``CSVIngestor`` so the two read a file identically (#237).

    Every *schema* column — numeric, date AND string alike — gets the full
    :data:`NA_SENTINELS` set, so ``""``/``NA``/``null``/``None`` parse as
    missing (-> SQL NULL) consistently across *every* category. The
    per-category split (tabular treated these as missing, other categories
    kept them as literals and then crashed the numeric cast) was the #237
    bug.

    Use with ``keep_default_na=False`` so pandas' global default NA set never
    reaches a non-schema column. Columns absent from the schema — the
    framework's own ``filename`` / ``mask_id`` / unique-id columns — are
    intentionally omitted: they get no NA coercion at read time, so a file
    named ``"NA.jpg"`` keeps its name (an empty cell there is normalised to
    ``None`` later by ``BaseIngestor.process_record``).
    """
    return {col: list(NA_SENTINELS) for col in schema}


def int_range_error(original: pd.Series, column: str, mysql_type: str) -> Optional[str]:
    """Return a clear error if an INT/BIGINT column holds values outside the
    signed 64-bit range, else ``None`` (#236).

    Object/string-dtype safe: coerces with ``errors="coerce"`` (which yields
    a float — lossy but sufficient to flag magnitude) and range-checks the
    float, so it never does the ``np.isinf`` / ``>`` on a raw object series
    that threw the cryptic ``ufunc 'isinf' not supported`` /
    ``'>' not supported between str and int`` errors.

    No-op for non-integer types (a ``1e26`` value is a valid FLOAT).
    """
    if _base_type(mysql_type) not in _INT_TYPES:
        return None
    coerced = pd.to_numeric(original, errors="coerce")
    # Present values that coerced to a finite number outside the int64 range.
    finite = coerced.notna() & np.isfinite(coerced)
    mask = finite & ((coerced > INT64_MAX) | (coerced < INT64_MIN))
    count = int(mask.sum())
    if count == 0:
        return None
    sample = original[mask].head(5).tolist()
    base = _base_type(mysql_type)
    hint = (
        ""
        if base == "BIGINT"
        else " Declare the column as BIGINT if you need integers this large."
    )
    return (
        f"Column '{column}' has {count} value(s) outside the signed 64-bit "
        f"integer range (max {INT64_MAX}): {sample}.{hint}"
    )


def int_value_overflows(value: Any) -> bool:
    """Scalar form of the int64-range check, for the JSON per-record path.

    ``float(value)`` is enough to flag magnitude: a 26-digit string becomes
    ``1e26`` which compares above :data:`INT64_MAX`. Non-numeric input is not
    our concern here (the caller's non-numeric check handles it), so it
    returns ``False``.
    """
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    if not np.isfinite(f):
        return False
    return f > INT64_MAX or f < INT64_MIN
