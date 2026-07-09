"""Owned tabular schema-inference rules — the platform's source of truth.

RFC-0002 §8.1 (epic backend#1008, design cli#174). When a tabular dataset
arrives without a declared column→type schema, *something* has to decide each
column's SQL type. That decision is load-bearing: it drives the CREATE TABLE
DDL, the per-column cast/validation (``CSVIngestor._validate_csv``), and the
model's ``num_feature_points``. A wrong type silently corrupts training data —
the canonical case being a zero-padded code like ``"007"`` inferred as ``INT``
and stored as ``7``, destroying a ZIP / accession / gene / account id.

Under Option A these rules live HERE and the Go CLI (``InferSchema``, re-scoped
cli#185) mirrors them, verified by the value-level parity harness
(backend#1009). ``tests/fixtures/schema_inference_parity.json`` enumerates the
(values → expected type) contract both sides must satisfy — edit it once and
both implementations are pinned to the same answer.

INPUT CONTRACT
--------------
Inference must run on the RAW, string-form values (a CSV read with
``dtype=str``, or the CLI's raw byte columns). If pandas has already
type-inferred the frame, ``"007"`` is *already* ``7`` and the leading-zero
signal is gone — inference then can't help. Feed strings.

RULES — evaluated per column, FIRST MATCH WINS (this is the precedence):
  0. All values missing/empty       -> ``VARCHAR(1)``  (nothing to infer)
  1. Any leading-zero numeric code   -> ``VARCHAR(n)``  (THE #349 fix)
       a token matching ``^[+-]?0\\d+$`` (all digits, leading zero,
       length > 1: ``"007"``, ``"00123"``) is a CODE, not a number — typing it
       INT strips the zeros. ONE such token in the sample pins the whole
       column to text.
  2. All values textual boolean      -> ``BOOLEAN``
       every token in ``{true,false,t,f,yes,no,y,n}`` (case-insensitive). A
       pure ``0``/``1`` column is deliberately INT, not BOOL: ``0/1`` is
       ambiguous and the INT reading is lossless.
  3. All values integer              -> ``INT`` / ``BIGINT``
       ``^[+-]?\\d+$`` and within signed int64. ``BIGINT`` when any ``|value|``
       exceeds signed int32 (2147483647), so a large id can't overflow a MySQL
       ``INT`` column. An all-digit value beyond int64 is not storable as an
       integer and falls through to ``VARCHAR``.
  4. All values float                -> ``FLOAT``
       parseable by ``float()`` and finite (``inf``/``-inf`` are rejected so a
       column of ``"inf"`` is not called numeric).
  5. All values date / datetime      -> ``DATE`` / ``DATETIME``
       parseable as a calendar date — checked AFTER numeric, so an all-digit id
       column (e.g. ``"20240101"``) is INT, never a date. ``DATETIME`` when any
       value carries a time-of-day component, else ``DATE``.
  6. Otherwise                       -> ``VARCHAR(n)``  (n = longest sampled
       value length, floor 1).

SAMPLING CAP
------------
Inference reads at most the first :data:`SAMPLE_CAP` (5,000) rows per column —
matching the CLI. This bounds work on wide/deep files. The trade-off: a value
longer than the sample's max, or a rare off-type token, that appears only past
row 5,000 won't influence the inferred type. That is safe because the cast
layer (``CSVIngestor._validate_csv``) still fails LOUDLY on a value that does
not fit the inferred type — an escaped off-type token surfaces as a clear
per-column error, never silent corruption.
"""

from __future__ import annotations

import math
import re
import warnings
from itertools import islice
from typing import Any, Dict, Iterable, List, Mapping, Union

import pandas as pd

from .utils import coercion

__all__ = ["SAMPLE_CAP", "infer_column_type", "infer_schema"]

# First N rows per column that influence inference. Mirrors the CLI's cap so
# the two implementations agree on the same prefix of a file.
SAMPLE_CAP = 5000

# Signed 32-bit bounds — a value outside this range needs BIGINT, not INT, or
# it overflows a MySQL INT column on write. (int64 bounds live in ``coercion``.)
INT32_MIN = -2147483648
INT32_MAX = 2147483647

# Textual boolean tokens ONLY — deliberately NOT the 0/1 digit forms that
# ``coercion.BOOL_STRINGS`` also blesses. A pure 0/1 column is inferred INT
# (lossless); only unambiguous words map to BOOLEAN here.
_BOOL_TEXT = frozenset({"true", "false", "t", "f", "yes", "no", "y", "n"})

_NA = frozenset(coercion.NA_SENTINELS)

_LEADING_ZERO_CODE = re.compile(r"^[+-]?0\d+$")
_INT_RE = re.compile(r"^[+-]?\d+$")


def _clean_tokens(values: Iterable[Any]) -> List[str]:
    """First :data:`SAMPLE_CAP` raw values → stripped, non-missing strings.

    The cap is applied to the RAW values (first 5,000 rows), then missing
    tokens are dropped — so a column of 5,000 blanks followed by data still
    reads as effectively empty, exactly as the CLI's row-capped scan would.
    """
    out: List[str] = []
    for v in islice(values, SAMPLE_CAP):
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass  # non-scalar (list/dict) — keep, str() below handles it
        s = str(v).strip()
        if s == "" or s in _NA:
            continue
        out.append(s)
    return out


def _is_finite_float(token: str) -> bool:
    try:
        return math.isfinite(float(token))
    except (TypeError, ValueError):
        return False


def _varchar(tokens: List[str]) -> str:
    return f"VARCHAR({max((len(t) for t in tokens), default=1)})"


def _infer_datetime(tokens: List[str]) -> Union[str, None]:
    """``DATE`` / ``DATETIME`` if every token parses as a calendar date, else
    ``None``.

    Guard against over-eager matching on plain words: require each token to
    contain at least one digit (so ``"apple"`` / month-name columns stay text).
    Numeric columns never reach here — they are classified earlier.
    """
    if not all(any(c.isdigit() for c in t) for t in tokens):
        return None
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        parsed = pd.to_datetime(pd.Series(tokens), errors="coerce", format="mixed")
    if parsed.isna().any():
        return None
    has_time = bool(
        ((parsed.dt.hour != 0) | (parsed.dt.minute != 0) | (parsed.dt.second != 0)).any()
    ) or any(":" in t for t in tokens)
    return "DATETIME" if has_time else "DATE"


def infer_column_type(values: Iterable[Any]) -> str:
    """Infer the SQL type for one column from its RAW values.

    See the module docstring for the full rule precedence. Returns an
    ``_get_sqlalchemy_type``-compatible type string (``INT``, ``BIGINT``,
    ``FLOAT``, ``BOOLEAN``, ``DATE``, ``DATETIME``, ``VARCHAR(n)``).
    """
    tokens = _clean_tokens(values)
    if not tokens:
        # No signal — the narrowest string type. The cast layer widens on
        # write; a real value never lands here (all-missing column).
        return "VARCHAR(1)"

    # 1. Leading-zero code — one such token pins the column to text (#349).
    if any(_LEADING_ZERO_CODE.match(t) for t in tokens):
        return _varchar(tokens)

    # 2. Textual boolean.
    if all(t.lower() in _BOOL_TEXT for t in tokens):
        return "BOOLEAN"

    # 3. Integer (INT vs BIGINT by magnitude; >int64 all-digit -> text).
    if all(_INT_RE.match(t) for t in tokens):
        ints = [int(t) for t in tokens]
        if all(coercion.INT64_MIN <= v <= coercion.INT64_MAX for v in ints):
            if all(INT32_MIN <= v <= INT32_MAX for v in ints):
                return "INT"
            return "BIGINT"
        return _varchar(tokens)  # beyond int64: not storable as an integer

    # 4. Float.
    if all(_is_finite_float(t) for t in tokens):
        return "FLOAT"

    # 5. Date / datetime (after numeric, so numeric ids can't be mis-dated).
    dt = _infer_datetime(tokens)
    if dt:
        return dt

    # 6. Fallback.
    return _varchar(tokens)


def infer_schema(
    data: Union[pd.DataFrame, Mapping[str, Iterable[Any]]],
) -> Dict[str, str]:
    """Infer a full ``column -> SQL type`` schema.

    Accepts a pandas ``DataFrame`` (read with ``dtype=str`` — see the INPUT
    CONTRACT) or any ``column -> values`` mapping. Column order is preserved.
    """
    if isinstance(data, pd.DataFrame):
        return {str(col): infer_column_type(data[col].tolist()) for col in data.columns}
    return {str(col): infer_column_type(vals) for col, vals in data.items()}
