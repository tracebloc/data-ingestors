"""Owned tabular schema-inference rules — the platform's source of truth.

RFC-0002 §8.1 (epic backend#1008, design cli#174). When a tabular dataset
arrives without a declared column→type schema, *something* has to decide each
column's SQL type. That decision is load-bearing: it is meant to drive the
CREATE TABLE DDL, the per-column cast/validation (``CSVIngestor._validate_csv``),
and the model's ``num_feature_points``. A wrong type silently corrupts training
data — the canonical case being a zero-padded code like ``"007"`` inferred as
``INT`` and stored as ``7``, destroying a ZIP / accession / gene / account id.

NOT YET WIRED (#349 scope): this module defines and pins the rules; the ingest
path is not changed to *call* it in this diff. Today ``CSVIngestor`` still
receives a caller-supplied schema (``resolved.schema``) and never infers. So the
``"007"`` corruption is prevented at the layer that OWNS the decision, but only
once a follow-up routes the no-schema path through ``infer_schema`` (and the Go
CLI through ``InferSchema``). Until then the protection lives in the tests and
the parity contract, not in a running ingest.

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
       value length in CHARACTERS / code points, floor 1).

``VARCHAR(n)`` — ``n`` counts CHARACTERS (Unicode code points), matching MySQL's
``VARCHAR(n)`` semantics (n characters, not bytes). The Go CLI mirror MUST size
by rune count (``utf8.RuneCountInString``), NOT ``len(string)`` (which counts
UTF-8 bytes) — otherwise the two emit different DDL for any multibyte value
(``"café"`` -> ``VARCHAR(4)`` here vs ``VARCHAR(5)`` for a byte count). The parity
fixture is ASCII-only, so it cannot catch that drift; the unit is fixed here.

SAMPLING CAP
------------
Inference reads at most the first :data:`SAMPLE_CAP` (5,000) rows per column —
matching the CLI. This bounds work on wide/deep files. The trade-off: a value
longer than the sample's max, or a rare off-type token, that appears only past
row 5,000 won't influence the inferred type.

The cast layer (``CSVIngestor._validate_csv``) is a PARTIAL backstop here, not a
full one. A token that is genuinely non-castable to the inferred type (e.g.
``"abc"`` in an INT column) surfaces as a clear per-column error. But a token
that is *numerically castable yet semantically wrong* is coerced SILENTLY: a
zero-padded code like ``"00501"`` first appearing past the cap infers ``INT`` and
casts to ``501`` (zeros stripped), and a ``"3.5"`` past the cap in an otherwise
integer column is coerced by ``pd.to_numeric`` without error. So the cap can
still lose a leading-zero code or truncate a float if the giveaway row sits
beyond row 5,000 — a real (if narrow) residual risk, not "never silent
corruption". Raise :data:`SAMPLE_CAP` (in lockstep with the CLI) if a dataset is
known to hide off-type values deep in a column.
"""

from __future__ import annotations

import math
import re
import warnings
from itertools import islice
from typing import Any, Dict, Iterable, List, Mapping, Union

import pandas as pd

from .utils import coercion

__all__ = ["SAMPLE_CAP", "canonical_dtype", "infer_column_type", "infer_schema"]

# Storage SQL type -> canonical LOGICAL dtype, for the enriched schema's
# cross-dataset comparison at combine time (data-ingestors#360 slice 1b,
# backend#1037). Parametrisation (``VARCHAR(255)``, ``DECIMAL(10,2)``) is
# stripped first, so two datasets whose "same" column differs only in width
# (``VARCHAR(255)`` vs ``VARCHAR(100)``) — or in integer storage size (``INT``
# vs ``BIGINT``) — compare EQUAL, while a genuine ``int`` vs ``float`` divergence
# still differs. Covers both the MySQL keywords ``get_table_schema`` reflects
# and the types ``infer_column_type`` emits.
_CANONICAL_DTYPE = {
    "INT": "int", "INTEGER": "int", "BIGINT": "int", "SMALLINT": "int",
    "MEDIUMINT": "int", "TINYINT": "int",
    "FLOAT": "float", "DOUBLE": "float", "DOUBLE_PRECISION": "float",
    "REAL": "float", "DECIMAL": "float", "NUMERIC": "float",
    "BOOL": "bool", "BOOLEAN": "bool",
    "DATE": "date",
    "DATETIME": "datetime", "TIMESTAMP": "datetime",
    "TIME": "time",
    "VARCHAR": "string", "CHAR": "string", "TEXT": "string",
    "TINYTEXT": "string", "MEDIUMTEXT": "string", "LONGTEXT": "string",
    "STRING": "string", "ENUM": "string",
    "BLOB": "binary", "BINARY": "binary", "VARBINARY": "binary",
    "LARGEBINARY": "binary",
}


def canonical_dtype(sql_type: str) -> str:
    """Map a storage SQL type (``"VARCHAR(255)"``, ``"BIGINT"``) to a canonical
    logical dtype (``"string"``, ``"int"``).

    Strips any ``(...)`` parametrisation and folds storage-size variants to one
    logical family, so cross-dataset ``dtype`` equality reflects a real type
    divergence rather than a width difference. An unrecognised type falls
    through as its lower-cased base keyword — honest rather than silently
    coerced to ``"string"``.
    """
    base = sql_type.split("(", 1)[0].strip().upper()
    return _CANONICAL_DTYPE.get(base, base.lower())

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

# ASCII-only digit classes ([0-9], NOT \d). Python's ``\d`` also matches Unicode
# decimal digits (e.g. Arabic-Indic ``"١٢٣"``) and ``int()``/``float()`` happily
# parse them — but the Go CLI mirror matches ASCII only, and a MySQL INT/FLOAT
# column can't store the raw glyphs. Restricting to ``[0-9]`` keeps the two
# implementations in lockstep and routes non-ASCII-digit tokens to VARCHAR.
_LEADING_ZERO_CODE = re.compile(r"^[+-]?0[0-9]+$")
_INT_RE = re.compile(r"^[+-]?[0-9]+$")

# Finite-float grammar, ASCII-only. Pre-screens the token BEFORE ``float()`` so
# Python leniencies that Go's ``strconv.ParseFloat`` rejects can't diverge:
# underscore grouping (``float("1_000") == 1000.0``), Unicode digits, and the
# ``inf``/``nan``/``Infinity`` spellings ``float()`` accepts. Scientific notation
# is allowed (Go parses it too). The ``math.isfinite`` guard below still catches
# a regex-valid overflow like ``"1e400"`` -> ``inf``.
_FLOAT_RE = re.compile(r"^[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$")


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
    if not _FLOAT_RE.match(token):
        return False
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
    contain at least one ASCII digit (so ``"apple"`` / month-name columns stay
    text). ASCII specifically — ``str.isdigit()`` is true for Unicode digits
    (``"١٢٣"``) which some pandas versions then PARSE as a date, mis-typing a
    non-ASCII-digit column DATE instead of VARCHAR. A real calendar date only
    ever contains ASCII digits. Numeric columns never reach here — classified
    earlier.
    """
    if not all(any(c in "0123456789" for c in t) for t in tokens):
        return None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            parsed = pd.to_datetime(pd.Series(tokens), errors="coerce", format="mixed")
    except (ValueError, TypeError):
        # pandas is not version-stable here: mixed-timezone tokens (one
        # "+00:00", one "+05:00") RAISE "Mixed timezones detected" on newer
        # pandas and return an OBJECT-dtype Series (handled below) on older —
        # either way it's not a clean single-tz calendar column. Fall back to
        # text rather than crash the whole schema pass.
        return None
    if parsed.isna().any():
        return None
    if not pd.api.types.is_datetime64_any_dtype(parsed):
        # Older pandas: mixed-tz tokens parse to an OBJECT-dtype Series of
        # Timestamps, not a DatetimeIndex — it has no ``.dt`` accessor, so the
        # ``has_time`` line below would raise an uncaught AttributeError and
        # abort the whole schema pass. Fall back to text: a tz-mixed column is
        # safer as VARCHAR than a tz-naive MySQL DATETIME that silently drops
        # the offset anyway.
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
