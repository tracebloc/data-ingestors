"""CSV Data Ingestor Module.

This module provides a specialized ingestor for handling CSV files, with optimized
pandas-based reading and validation capabilities.
"""

from typing import Dict, Any, Generator, Optional, List
import codecs
from collections import Counter
import csv as _csv
import numpy as np
import pandas as pd
from ..utils import redaction
from ..utils.columns import resolve_column
import logging
from pathlib import Path

from .base import BaseIngestor
from ..database import Database
from ..api.client import APIClient
from ..cli.conventions import REGRESSION_CLASS_CATEGORIES
from ..utils.constants import RESET, RED, YELLOW, TaskCategory
from ..utils import label_policy as label_policy_module
from ..utils import coercion
from ..config import Config

config = Config()

# Above this many distinct values a VARCHAR column is treated as free text / an
# id-like field rather than a categorical feature, and no union vocabulary is
# emitted for it (di#360). A near-unique column's value-set is useless for
# stable cross-client encoding and would bloat the payload / leak more values.
_MAX_CATEGORICAL_CARDINALITY = 1000

# The time-series forecasting timestamp column is the literal name "timestamp"
# (TimeFormatValidator). A bounded, in-order sample is enough for
# ``pd.infer_freq`` to derive the sampling cadence; retaining all rows would be
# wasteful on large series.
_TIMESTAMP_COLUMN = "timestamp"
_MAX_TIMESTAMP_SAMPLE = 500


def _bom_safe_encoding(encoding: Optional[str]) -> str:
    """Return a BOM-stripping encoding for the stdlib ``csv.reader`` header
    probes (#338).

    ``utf-8-sig`` decodes UTF-8 identically to ``utf-8`` but also strips a
    leading byte-order mark. Excel's "CSV UTF-8" export prepends a BOM;
    ``open(..., encoding="utf-8")`` leaves it on the first header (a U+FEFF
    byte-order mark glued to ``age``) and ``str.strip()`` does not remove it
    — so a probe keyed on that header (the string-dtype pin, the
    duplicate-header check)
    silently misreads the first column, while every pandas read path (which
    strips the BOM) accepts the same file. Only the UTF-8 family is
    upgraded; an explicit non-UTF-8 encoding is returned unchanged.

    Canonicalise via ``codecs.lookup`` rather than matching a hardcoded alias
    tuple: Python accepts several spellings for UTF-8 (``utf_8``, ``U8``,
    ``utf``, ``cp65001``, ...), none of which strip a BOM, so a config that
    used one of those would silently reintroduce the bug this guards against.
    """
    if not encoding:
        return "utf-8-sig"
    try:
        if codecs.lookup(encoding).name == "utf-8":
            return "utf-8-sig"
    except LookupError:
        # Unknown encoding name — leave it untouched so the probe's own
        # open() raises the real UnicodeDecodeError/LookupError, matching the
        # main read's behaviour.
        pass
    return encoding


def _raise_on_overflow(
    column: str, original: pd.Series, converted: pd.Series, dtype: str
) -> None:
    """Reject overflow ±inf in a numeric column at cast time.

    ``pd.to_numeric("1e400")`` returns ``+inf`` silently (IEEE float
    overflow). Without this guard the overflowed value lands in MySQL as
    a legitimately-looking number. ``DataValidator`` already runs an inf
    guard at the gate via ``_non_finite_error``; this is defense-in-depth
    on the cast path so a bad token that slips through validation can't
    corrupt the row silently.

    Distinguishes overflow from legitimate missing: a genuinely empty
    cell was ``NaN``/``NA`` in ``original`` *before* coercion, so we
    only flag values that are non-finite in ``converted`` AND were
    present (non-NA) in ``original``.
    """
    # Use np.isinf rather than ~np.isfinite to keep NaN tolerance:
    # legitimate missing cells were NaN in `original` and stay NaN in
    # `converted`; pd.to_numeric never produces NaN from an inf path.
    overflowed = np.isinf(converted) | (np.isnan(converted) & original.notna())
    if overflowed.any():
        offender_rows = original.index[overflowed][:5].tolist()
        raise ValueError(
            f"Column '{column}' (dtype {dtype}) contains "
            f"{int(overflowed.sum())} value(s) that overflow IEEE float "
            f"(±inf / NaN) at "
            f"{redaction.row_refs(offender_rows, int(overflowed.sum()))}. "
            f"``pd.to_numeric`` "
            f"silently converted overflow (e.g. ``1e400``) into ±inf; "
            f"this guard surfaces it at cast time instead of letting it "
            f"reach MySQL."
        )
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


__all__ = ["CSVIngestor"]

# The framework's standard prediction-target column: the user's label_column is
# mapped onto a fixed ``label`` column (database.create_table), and the enriched
# schema flags THAT column role:"target". feature_stats keys the regression-class
# target under the same name so the two channels agree on the target key — a
# consumer can look up feature_stats[label] directly instead of guessing which
# column is the target (see the tracebloc-engine TSF scaler_y seeding).
_TARGET_COLUMN = "label"


def _cast_datetime_strict(series: pd.Series, column: str, dtype: str) -> pd.Series:
    """Cast a CSV column to datetime with the SAME error policy as numeric
    columns: an un-parseable token raises (instead of silently coercing
    to NaT).

    Backend #765 item 3: date cast was ``errors="coerce"`` (silent NULL)
    while numeric cast was ``errors="raise"`` — opposite policies for
    the same bad-input class, adjacent lines. Pick one: both ``raise``,
    because ``DataValidator`` is the gate (catches bad dates with a
    clear per-column error at preflight), and the cast layer trusts
    what passed it. A token that reaches the cast and fails to parse
    means either a validator gap or a schema-mismatch — both surface
    here as a clear ``ValueError`` instead of becoming a silent NULL.

    Genuine missing cells (already ``NaN`` / ``NA`` in the input) stay
    that way: ``pd.to_datetime`` with ``errors="raise"`` treats pre-
    existing NaN as missing (returns ``NaT``), it only raises on a
    *present* value that fails to parse.
    """
    try:
        return pd.to_datetime(series, errors="raise", format="mixed")
    except Exception as exc:
        # Surface the offender per the project convention: name the
        # column, dtype, and a sample of the bad tokens.
        bad_mask = (
            pd.to_datetime(series, errors="coerce", format="mixed").isna()
            & series.notna()
        )
        offender_rows = series.index[bad_mask][:5].tolist()
        # Shape, not content: the FORMAT is the diagnosis for date errors.
        shapes = sorted(
            {redaction.mask_shape(v) for v in series[bad_mask].head(5)}
        )
        raise ValueError(
            f"Column '{column}' (dtype {dtype}) has un-parseable date "
            f"value(s) at {redaction.row_refs(offender_rows, int(bad_mask.sum()))} "
            f"(masked shapes: {shapes}). The cast layer raises on "
            f"un-parseable dates (matching the numeric branch); fix the "
            f"value(s) or declare the column as VARCHAR if the content "
            f"isn't actually a date. (Parser: {type(exc).__name__} — its "
            f"message is suppressed; pandas embeds the raw cell value, "
            f"which must not reach logs, #226.)"
        ) from None


class CSVIngestor(BaseIngestor):
    """A specialized ingestor for CSV files.

    This ingestor extends the BaseIngestor to provide optimized CSV file handling
    using pandas. It includes features for efficient chunked reading, data validation,
    and type conversion.

    Attributes:
        csv_options: Additional options for pandas read_csv
    """

    def __init__(
        self,
        database: Database,
        api_client: APIClient,
        table_name: str,
        schema: Dict[str, str] = {},
        csv_options: Optional[Dict[str, Any]] = None,
        file_options: Optional[Dict[str, Any]] = None,
        unique_id_column: Optional[str] = None,
        label_column: Optional[str] = None,
        intent: Optional[str] = None,
        annotation_column: Optional[str] = None,
        category: Optional[str] = None,
        data_format: Optional[str] = None,
        label_policy: str = label_policy_module.PASSTHROUGH,
        data_id_strategy: str = "uuid",
    ):
        """Initialize CSV Ingestor.

        Args:
            database: Database instance for data storage
            api_client: API client instance for data transmission
            table_name: Name of the target table
            schema: Database schema definition
            csv_options: Additional options for pandas read_csv
            file_options: Additional options for file processing
            unique_id_column: Name of the column to use as unique identifier
            label_column: Name of the column to use as label
            intent: Is the data for training or testing
            annotation_column: Name of the column to use as annotation
            category: Category of the data
            data_format: Format of the data
            label_policy: Bucketing policy for the label value before it's
                sent to the central backend. ``"passthrough"`` for
                classification (default); ``"bucket"`` for regression-class
                tasks so raw target values never leak.
        """
        super().__init__(
            database,
            api_client,
            table_name,
            schema,
            unique_id_column,
            label_column,
            intent,
            annotation_column,
            category,
            data_format,
            file_options,
            label_policy=label_policy,
            data_id_strategy=data_id_strategy,
        )
        self.csv_options = csv_options or {}

        # Per numeric-feature-column sufficient statistics, accumulated in the
        # cast pass (_validate_csv) and emitted on the global-metadata channel
        # for federated/global normalization (data-ingestors#360, backend#1037).
        # Additive aggregates only — no raw values leave the client.
        self._feature_stats_acc: Dict[str, Dict[str, Any]] = {}
        # Row-id and annotation columns are never features — a data_id would
        # pollute normalization. The label column is excluded for CLASSIFICATION
        # tasks (its value is a class, not a numeric feature), but INCLUDED for
        # REGRESSION-CLASS tasks — forecasting / time-to-event / tabular
        # regression — where the target is a numeric column the backend must
        # normalize globally (data-ingestors#360, backend#1037). Only the
        # additive aggregates (count/sum/sum_sq/min/max) ship, under the target's
        # column name; raw per-row target values remain governed by
        # label_policy="bucket". min/max do disclose the two extremes — an
        # accepted trade for enabling federated target normalization, and the
        # only scalers derivable from these stats are Standard/MinMax/MaxAbs
        # (Robust/Quantile/Power need quantiles/λ — a separate follow-up).
        excluded = {c for c in (self.unique_id_column, self.annotation_column) if c}
        if self.label_column and self.category not in REGRESSION_CLASS_CATEGORIES:
            excluded.add(self.label_column)
        self._feature_stats_excluded = excluded

        # Per categorical-feature-column union vocabulary, accumulated in the
        # VARCHAR/CHAR/TEXT cast pass and emitted under the same
        # ``attributes.feature_stats[col].categories`` key the backend folds into
        # the merged dataset's union vocab (backend#1037) and the edge encodes
        # against for a stable cross-client index (tracebloc-engine#455). The
        # label column is always excluded — its value-set is a *class* set gated
        # by ``check_same_labels``, not a feature vocab — as are the row-id and
        # annotation columns. A column whose distinct count crosses
        # ``_MAX_CATEGORICAL_CARDINALITY`` is dropped (free text / id, not a
        # categorical feature). Emitting the value-set discloses category
        # presence — the same accepted trade as feature_stats min/max.
        # Per-value OCCURRENCE COUNTS (not just presence) so a min-count
        # threshold can suppress rare, re-identifying values at finalize
        # (CATEGORICAL_MIN_COUNT).
        self._categorical_acc: Dict[str, Counter] = {}
        self._categorical_over_cap: set = set()
        self._categorical_excluded = {
            c
            for c in (
                self.unique_id_column,
                self.annotation_column,
                self.label_column,
            )
            if c
        }

        # Temporal facts for time-series forecasting (di#360): the timestamp
        # column's timezone (tz-aware iff the parsed dtype carries a tz) and a
        # bounded in-order sample used to infer ``sampling_frequency`` via
        # ``pd.infer_freq``. Accumulated in the TIMESTAMP cast branch.
        self._timestamp_sample: List[Any] = []
        self._timestamp_tz: Optional[str] = None

    def _validate_csv(self, df: pd.DataFrame) -> None:
        """Validate CSV data against schema using pandas functionality.

        This method performs type validation and conversion for the CSV data
        according to the specified schema. It handles common data types including
        integers, floats, booleans, dates, and strings.

        Args:
            df: Pandas DataFrame to validate

        Raises:
            ValueError: If validation fails for any column
        """
        # Only validate columns that exist in both schema and CSV
        common_columns = set(self.schema.keys()) & set(df.columns)

        # Log which schema columns are not in the CSV (for information only)
        missing_columns = set(self.schema.keys()) - set(df.columns)
        if missing_columns:
            # A delimiter mismatch is the usual cause when EVERY schema column
            # reads as "missing": a ;/tab/pipe-delimited file (European Excel
            # export, or a TSV saved as .csv) parses as a single column whose
            # header still contains the real delimiter. Surface that instead of
            # the misleading "every column missing" (#238).
            hint = ""
            if len(df.columns) == 1:
                only = str(df.columns[0])
                for delim, label in (
                    (";", "';' (semicolon)"),
                    ("\t", "a tab"),
                    ("|", "'|' (pipe)"),
                ):
                    if delim in only:
                        hint = (
                            f" The file parsed as a single column — it appears to "
                            f"be delimited by {label}, not a comma. Set the "
                            f"delimiter (csv_options 'delimiter') to match and "
                            f"re-ingest."
                        )
                        break
            raise ValueError(
                f"{RED}Schema columns not present in CSV: "
                f"{', '.join(sorted(missing_columns))}.{hint}{RESET}"
            )

        # Type validation using pandas dtypes - only for columns that exist in the CSV
        for column in common_columns:
            dtype = self.schema[column]
            try:
                if "INT" in dtype.upper():
                    # Reject out-of-int64 values FIRST with a clear message.
                    # On a value beyond int64 pandas reads the column as object
                    # (strings) and ``pd.to_numeric(errors="raise")`` below
                    # throws a cryptic "Integer out of range" (or, on other
                    # pandas versions, the old overflow guard threw numpy's
                    # "ufunc 'isinf' not supported") — #236. The shared check
                    # coerces-then-range-checks so it's object/string-dtype
                    # safe, and it pre-empts the cryptic raise. Same check runs
                    # in DataValidator so preflight and ingest agree.
                    overflow = coercion.int_range_error(df[column], column, dtype)
                    if overflow:
                        raise ValueError(overflow)
                    # Nullable Int64, NOT to_numeric(downcast="integer"): under
                    # the old code any missing cell forced the column to float64,
                    # so 7 round-tripped as "7.0" — silent corruption of every
                    # integer in any INT column that had a single blank cell.
                    # Int64 keeps integers integral and stores missing as pd.NA
                    # (-> SQL NULL); default errors="raise" still surfaces a
                    # genuinely non-numeric value as a clear per-column error.
                    # Coerce-then-check instead of errors="raise": pandas'
                    # own raise message embeds the offending cell value,
                    # which must not reach logs (#226) — and while
                    # DataValidator gates tabular categories first, image-
                    # family categories and category=None runs reach this
                    # cast un-gated.
                    converted = pd.to_numeric(df[column], errors="coerce")
                    non_numeric = converted.isna() & df[column].notna()
                    if non_numeric.any():
                        offender_rows = df.index[non_numeric][:5].tolist()
                        raise ValueError(
                            f"Column '{column}' contains {int(non_numeric.sum())} "
                            f"non-numeric value(s) at "
                            f"{redaction.row_refs(offender_rows, int(non_numeric.sum()))}."
                        )
                    _raise_on_overflow(column, df[column], converted, dtype)
                    df[column] = converted.astype("Int64")
                    self._accumulate_feature_stats(column, df[column])
                elif any(t in dtype.upper() for t in ("FLOAT", "DOUBLE", "DECIMAL", "NUMERIC")):
                    # float64 — NOT downcast='float' (float32), which corrupted
                    # precision: 3.14 -> '3.140000104904175'. Also covers DOUBLE/
                    # DECIMAL/NUMERIC, which previously matched NO branch and let
                    # non-numeric junk flow untouched to the DB. MySQL still
                    # applies the column's own precision/scale on write.
                    #
                    # Coerce-then-detect (NOT errors="raise"): a value with more
                    # than ~15 integer digits is a perfectly valid float (1e26)
                    # but pandas reads it as an object/string column, and
                    # errors="raise" then threw the same cryptic "Integer out of
                    # range" #236 hits on INT. Coercing yields the float; we
                    # surface a genuinely non-numeric token with a clear
                    # per-column message instead of a raw parser error.
                    converted = pd.to_numeric(df[column], errors="coerce")
                    non_numeric = converted.isna() & df[column].notna()
                    if non_numeric.any():
                        offender_rows = df.index[non_numeric][:5].tolist()
                        raise ValueError(
                            f"Column '{column}' contains {int(non_numeric.sum())} "
                            f"non-numeric value(s) at "
                            f"{redaction.row_refs(offender_rows, int(non_numeric.sum()))}."
                        )
                    # inf guard (e.g. an overflow that coerced to ±inf). Safe to
                    # call np.isinf here: `converted` is now a float series, never
                    # the object dtype that made this throw on the raw column.
                    _raise_on_overflow(column, df[column], converted, dtype)
                    df[column] = converted
                    self._accumulate_feature_stats(column, df[column])
                elif "BOOL" in dtype.upper():
                    # Map the textual/numeric boolean forms DataValidator accepts
                    # (true/false, yes/no, t/f, y/n, 1/0) to a nullable boolean
                    # column. df.astype("boolean") alone raises "Need to pass
                    # bool-like values" on those strings — a direct contradiction
                    # with the validator, which blesses them, so a CSV with a
                    # yes/no column passed validation then crashed the ingestor.
                    # Vocabulary lives in coercion (the single source the
                    # validator gate + JSON check read too) so the layers
                    # can't drift. No numeric fallback here by design — only
                    # these exact tokens map to a bool; see coercion.BOOL_*.
                    _truthy = coercion.BOOL_TRUE_STRINGS
                    _falsy = coercion.BOOL_FALSE_STRINGS
                    _norm = df[column].astype("string").str.strip().str.lower()
                    df[column] = _norm.map(
                        lambda x: True if x in _truthy
                        else (False if x in _falsy else pd.NA),
                        na_action="ignore",
                    ).astype("boolean")
                elif "DATETIME" in dtype.upper() or "TIMESTAMP" in dtype.upper():
                    # Full date+time. Checked before DATE/TIME because the
                    # substrings "DATE" and "TIME" both appear in "DATETIME"
                    # (and "TIME" in "TIMESTAMP").
                    df[column] = _cast_datetime_strict(df[column], column, dtype)
                    if column == _TIMESTAMP_COLUMN:
                        self._accumulate_temporal(df[column])
                elif "DATE" in dtype.upper():
                    # DATE only — emit a plain date so the value doesn't gain a
                    # spurious time ('2026-01-02' was becoming '2026-01-02 00:00:00').
                    df[column] = _cast_datetime_strict(df[column], column, dtype).dt.date
                elif "TIME" in dtype.upper():
                    # TIME only — emit a plain time so the value doesn't gain a
                    # spurious (today's) date ('14:30:00' was becoming
                    # '2026-06-08 14:30:00', which MySQL TIME then truncates).
                    df[column] = _cast_datetime_strict(df[column], column, dtype).dt.time
                elif any(t in dtype.upper() for t in ("STRING", "TEXT", "VARCHAR", "CHAR")):
                    # Coerce to pandas StringDtype so missing cells become pd.NA
                    # (not float NaN), then map pd.NA -> Python None so the DB
                    # binder writes SQL NULL. Without this, VARCHAR/CHAR columns
                    # were left as the float64 dtype pandas inferred for an
                    # empty/mixed cell, and str(nan) "nan" landed in MySQL —
                    # silent corruption of missing-data semantics. #167 widened
                    # NULL-tolerance in the validator so all-null VARCHAR no
                    # longer fails validation; this completes the fix on the
                    # write side.
                    df[column] = (
                        df[column].astype("string").astype(object).where(
                            df[column].notna(), None
                        )
                    )
                    self._accumulate_categorical(column, df[column])
            except Exception as e:
                # Our own ValueErrors above are already content-safe; any
                # OTHER exception may embed cell values (pandas/numpy
                # messages do), so report only its type and sever the chain
                # — a logged traceback must not resurrect the content (#226).
                if isinstance(e, ValueError):
                    raise ValueError(
                        f"{RED}Data type validation failed for column "
                        f"{column}: {str(e)}{RESET}"
                    ) from None
                raise ValueError(
                    f"{RED}Data type validation failed for column {column}: "
                    f"{type(e).__name__} (message suppressed — it can embed "
                    f"cell values, #226){RESET}"
                ) from None

    def _accumulate_feature_stats(self, column: str, series: pd.Series) -> None:
        """Fold one chunk's numeric column into the running sufficient stats.

        Called from the INT / FLOAT cast branches of ``_validate_csv`` with the
        already-cast column — so there is no second read of the data. Accumulates
        the five additive aggregates the backend folds into global mean/std/min/
        max (backend#1037): ``count``, ``sum`` (Σx), ``sum_sq`` (Σx²), ``min``,
        ``max``. Missing cells (``pd.NA`` / ``NaN``) are dropped, so ``count`` is
        the non-null count and the aggregates ignore nulls.

        Non-feature columns (label/target, row-id, annotation) are skipped — see
        ``_feature_stats_excluded``. An all-null column contributes nothing and
        never appears in the emitted stats (min/max would be undefined).
        """
        # Match configured excluded names case-/whitespace-insensitively against
        # the actual header via resolve_column (the #340 rule) — accumulation runs
        # on raw CSV headers during _validate_csv, before label pinning, so a
        # config ``Label`` must still exclude a header ``label`` (or a drifted
        # id / annotation column). Mirrors the categorical exclusion below;
        # exact membership here would leak the label/id/annotation into stats.
        if any(resolve_column([column], name) for name in self._feature_stats_excluded):
            return
        vals = series.dropna()
        if vals.empty:
            return
        # Square in float64: an ``Int64`` column squared stays Int64 and a large
        # value would overflow; the aggregates are float sufficient statistics.
        fvals = vals.astype("float64")
        chunk_count = int(len(vals))
        chunk_sum = float(fvals.sum())
        chunk_sum_sq = float((fvals**2).sum())
        # ``.item()`` unwraps the numpy scalar to a native Python int/float, so an
        # INT column reports integer min/max (18, not 18.0) while a float column
        # stays float — and the emitted JSON matches the column's real type.
        chunk_min = vals.min().item()
        chunk_max = vals.max().item()

        acc = self._feature_stats_acc.get(column)
        if acc is None:
            self._feature_stats_acc[column] = {
                "count": chunk_count,
                "sum": chunk_sum,
                "sum_sq": chunk_sum_sq,
                "min": chunk_min,
                "max": chunk_max,
            }
        else:
            acc["count"] += chunk_count
            acc["sum"] += chunk_sum
            acc["sum_sq"] += chunk_sum_sq
            acc["min"] = min(acc["min"], chunk_min)
            acc["max"] = max(acc["max"], chunk_max)

    def feature_stats(self) -> Dict[str, Dict[str, Any]]:
        """The finalized per-numeric-column sufficient statistics for this run.

        Empty when the dataset has no numeric feature columns (e.g. an image
        manifest, or a table of only string/date columns) — the caller then
        omits the field entirely.

        For regression-class tasks the target's stats are re-keyed from its
        original CSV column name to the standardized ``label`` name, so the key
        matches the enriched schema's ``role: "target"`` column and the target
        stored in the DB ``label`` column. Feature columns keep their own names.
        """
        stats = {col: dict(s) for col, s in self._feature_stats_acc.items()}
        # Resolve the configured target against the accumulator keys (actual CSV
        # headers) case-/whitespace-insensitively: the keys use the raw header
        # spelling, which can differ from label_column by case/whitespace, so an
        # exact match would leave the target under the CSV name and break
        # alignment with the schema's role:"target" and backend feature_stats[label].
        resolved_target = (
            resolve_column(stats.keys(), self.label_column)
            if self.label_column
            else None
        )
        if (
            resolved_target
            and self.category in REGRESSION_CLASS_CATEGORIES
            and resolved_target != _TARGET_COLUMN
        ):
            # pop→assign standardizes the key; if a feature were literally named
            # "label" it would collide, but "label" is the framework's own target
            # column name, so a user feature can't legitimately claim it.
            stats[_TARGET_COLUMN] = stats.pop(resolved_target)
        return stats

    def _accumulate_categorical(self, column: str, series: pd.Series) -> None:
        """Fold one chunk's distinct categorical values into the running vocab.

        Called from the VARCHAR/CHAR/TEXT cast branch of ``_validate_csv`` with
        the already-cast column, so there is no second read. Non-null values
        accumulate into a per-column ``Counter`` (occurrence counts, so a
        min-count threshold can drop rare values at finalize); once a column's
        distinct count crosses ``_MAX_CATEGORICAL_CARDINALITY`` it is dropped and
        never re-considered (free text / id, not a categorical feature). Excluded
        columns (label, row-id, annotation) are skipped.

        Exclusion matches the configured names case- and whitespace-insensitively
        via ``resolve_column`` (the #340 rule): this runs during ``_validate_csv``
        on the raw header, before label pinning, so a header spelled ``Label`` /
        ``" label "`` must still exclude a config that says ``label`` — otherwise
        a row-id / label column's raw values would leak into ``feature_stats``.
        """
        if column in self._categorical_over_cap or any(
            resolve_column([column], name) for name in self._categorical_excluded
        ):
            return
        vals = series.dropna()
        if vals.empty:
            return
        acc = self._categorical_acc.setdefault(column, Counter())
        # Vectorised per-chunk counts; Counter.update ADDS them across chunks.
        acc.update(vals.astype(str).value_counts().to_dict())
        if len(acc) > _MAX_CATEGORICAL_CARDINALITY:
            self._categorical_over_cap.add(column)
            self._categorical_acc.pop(column, None)

    def categorical_vocab(self) -> Dict[str, List[str]]:
        """The finalized per-categorical-column union vocabulary (sorted).

        Values seen fewer than ``CATEGORICAL_MIN_COUNT`` times are suppressed —
        a re-identification guard for rare categories (default 1 ⇒ keep all; see
        the config field). A column left with no values is omitted. Empty when
        the dataset has no categorical feature columns within the cardinality
        cap. Sorted so the emitted order is deterministic and the backend's union
        / the edge's label index are stable.
        """
        min_count = self.database.config.CATEGORICAL_MIN_COUNT
        vocab: Dict[str, List[str]] = {}
        for col, counts in self._categorical_acc.items():
            kept = sorted(v for v, n in counts.items() if n >= min_count)
            if kept:
                vocab[col] = kept
        return vocab

    def _accumulate_temporal(self, series: pd.Series) -> None:
        """Fold one chunk of the (already-parsed) timestamp column into the
        temporal accumulators: capture the timezone and grow a bounded in-order
        sample for ``pd.infer_freq``. Rows arrive in time order
        (``TimeOrderedValidator`` gates it), so appending across chunks preserves
        the cadence.
        """
        s = series.dropna()
        if s.empty:
            return
        tz = getattr(s.dt, "tz", None)
        if tz is not None:
            self._timestamp_tz = str(tz)
        if len(self._timestamp_sample) < _MAX_TIMESTAMP_SAMPLE:
            need = _MAX_TIMESTAMP_SAMPLE - len(self._timestamp_sample)
            self._timestamp_sample.extend(s.iloc[:need].tolist())

    def _temporal_attributes(self) -> Dict[str, Any]:
        """Reconcilable temporal facts for time-series forecasting (di#360):
        ``timezone`` (when the timestamps are tz-aware) and ``sampling_frequency``
        (a pandas offset alias, only when a regular cadence is inferable). Both
        are omitted when unavailable — forward-compatible (absent → WARN)."""
        if self.category != TaskCategory.TIME_SERIES_FORECASTING:
            return {}
        attrs: Dict[str, Any] = {}
        if self._timestamp_tz is not None:
            attrs["timezone"] = self._timestamp_tz
        if len(self._timestamp_sample) >= 3:
            try:
                freq = pd.infer_freq(pd.DatetimeIndex(self._timestamp_sample))
            except (ValueError, TypeError):
                freq = None
            if freq:
                attrs["sampling_frequency"] = freq
        return attrs

    def _collect_run_metadata(self) -> Dict[str, Any]:
        """Contribute this run's derived facts under the shared ``attributes``
        namespace on the global-metadata channel (#360).

        Per backend#1037's final ``dataset_meta`` shape, per-column extras live
        under ``attributes.feature_stats`` (``schema`` stays a plain
        ``{column: dtype}`` map): numeric columns carry the folded sufficient
        stats ``{count, sum, sum_sq, min, max}``; categorical columns carry the
        union vocabulary ``{categories: [...]}`` — a column is one or the other,
        so they never collide. Time-series runs also add the reconcilable
        ``timezone`` / ``sampling_frequency`` scalar facts. The base's
        data-format scalar attributes (image ``resolution``, text ``encoding``)
        are folded in via ``super()`` so a CSV manifest for an image/text dataset
        still emits them. Omitted entirely when there is nothing to contribute so
        the payload stays clean.
        """
        # Start from the base's data-format scalar attributes (image resolution,
        # text encoding) so a CSV manifest for an image/text dataset still emits
        # them, then add the tabular/temporal facts this ingestor derives.
        meta = super()._collect_run_metadata()
        attributes = meta.get("attributes", {})

        feature_stats = self.feature_stats()
        for column, categories in self.categorical_vocab().items():
            feature_stats[column] = {"categories": categories}
        if feature_stats:
            attributes["feature_stats"] = feature_stats

        attributes.update(self._temporal_attributes())

        return {"attributes": attributes} if attributes else {}

    def read_data(self, file_path: str) -> Generator[Dict[str, Any], None, None]:
        """Read and validate CSV file using pandas optimizations.

        This method reads the CSV file in chunks for memory efficiency and performs
        validation according to the schema. It uses pandas' optimized C engine for
        better performance.

        Args:
            file_path: Path to the CSV file

        Yields:
            Dict containing record data

        Raises:
            FileNotFoundError: If the CSV file doesn't exist
            ValueError: If the unique_id_column is not found in the CSV
            pd.errors.ParserError: If there's an error parsing the CSV
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"{RED}CSV file not found: {file_path}{RESET}")

        try:
            chunk_size = self.csv_options.pop("chunk_size", 1000)

            # NA handling — PER COLUMN, identical to the validator gate. Every
            # schema column treats ""/NA/null/None as missing (-> NULL);
            # non-schema columns (filename, unique-id, …) are omitted so a file
            # named "NA.jpg" survives. (semseg's mask_id is a DECLARED schema
            # column — backend#816 — so it DOES get this treatment.) The same
            # builder feeds DataValidator's read, so a file can't pass validation
            # and then crash the cast on a token one layer treats as missing and
            # the other as data — the whole-category split (tabular vs other)
            # that caused #237. Used with keep_default_na=False so pandas' global
            # default set never reaches a non-schema column.
            na_values = coercion.build_csv_na_values(self.schema)

            # Pin string-family schema columns to dtype=str so pandas can't infer
            # them numeric and silently strip meaning. An all-digit code column
            # (zip / UniProt accession / zero-padded ID) like "007" is otherwise
            # inferred as int64 at read time and is already 7 by the time the
            # VARCHAR cast in _validate_csv runs -> "7", with the leading zeros
            # gone. na_values are still applied first, so empty/NA cells become
            # NaN (-> SQL NULL) before the str pin; only present values are kept
            # verbatim. dtype keys for columns absent from the file are ignored
            # by pandas, so this is safe when the schema lists more columns than
            # the CSV carries.
            _STRING_TYPES = ("VARCHAR", "CHAR", "TEXT", "STRING")
            _string_schema_cols = {
                col
                for col, t in self.schema.items()
                if isinstance(t, str)
                and t.upper().split("(")[0].strip() in _STRING_TYPES
            }
            # Pandas applies `dtype` keyed by the RAW header literal — before
            # `chunk.columns.str.strip()` runs. If a header carries surrounding
            # whitespace (" code "), keying the dtype dict by the clean schema
            # name ("code") misses, pandas infers numeric, and "007" lands as
            # 7 — the leading zeros silently lost on the very read this pin was
            # meant to prevent. Read the raw header up front (same csv module
            # path we use for duplicate detection) and pin every raw spelling
            # whose stripped form matches a string-family schema column.
            _string_raw_headers = set()
            try:
                _sep_probe = self.csv_options.get(
                    "sep", self.csv_options.get("delimiter", ",")
                )
                with open(
                    file_path,
                    "r",
                    encoding=_bom_safe_encoding(self.csv_options.get("encoding")),
                    newline="",
                ) as _fh:
                    _raw = next(_csv.reader(_fh, delimiter=_sep_probe), [])
                for _h in _raw:
                    if str(_h).strip() in _string_schema_cols:
                        _string_raw_headers.add(_h)
            except (OSError, UnicodeDecodeError, _csv.Error, TypeError, StopIteration):
                # Probe failed; fall back to keying by the schema name only —
                # files without leading/trailing whitespace headers (the
                # common case) still get pinned.
                _string_raw_headers = set(_string_schema_cols)
            else:
                # Always include the bare schema name too, so a file without
                # the whitespace variant still gets pinned cleanly.
                _string_raw_headers |= _string_schema_cols
            string_dtype = {h: str for h in _string_raw_headers}

            # Enhanced default options for pandas
            default_options = {
                # Pin string-family columns to str; let pandas infer the rest
                # (numeric/bool/date columns are coerced explicitly in
                # _validate_csv). None when there are no string columns.
                "dtype": string_dtype or None,
                # keep_default_na=False: NA detection is driven entirely by the
                # per-column na_values dict above, so pandas' global default set
                # can't turn a protected column's "NA" into NULL.
                "keep_default_na": False,
                "na_values": na_values,
                "encoding": "utf-8",
                # Fail loudly on a malformed (ragged) row instead of silently
                # dropping it. A wrong-field-count line almost always signals a
                # real problem (wrong delimiter, an unquoted/embedded comma), and
                # silently shrinking the dataset corrupts it with no signal and a
                # still-green "success". pandas' error names the offending line +
                # field counts. (This also matches _count_records, which reads
                # with pandas' default on_bad_lines='error'.)
                "on_bad_lines": "error",
                "low_memory": False,  # Prevent mixed type inference warnings
                "engine": "c",  # Use faster C engine
            }

            csv_options = {**default_options, **self.csv_options}

            # Reject duplicate column names before pandas silently disambiguates
            # them (a, a -> a, a.1) and the schema mapping then targets the wrong
            # physical column — invisible corruption. Read just the raw header row
            # with the stdlib csv module (NOT pandas) so this is independent of
            # the pd.read_csv path (and the same delimiter/encoding as the main
            # read). csv.reader needs a single-char delimiter; a multi-char/regex
            # sep or a bad encoding falls back to "no header read" (the main read
            # then surfaces the real error).
            _sep = csv_options.get("sep", csv_options.get("delimiter", ","))
            _header = []
            try:
                with open(
                    file_path,
                    "r",
                    encoding=_bom_safe_encoding(csv_options.get("encoding")),
                    newline="",
                ) as _fh:
                    _row = next(_csv.reader(_fh, delimiter=_sep), [])
                _header = [str(h).strip() for h in _row]
            except (OSError, UnicodeDecodeError, _csv.Error, TypeError):
                _header = []
            _dup_headers = sorted({h for h in _header if _header.count(h) > 1})
            if _dup_headers:
                raise ValueError(
                    f"{RED}Duplicate column name(s) in the CSV header: "
                    f"{_dup_headers}. Each column must be unique — otherwise the "
                    f"second is silently renamed '<name>.1' by the parser and the "
                    f"schema maps onto the wrong column. Rename the duplicates and "
                    f"re-ingest.{RESET}"
                )

            first_chunk = True
            for chunk in pd.read_csv(file_path, chunksize=chunk_size, **csv_options):
                # Strip headers + type-convert EVERY chunk. Doing this only for
                # the first chunk left every row past chunk_size (default 1000)
                # un-converted — a DATE column came back as raw strings, numeric
                # columns fell back to pandas' per-chunk inference, and header
                # whitespace was stripped only for chunk 1 — all invisible until
                # a file exceeds a single chunk.
                chunk.columns = chunk.columns.str.strip()
                self._validate_csv(chunk)
                if first_chunk:
                    # One-time check: the unique_id column exists (columns are
                    # identical across chunks, so checking the first is enough).
                    if (
                        self.unique_id_column
                        and self.unique_id_column not in chunk.columns
                    ):
                        raise ValueError(
                            f"{RED}Specified unique_id_column '{self.unique_id_column}' not found in CSV{RESET}"
                        )
                    first_chunk = False

                # Process each row efficiently using itertuples instead of iterrows
                for row in chunk.itertuples(index=False, name=None):
                    record = dict(zip(chunk.columns, row))
                    yield record

        except pd.errors.EmptyDataError:
            # An empty (zero-byte) CSV is a hard input error, not a
            # successful "0 rows" run. Previously this branch logged a
            # WARNING and silently returned an empty generator — the
            # ingestor then proceeded to create an empty MySQL table and
            # called `send_generate_edge_label_meta`, which 400'd with
            # the misleading "No data found for table X" message that
            # blamed the BACKEND instead of the input. (Same misleading
            # cascade #213 traced for self-supervised + label mismatch.)
            # Raise here so DataValidator's existing "No data found to
            # validate" error path surfaces the empty-input cause with a
            # clear, source-truthful message — and no backend round-trip.
            raise ValueError(
                f"{RED}Empty CSV file: {file_path}. The file has no "
                f"header and no rows. Either stage a non-empty CSV at "
                f"this path, or check the cluster-side path (the chart "
                f"mounts your PVC at /data/shared/ — confirm staging "
                f"completed before helm install).{RESET}"
            )

        except (pd.errors.ParserError, Exception):
            raise

    def ingest(self, file_path: str, batch_size: int = 50) -> List[Dict[str, Any]]:
        """Ingest CSV file with progress tracking.

        This method extends the base ingest method to add CSV-specific logging
        and error handling.

        Args:
            file_path: Path to the CSV file
            batch_size: Size of each batch for processing

        Returns:
            List of failed records

        Raises:
            Exception: If ingestion fails
        """
        logger.info(f"Starting CSV ingestion from {file_path}")

        try:
            failed_records = super().ingest(file_path, batch_size)

            logger.info(
                f"CSV ingestion completed. " f"Failed records: {len(failed_records)}"
            )

            return failed_records

        except Exception:
            raise

    def _count_records(self, file_path: str) -> Optional[int]:
        """Count total records in CSV file efficiently using pandas.

        This method provides an optimized way to count records in a CSV file
        using pandas' efficient reading capabilities.

        Args:
            file_path: Path to the CSV file

        Returns:
            Total number of records if countable, None otherwise
        """
        try:
            # Count rows WITHOUT materialising the whole file. The old
            # `pd.read_csv(file_path).shape[0]` loaded every column of every row
            # into memory just to get a count — for a multi-GB dataset that's an
            # OOM (the pod is Killed/137) before ingestion even starts. Read a
            # single column in chunks and sum the lengths: CSV-aware (quoting /
            # embedded newlines handled, unlike a raw line count) and bounded
            # memory. usecols=[0] keeps each chunk to one column.
            total = 0
            for chunk in pd.read_csv(
                file_path, usecols=[0], chunksize=100_000, encoding="utf-8"
            ):
                total += len(chunk)
            return total
        except Exception as e:
            logger.debug(
                f"{YELLOW}Unable to count CSV records using pandas: {str(e)}{RESET}"
            )
            return None
