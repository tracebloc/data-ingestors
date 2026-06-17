"""Label Diversity Validator Module.

Classification categories need at least 2 distinct label values to be
learnable — a single-class dataset is not a classification problem (and
the backend's ``/global_meta/prepare/`` endpoint correctly rejects it
with ``HTTP 400: "Please provide atleast 2 labels."``).

Without a preflight check, a degenerate single-label CSV:
  1. passes per-record validation (every cell is fine in isolation),
  2. inserts every row into MySQL,
  3. dies at backend ``prepare_dataset`` with the message above, and
  4. surfaces to the user as the generic "Backend failed to prepare the
     dataset; it was NOT registered" — the actual cause is buried in a
     preceding log line.

This validator catches the degenerate case at the gate, before any DB
or backend round-trip, and names the actual distinct label(s) found in
the message so the user immediately knows what's wrong with the input.

Skipped for regression-family categories (tabular_regression,
time_series_forecasting, time_to_event_prediction) — those have
continuous targets where uniqueness is meaningless — and for
self-supervised categories (masked_language_modeling) which have no
label column at all.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils import coercion

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class LabelDiversityValidator(BaseValidator):
    """Reject classification datasets with fewer than 2 distinct label values.

    Attributes:
        label_column: CSV column holding the class label. Resolved
            case-insensitively against the actual header.
        min_distinct: Minimum required distinct non-null label values
            (default 2). The backend's contract is exactly 2; the
            parameter is exposed only for future stricter use cases.
        schema: Optional column→SQL-type map. When the label column is a
            schema column, it's read with the same NA / dtype rules
            CSVIngestor and DataValidator apply, so the distinct-label
            count matches what's actually ingested (bugbot #252).
    """

    def __init__(
        self,
        label_column: str = "label",
        min_distinct: int = 2,
        schema: Optional[Dict[str, str]] = None,
        name: str = "Label Diversity Validator",
    ):
        super().__init__(name)
        self.label_column = label_column
        self.min_distinct = min_distinct
        self.schema = schema or {}

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        try:
            df = self._load_data(data)
            if df is None or df.empty:
                # An empty input is the empty-CSV / no-data class —
                # other validators surface that with their own clear
                # messages (CSV ingestor raises at read; DataValidator
                # returns "No data found to validate"). Don't double-
                # report here.
                return self._create_result(
                    is_valid=True,
                    metadata={"rows_checked": 0, "label_column": self.label_column},
                )

            col = self._resolve_column(df, self.label_column)
            if col is None:
                # The label column isn't in the CSV — caller's
                # responsibility to surface (DataValidator or the
                # ingestor will reject). Don't double-report.
                return self._create_result(
                    is_valid=True,
                    warnings=[
                        f"label column '{self.label_column}' not found in CSV; "
                        f"skipping label-diversity check"
                    ],
                    metadata={"label_column": self.label_column},
                )

            # Surface whitespace-collapsable duplicates before counting
            # distinct values (issue #261). A user CSV with values like
            # ``"  A  "`` mixed with ``"A"`` looks fine in a notebook
            # (pandas treats them as distinct, and so do we), inserts
            # into MySQL with both stored verbatim, and trains a model
            # with one extra class the user never intended — silent
            # label-set corruption. Spot the pattern here so the warning
            # reaches the user at preflight, and ingestion strips at the
            # write side (BaseIngestor.process_record) so MySQL only
            # sees the trimmed value.
            warnings: list = []
            raw_distinct = df[col].dropna().unique()
            # Build the strip-collapsed set on string-typed values only
            # (label columns are VARCHAR/CHAR/TEXT in our schema; INT or
            # FLOAT labels — if anyone ever has them — have no whitespace
            # to collapse).
            collapsed: dict = {}
            for v in raw_distinct:
                if isinstance(v, str):
                    stripped = v.strip()
                    collapsed.setdefault(stripped, []).append(v)
                else:
                    collapsed.setdefault(v, []).append(v)
            whitespace_dupes = {
                stripped: variants
                for stripped, variants in collapsed.items()
                if len(variants) > 1
            }
            if whitespace_dupes:
                # Cap the message length — a wholly-messy dataset shouldn't
                # produce a 10kB warning.
                sample = dict(list(whitespace_dupes.items())[:3])
                warnings.append(
                    f"label column '{col}' contains values that differ only "
                    f"in surrounding whitespace and will be stored as "
                    f"separate classes unless cleaned upstream: {sample}. "
                    f"Ingestion strips whitespace from the label column at "
                    f"write time, so MySQL stores the trimmed value — but "
                    f"if you intended these to be DIFFERENT classes, fix "
                    f"the CSV before re-running (see issue #261)."
                )

            # Count distinct AFTER collapsing whitespace duplicates — those
            # land as ONE class in MySQL after the write-side strip, so the
            # validator must use the same number when deciding whether the
            # dataset crosses the min_distinct gate.
            distinct = list(collapsed.keys())
            n = len(distinct)
            if n < self.min_distinct:
                # Show the actual values found, capped — a user with a
                # 50k-row degenerate dataset doesn't need the full list,
                # but the first few values plus the count tell them
                # exactly what's wrong with the input.
                sample = distinct[:5]
                # Surface counts per distinct value to make "all one
                # class" stand out clearly: "{'X': 10}" vs "{'X': 10000}"
                # both clearly read as single-class but the latter gives
                # the user the full row count for free.
                raw_counts = df[col].value_counts(dropna=True).head(5).to_dict()
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"Classification category requires at least "
                        f"{self.min_distinct} distinct label values in column "
                        f"'{col}' (after whitespace stripping); this dataset "
                        f"has {n} distinct value(s): {sample}. Raw value "
                        f"counts: {raw_counts}. If this is intentional "
                        f"(e.g. you have a continuous target), pick a "
                        f"regression-family category like tabular_regression "
                        f"or time_series_forecasting instead."
                    ],
                    metadata={
                        "label_column": col,
                        "distinct_count": n,
                        "value_counts": raw_counts,
                    },
                )

            return self._create_result(
                is_valid=True,
                warnings=warnings or None,
                metadata={
                    "label_column": col,
                    "distinct_count": n,
                },
            )

        except Exception as e:  # noqa: BLE001 — mirror sibling validators
            logger.error(f"Error during label-diversity validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Label-diversity validation error: {str(e)}"],
            )

    def _load_data(self, data: Any) -> Optional[pd.DataFrame]:
        """Load just the label column from CSV (memory-efficient) or pass
        through a DataFrame as-is. Mirrors DataValidator's loader shape but
        only reads the one column it needs.

        Read errors are deliberately NOT swallowed — they propagate to
        ``validate``'s handler, which fails the check. A CSV that can't be
        read must not silently skip the single-label gate this validator
        exists to enforce (bugbot #252, high severity). Only the genuinely
        not-applicable cases — missing file, unsupported suffix, label
        column absent — return ``None``, which ``validate`` treats as a
        benign skip that sibling validators surface.
        """
        if isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, (str, Path)):
            path = Path(data)
            if not path.exists():
                return None
            if path.suffix.lower() == ".csv":
                # Resolve the label column against pandas' OWN header
                # parsing (nrows=0 reads only the header row, cheaply),
                # not a naive ``split(",")``. The old hand-rolled split
                # diverged from pandas on quoted headers / alternate
                # delimiters: it could (a) miss a column pandas does
                # expose, then fall back to a 1-row read and miscount
                # distinct labels — rejecting a diverse dataset (bugbot
                # #252, medium) — or (b) build a ``usecols`` spelling
                # pandas then rejected, erroring the read.
                header_df = pd.read_csv(path, nrows=0, encoding="utf-8")
                actual = self._resolve_column(header_df, self.label_column)
                if actual is None:
                    # Label column genuinely absent — benign skip; the
                    # ingestor / DataValidator report the missing column.
                    return None
                # Load only the label column — for a 50-feature wide CSV
                # (or a multi-GB proteomics panel) we don't need the rest
                # to count distinct labels. Read it with the SAME NA / dtype
                # rules CSVIngestor + DataValidator use so the distinct count
                # agrees with what's ingested (bugbot #252).
                return pd.read_csv(
                    path,
                    usecols=[actual],
                    encoding="utf-8",
                    **self._label_read_kwargs(actual),
                )
            if path.suffix.lower() == ".json":
                # Mirror ``DataValidator._load_data``'s JSON branch (and
                # ``JSONIngestor.read_data``'s contract): both a top-level
                # array of records AND a top-level single dict are accepted —
                # ``pd.read_json(orient="records")`` rejects the single-dict
                # case and would otherwise turn a perfectly valid input into
                # a silent benign-skip that lets a single-label dataset
                # bypass the diversity gate (bugbot, PR #294 — re-opens #251).
                #
                # Use ``json.load`` directly so the loader accepts the same
                # shapes the ingestor and DataValidator do. Genuinely broken
                # JSON / JSONL (mis-extensioned as ``.json``) still benign-
                # skips here: the actionable error belongs to DataValidator's
                # JSONL detection (#263), and surfacing the raw pandas
                # ``Trailing data`` here would just duplicate the noise
                # (#267).
                try:
                    with open(path, "r", encoding="utf-8") as fh:
                        raw = json.load(fh)
                except (ValueError, json.JSONDecodeError, OSError):
                    return None
                # Mirror JSONIngestor.read_data: a bare dict is one record;
                # a top-level array stays as-is.
                if isinstance(raw, dict):
                    raw = [raw]
                elif not isinstance(raw, list):
                    # Top-level scalar / non-object / non-array — not a
                    # records shape. DataValidator surfaces a clear rejection;
                    # benign-skip here.
                    return None
                # Mirror JSONIngestor._iter_validated_records, which drops
                # non-dict elements (so a top-level scalar array doesn't
                # become a 1-column DataFrame that fools the diversity check).
                records = [item for item in raw if isinstance(item, dict)]
                if not records:
                    return None
                return pd.DataFrame(records)
        return None

    def _label_read_kwargs(self, actual: str) -> Dict[str, Any]:
        """``pd.read_csv`` kwargs for the label column that mirror how
        CSVIngestor / DataValidator read it, so the distinct-label count
        matches what's actually ingested (bugbot #252).

        - ``keep_default_na=False`` always: pandas' shifting global NA set
          must not silently turn a literal ``"NA"``/``"null"`` label into a
          missing value here when the ingestor keeps it as a real class.
        - ``na_values``: the full :data:`coercion.NA_SENTINELS` set, but only
          when the label is a *schema* column — the ingestor coerces only
          schema columns; a non-schema classification label keeps
          ``"NA"``/``"null"`` as a genuine class.
        - ``dtype=str``: when the label is a string-family schema column,
          matching the ingestor's string pin so numeric-looking labels
          (``"007"``, ``"1.0"``) aren't collapsed by numeric inference and
          under-counted.
        """
        kwargs: Dict[str, Any] = {"keep_default_na": False}
        schema_type = self._schema_type_for(actual)
        if schema_type is not None:
            kwargs["na_values"] = {actual: list(coercion.NA_SENTINELS)}
            base = str(schema_type).upper().split("(")[0].strip()
            if base in ("VARCHAR", "CHAR", "TEXT", "STRING"):
                kwargs["dtype"] = {actual: str}
        return kwargs

    def _schema_type_for(self, actual: str) -> Optional[str]:
        """Look up the label column's declared SQL type, or ``None`` when the
        label isn't a schema column. Matched case- AND whitespace-insensitively
        so a CSV header like ``" label "`` (which CSVIngestor strips to
        ``label`` on read) still finds its schema entry (bugbot #252)."""
        if actual in self.schema:
            return self.schema[actual]
        target = str(actual).strip().lower()
        normalised = {str(k).strip().lower(): v for k, v in self.schema.items()}
        return normalised.get(target)

    @staticmethod
    def _resolve_column(df: pd.DataFrame, name: str) -> Optional[str]:
        """Return the actual column name matching ``name`` case- AND
        whitespace-insensitively.

        CSVIngestor strips column-name whitespace on read
        (``chunk.columns.str.strip()``), so a header like ``" label "`` is
        ingested as ``label``. Resolving against the raw header without the
        same strip treated the column as missing, skipped the diversity check
        with a warning, and let a single-class CSV pass preflight (bugbot
        #252). Match the strip here so the gate sees the same column the
        ingestor does."""
        if name in df.columns:
            return name
        target = str(name).strip().lower()
        normalised = {str(c).strip().lower(): c for c in df.columns}
        return normalised.get(target)
