"""Run-level preflight checks (structural refactor — backend#796, P5a).

Fail-fast guards that run once per ingest, BEFORE any lock / DB / row work,
so a misconfiguration aborts loudly with an actionable message instead of
masquerading as N silent per-row skips or a misleading "No data found".

Extracted verbatim from ``BaseIngestor`` (the god-class decomposition, P5):
these are stateless validations of a single input each, so they live as
module functions rather than methods — ``BaseIngestor`` calls them directly.
The error messages are unchanged (they're pinned by tests).
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

from ..config import Config
from ..utils import redaction
from ..utils.columns import resolve_column
from ..utils.constants import Intent, RED, RESET
from ..utils.csv_dialect import read_dialect_kwargs


def check_src_path(config: Optional[Config] = None) -> None:
    """Fail fast with a clear message when ``config.SRC_PATH`` isn't set
    or doesn't exist (#772 P2).

    ``config`` is the run's resolved Config — ``validate_data`` threads
    ``self.database.config`` so SRC_PATH comes from the resolved ingest.yaml,
    not ``os.environ`` (P4c). A ``None`` default keeps the env-reading path for
    direct callers / tests that monkeypatch env.

    Every file-bearing category resolves its per-row sidecars against
    ``config.SRC_PATH`` (file_transfer.py:105 etc.). If the env var / ConfigMap
    key is empty, ``os.path.join("", "images", "x.jpg")`` returns the relative
    path ``"images/x.jpg"`` — every file lookup fails and the user sees N
    copies of ``Source image not found: images/x.jpg`` blaming the data, when
    the real cause is "SRC_PATH was never set / the PVC wasn't staged".
    """
    src = (config if config is not None else Config()).SRC_PATH
    if not src or not str(src).strip():
        raise RuntimeError(
            f"{RED}SRC_PATH is empty. Set it to the cluster-PVC path where "
            f"your data is staged (e.g. /data/shared/<dataset>/). The "
            f"chart's data-staging recipe (kubectl cp or init-container "
            f"sync) must run before the ingest Job — see "
            f"tracebloc/client/ingestor/README.md.{RESET}"
        )
    if not os.path.isabs(src):
        raise RuntimeError(
            f"{RED}SRC_PATH={src!r} is not an absolute path. The ingestor "
            f"resolves every sidecar file against it via os.path.join, "
            f"and a relative SRC_PATH silently falls through to the "
            f"working directory — every file lookup then fails with a "
            f"misleading 'Source image not found'. Use an absolute path "
            f"(e.g. /data/shared/<dataset>/).{RESET}"
        )
    if not os.path.isdir(src):
        raise RuntimeError(
            f"{RED}SRC_PATH={src!r} does not exist or is not a directory. "
            f"Did the data-staging step (kubectl cp / init-container sync) "
            f"run before the ingest Job? Verify with "
            f"`kubectl exec <pod> -- ls {src}`.{RESET}"
        )


def check_csv_encoding(source: Any) -> None:
    """Fail fast with a clear message on a CSV that isn't valid UTF-8 or
    that contains a NUL byte.

    Every validator reads CSVs as UTF-8 and swallows decode errors into a
    misleading "No data found"; a non-UTF-8 export (e.g. a Latin-1/Windows
    CSV with umlauts) would otherwise crash or mislead. A NUL byte (0x00)
    is sneakier: it IS valid UTF-8 (U+0000) so it slips past the decode
    check, but pandas' C parser silently TRUNCATES the field at the NUL
    (``"a\\x00b"`` -> ``"a"``) — silent corruption (#238). Probe once, up
    front, and reject both.
    """
    if not isinstance(source, (str, Path)):
        return
    path = Path(source)
    if path.suffix.lower() != ".csv" or not path.exists():
        return
    offset = 0
    try:
        with open(path, "r", encoding="utf-8") as fh:
            while True:
                chunk = fh.read(1 << 20)  # 1 MB; decode raises on a bad byte
                if not chunk:
                    break
                nul = chunk.find("\x00")
                if nul != -1:
                    raise ValueError(
                        f"{RED}'{path.name}' contains a NUL byte (0x00) at "
                        f"character {offset + nul}. This is not valid CSV text "
                        f"— the file is likely binary or a corrupt export, and "
                        f"pandas would silently truncate the field at the NUL. "
                        f"Remove the NUL byte(s) and re-ingest.{RESET}"
                    )
                offset += len(chunk)
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"{RED}'{path.name}' is not valid UTF-8 — a non-UTF-8 byte was found at "
            f"byte {exc.start}. Re-save the file as UTF-8 (in Excel: Save As → "
            f"'CSV UTF-8 (Comma delimited)'), then re-ingest.{RESET}"
        ) from exc


def check_time_column(
    source: Any,
    time_column: Optional[str],
    csv_options: Optional[Dict[str, Any]] = None,
) -> None:
    """Fail fast when a configured ``time_column`` names a column that isn't in
    the CSV header (data-ingestors#441).

    The time-series categories order and validate rows by a time column. When
    the config points ``time_column`` at a name that doesn't exist, the
    forecasting validators silently fall back to the fixed ``timestamp`` column
    and the rows land anyway — a silent accept that only surfaces (as undefined
    ordering) much later, in training. Resolve the configured name against the
    actual header once, up front, and reject a miss with the bad name plus the
    columns that DO exist, before any DB writes.

    No-op when ``time_column`` is unset (the category default applies) or the
    source isn't a readable CSV (JSON columns are validated downstream by the
    per-category validators). The header is tokenized with the run's CSV dialect
    (``csv_options``) so a non-comma / BOM manifest that ingests fine resolves
    the same columns the write path does — matching the CSV-reading validators.
    """
    if not time_column:
        return
    if not isinstance(source, (str, Path)):
        return
    path = Path(source)
    if path.suffix.lower() != ".csv" or not path.exists():
        return

    # Lazy import so preflight has no hard pandas dependency for the non-CSV
    # paths (mirrors how the CSV-reading validators import it locally).
    try:
        import pandas as pd
    except ImportError:
        return

    try:
        header = pd.read_csv(path, nrows=0, **read_dialect_kwargs(csv_options))
    except Exception:
        # A malformed / unreadable CSV surfaces with clearer messages through
        # check_csv_encoding and the data validators; don't fail on an
        # unrelated parse error here (nor mask the real cause).
        return

    columns = list(header.columns)
    if resolve_column(columns, time_column) is None:
        raise ValueError(
            f"{RED}time_column '{time_column}' not found in CSV columns: "
            f"{redaction.column_preview(columns)}. Set 'time_column' to one of "
            f"the dataset's columns, or remove it to use the category "
            f"default.{RESET}"
        )


def check_intent(intent: Any) -> None:
    """Fail fast on a missing/invalid ``intent`` before any lock, DB, or
    row work (#234).

    ``intent`` is a single run-wide config value, not per-row data. When it
    was wrong (e.g. a ``"trian"`` typo), ``_map_unique_id`` returned None for
    EVERY record, so every row was silently skipped and — before #234 — the
    run still exited 0 with an empty dataset and a Job marked Succeeded. A
    config error must abort loudly, not masquerade as N per-row skips.
    """
    if not intent or intent not in Intent.get_all_intents():
        raise ValueError(
            f"{RED}Invalid intent {intent!r}. Must be one of "
            f"{Intent.get_all_intents()}. This is a configuration error — "
            f"set 'intent: train' or 'intent: test' in your config.{RESET}"
        )
