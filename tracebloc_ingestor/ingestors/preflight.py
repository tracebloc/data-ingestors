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
from typing import Any, Optional

from ..config import Config
from ..utils.columns import resolve_column
from ..utils.constants import Intent, RED, RESET


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
    time_column: Optional[str],
    fixed_column: Optional[str],
    category: Optional[str] = None,
) -> None:
    """Reject a configured ``time_column`` that a fixed-time-column category
    will never honor (data-ingestors#441).

    For ``time_series_forecasting`` / ``time_series_classification`` the time
    column is a FIXED physical name (``timestamp``, Decision-2): ordering is
    always by that column and the config ``time_column`` is not consumed. So a
    ``time_column`` pointing anywhere else — a typo like ``nonexistent_col`` OR
    a real-but-wrong column like ``temp_c`` — is silently ignored while the rows
    land ordered by ``timestamp``. That silent accept (the #441 repro, and the
    real-but-wrong subcase saadqbal flagged) is exactly what this rejects, up
    front, before any DB writes: the field is decorative for these categories,
    so validating it against the header would only give false confidence.

    ``fixed_column`` is the category's fixed column (from
    ``registry.FIXED_TIME_COLUMN_BY_CATEGORY``) or ``None`` for categories where
    ``time_column`` IS user-configurable — ``time_to_event_prediction``, whose
    ``time_column`` is validated (exactly) by ``TimeToEventValidator``. This is
    a no-op when either ``time_column`` or ``fixed_column`` is unset, so it never
    touches TTE or the non-time-series categories. The match is case-/whitespace-
    insensitive (``resolve_column``), so ``Timestamp`` is accepted as the fixed
    ``timestamp``.
    """
    if not time_column or not fixed_column:
        return
    if resolve_column([fixed_column], time_column) is None:
        raise ValueError(
            f"{RED}time_column '{time_column}' is not honored for "
            f"{category or 'this category'}: rows are always ordered by the "
            f"fixed '{fixed_column}' column, whose name is set by the platform "
            f"(the top-level 'time_column' is a time_to_event_prediction field). "
            f"Rename your time column to '{fixed_column}' and declare it in the "
            f"schema, or remove the 'time_column' field.{RESET}"
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
