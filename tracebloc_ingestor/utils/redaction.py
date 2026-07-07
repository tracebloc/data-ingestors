"""Content-safe diagnostics for error messages and logs (#226 Phase 2).

Validator/cast errors used to embed raw cell values ("Sample invalid
values: [...]"). Those strings land in the install log and can egress via
failure reports — a hole in the data-stays-on-prem guarantee. The helpers
here keep errors actionable without carrying content:

- ``row_refs``   — WHERE the offenders are (row indices), which is what a
  customer actually needs to fix their file;
- ``mask_shape`` — the SHAPE of a value (digits → ``#``, letters → ``x``,
  separators kept), for the cases where structure is the diagnosis
  (date-format problems) but content must not leak.

Policy line: cell VALUES never appear in errors/logs; column names, file
names, counts, dtypes, and row indices may (they are dataset structure,
already stored by design). Classification LABEL VALUES are also exempt
where a validator's purpose is label diagnostics (LabelDiversityValidator)
— labels egress by design in the ingest summary's label counts.
"""

from typing import Any, List


def row_refs(indices: List[Any], total: int, limit: int = 5) -> str:
    """``rows [2, 17, 108] (+3 more)`` — 0-based data-row indices."""
    shown = list(indices[:limit])
    more = total - len(shown)
    suffix = f" (+{more} more)" if more > 0 else ""
    return f"rows {shown}{suffix}"


def mask_shape(value: Any, cap: int = 24) -> str:
    """Structure-preserving mask: digits → ``#``, letters → ``x``.

    ``'03/04/2026 10:15'`` → ``'##/##/#### ##:##'`` — the format survives,
    the content does not. Long values are truncated with an ellipsis so a
    pathological cell can't bloat the log.
    """
    s = str(value)
    masked = "".join(
        "#" if ch.isdigit() else ("x" if ch.isalpha() else ch) for ch in s[:cap]
    )
    return masked + ("…" if len(s) > cap else "")


_MYSQL_ERRNO_HINTS = {
    1062: "duplicate key",
    1264: "value out of column range",
    1265: "value rejected by column type",
    1292: "invalid value for column type",
    1366: "incorrect value for column type",
    1406: "data too long for column",
}


def safe_db_error(exc: BaseException) -> str:
    """A loggable summary of a DB error WITHOUT the driver message.

    MySQL driver messages embed cell values ("Duplicate entry 'X' …",
    "Incorrect integer value: 'X' for column …"), and SQLAlchemy appends
    the full statement parameters unless the engine hides them — so the
    raw ``str(e)`` of a DB error is a content leak. The exception class +
    MySQL errno (+ a short hint for the common ones) carries the
    actionable diagnosis.
    """
    orig = getattr(exc, "orig", None)
    errno = getattr(orig, "errno", None) if orig is not None else getattr(exc, "errno", None)
    hint = _MYSQL_ERRNO_HINTS.get(errno)
    if orig is not None and type(orig) is not type(exc):
        name = f"{type(exc).__name__} ({type(orig).__name__})"
    else:
        name = type(exc).__name__
    parts = [name]
    if errno is not None:
        parts.append(f"errno={errno}")
    if hint:
        parts.append(hint)
    return (
        f"{' — '.join(parts)} (driver message suppressed: it can embed "
        f"cell values, #226)"
    )
