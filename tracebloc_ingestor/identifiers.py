"""Canonical column-identifier grammar — the single source of truth.

This module is the authoritative definition of what counts as a valid *column*
identifier across the tracebloc platform. It is deliberately dependency-light
(stdlib only) so the trainer (``tracebloc-client``) can mirror it verbatim
without taking a dependency on the rest of this package.

The reconciliation (ISSUE #382)
-------------------------------
Two repos used to disagree on column-name legality, and the disagreement was
silent until training: the ingestor accepts and backtick-quotes arbitrary
headers (proteomics ``P08254|MMP3``, clinical ``Körpergröße``, spaces, dots —
see ``tests/test_i18n_adversarial_csv.py`` / #739 and #184/#185), while the
trainer rejected anything outside ``^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`` — raising
*uncaught*, after an experiment was already running.

We reconciled toward **quote-and-allow**: the ingestor's accepting behaviour is
correct and is the source of truth. A column name is valid iff it can be safely
backtick-quoted for MySQL:

  * it is a non-empty string,
  * at most 64 characters (MySQL's identifier limit — the one constraint
    quoting cannot rescue), and
  * contains no NUL (``\\x00``), which is illegal in a MySQL identifier.

Every other character is permitted and made safe by quoting; backticks are
escaped by doubling (``quote_column_identifier``). This mirrors exactly what
SQLAlchemy emits in the ingestor's ``CREATE TABLE`` DDL.

Note on *table* names: those are stricter and validated separately
(``TableNameValidator`` here, ``_validate_sql_identifier`` in the trainer) —
table names are platform-controlled, columns come from raw user data.
"""

# MySQL's maximum identifier length. The one column-name constraint that
# backtick-quoting cannot work around, so both repos hard-fail above it.
MAX_COLUMN_IDENTIFIER_LENGTH = 64


class InvalidColumnIdentifierError(ValueError):
    """Raised when a column name cannot be safely used as a MySQL identifier."""


def is_valid_column_identifier(name: object) -> bool:
    """Return True if ``name`` is a valid (safely quotable) column identifier."""
    return (
        isinstance(name, str)
        and name != ""
        and len(name) <= MAX_COLUMN_IDENTIFIER_LENGTH
        and "\x00" not in name
    )


def validate_column_identifier(name: str) -> str:
    """Validate a single column name, returning it unchanged when valid.

    Raises ``InvalidColumnIdentifierError`` with an actionable message otherwise.
    """
    if not isinstance(name, str) or name == "":
        raise InvalidColumnIdentifierError(
            f"Invalid column name {name!r}: must be a non-empty string."
        )
    if "\x00" in name:
        raise InvalidColumnIdentifierError(
            f"Invalid column name {name!r}: must not contain a NUL character."
        )
    if len(name) > MAX_COLUMN_IDENTIFIER_LENGTH:
        raise InvalidColumnIdentifierError(
            f"Column name {name!r} is {len(name)} characters, exceeding the "
            f"{MAX_COLUMN_IDENTIFIER_LENGTH}-character database limit — shorten it."
        )
    return name


def quote_column_identifier(name: str) -> str:
    """Return a MySQL backtick-quoted column identifier after validation.

    Backticks in the name are escaped by doubling, matching MySQL's identifier
    quoting rules — so any valid name is rendered injection-safe.
    """
    validate_column_identifier(name)
    return "`" + name.replace("`", "``") + "`"
