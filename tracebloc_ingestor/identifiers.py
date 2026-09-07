"""Canonical column-identifier grammar — the single source of truth.

This module is the authoritative definition of what counts as a valid *column*
identifier across the tracebloc platform. It is deliberately dependency-light
(stdlib only) so the trainer (``tracebloc-engine`` — renamed from
``tracebloc-client``) can mirror it verbatim without taking a dependency on the
rest of this package.

It is published machine-readably as ``schema/column_identifier.v1.json``, pinned
to this module in both directions by ``tests/test_column_identifier_schema.py``.
Consumers should GENERATE their cases from that file rather than transcribing
the table below: two hand-copied copies is what let ISSUE #382's disagreement
stay silent until training (backend#1780).

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

Note on *database* names: ``validate_database_identifier`` /
``quote_database_identifier`` below. They share the quoting mechanic but state
their own constraints and raise their own error type — see the comment on
``MAX_DATABASE_IDENTIFIER_LENGTH`` for why they deliberately do not reuse the
column entry points (backend#952).
"""

# MySQL's maximum identifier length. The one column-name constraint that
# backtick-quoting cannot work around, so both repos hard-fail above it.
MAX_COLUMN_IDENTIFIER_LENGTH = 64

# Restated for *database* names rather than aliased to the column constant. The
# column grammar above is reconciled with tracebloc-engine and pinned to
# schema/column_identifier.v1.json (ISSUE #382); a future loosening there for
# some customer CSV header must not silently loosen DB_NAME validation too, and
# a DB-motivated tightening must not break the trainer contract.
#
# The two constraint sets happen to be identical today, and that is a measured
# claim, not an assumption: MySQL 5.7.44 and 8.0.46 both ACCEPT ``/``, ``\``,
# ``.``, backtick, space and ``?`` in a backtick-quoted database name (they are
# filename-encoded on disk), and both reject only length — error 1059 at 65
# characters. So there is deliberately no extra character rejection here;
# adding one would refuse names the server itself accepts.
MAX_DATABASE_IDENTIFIER_LENGTH = 64


class InvalidColumnIdentifierError(ValueError):
    """Raised when a column name cannot be safely used as a MySQL identifier."""


class InvalidDatabaseIdentifierError(ValueError):
    """Raised when a database name cannot be safely used as a MySQL identifier."""


def _backtick_quote(name: str) -> str:
    """Backtick-quote an already-validated identifier, doubling any backtick."""
    return "`" + name.replace("`", "``") + "`"


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
    return _backtick_quote(name)


def is_valid_database_identifier(name: object) -> bool:
    """Return True if ``name`` is a valid (safely quotable) database identifier."""
    return (
        isinstance(name, str)
        and name != ""
        and len(name) <= MAX_DATABASE_IDENTIFIER_LENGTH
        and "\x00" not in name
    )


def validate_database_identifier(name: str) -> str:
    """Validate a database name, returning it unchanged when valid.

    Worded for the operator who sets ``DB_NAME``: this runs before any
    connection is made, so the message is the only diagnostic they get.
    """
    if not isinstance(name, str) or name == "":
        raise InvalidDatabaseIdentifierError(
            f"Invalid database name {name!r}: DB_NAME must be a non-empty string."
        )
    if "\x00" in name:
        raise InvalidDatabaseIdentifierError(
            f"Invalid database name {name!r}: DB_NAME must not contain a NUL "
            "character."
        )
    if len(name) > MAX_DATABASE_IDENTIFIER_LENGTH:
        raise InvalidDatabaseIdentifierError(
            f"Database name {name!r} is {len(name)} characters, exceeding "
            f"MySQL's {MAX_DATABASE_IDENTIFIER_LENGTH}-character identifier "
            "limit — shorten DB_NAME."
        )
    return name


def quote_database_identifier(name: str) -> str:
    """Return a MySQL backtick-quoted database identifier after validation."""
    validate_database_identifier(name)
    return _backtick_quote(name)
