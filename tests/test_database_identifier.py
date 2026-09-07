"""Database-name grammar (backend#952) — and its independence from columns.

``DB_NAME`` reaches a ``CREATE DATABASE`` DDL that cannot be parameterised, so
it has to be validated and backtick-quoted. It deliberately does NOT reuse
``quote_column_identifier``: that entry point is the column grammar reconciled
with ``tracebloc-engine`` and pinned to ``schema/column_identifier.v1.json``
(ISSUE #382), so borrowing it would (a) report a ``DB_NAME`` misconfiguration
as a *column* problem to the operator, and (b) couple DB_NAME's legality to a
grammar that moves for unrelated reasons — a customer CSV header.

The constraint sets are identical today. That is measured, not assumed: MySQL
**5.7.44 and 8.0.46** both ACCEPT ``/``, ``\\``, ``.``, backtick, space and
``?`` in a backtick-quoted database name (they are filename-encoded on disk),
and both reject only length — ``ERROR 1059`` at 65 characters. So this grammar
adds no character rejections; doing so would refuse names the server accepts.
"""

import pytest

from tracebloc_ingestor.identifiers import (
    MAX_DATABASE_IDENTIFIER_LENGTH,
    InvalidColumnIdentifierError,
    InvalidDatabaseIdentifierError,
    is_valid_database_identifier,
    quote_database_identifier,
    validate_database_identifier,
)

# Verified against live MySQL 5.7.44 and 8.0.46: every one of these is created
# successfully when backtick-quoted, so the validator must accept them.
SERVER_ACCEPTS = [
    "training_test_datasets",
    "db/other",
    "db\\other",
    "db.other",
    "a`b",
    "db name",
    "x?ssl_disabled=true",
    "Körpergröße",
    "a" * MAX_DATABASE_IDENTIFIER_LENGTH,
]

SERVER_REJECTS = [
    "",
    "a" * (MAX_DATABASE_IDENTIFIER_LENGTH + 1),
    "bad\x00name",
]


@pytest.mark.parametrize("name", SERVER_ACCEPTS)
def test_names_the_server_accepts_are_valid(name):
    assert is_valid_database_identifier(name) is True
    assert validate_database_identifier(name) == name


@pytest.mark.parametrize("name", SERVER_REJECTS)
def test_names_the_server_rejects_are_invalid(name):
    assert is_valid_database_identifier(name) is False
    with pytest.raises(InvalidDatabaseIdentifierError):
        validate_database_identifier(name)


@pytest.mark.parametrize("name", [None, 123, b"bytes"])
def test_non_strings_are_invalid(name):
    assert is_valid_database_identifier(name) is False
    with pytest.raises(InvalidDatabaseIdentifierError):
        validate_database_identifier(name)


@pytest.mark.parametrize(
    ("name", "quoted"),
    [
        ("cust_db", "`cust_db`"),
        ("a`b", "`a``b`"),
        ("a``b", "`a````b`"),
        ("`", "````"),
        ("x?ssl_disabled=true", "`x?ssl_disabled=true`"),
    ],
)
def test_quoting_doubles_backticks(name, quoted):
    assert quote_database_identifier(name) == quoted


def test_the_error_names_db_name_and_never_a_column():
    """The operator's only diagnostic — it must point at the env var they set.

    This validation runs before any connection, so a misleading message sends
    them hunting through CSV headers for a ``DB_NAME`` typo.
    """
    for bad in SERVER_REJECTS:
        with pytest.raises(InvalidDatabaseIdentifierError) as exc:
            validate_database_identifier(bad)
        assert "DB_NAME" in str(exc.value)
        assert "olumn" not in str(exc.value)


def test_the_database_error_is_not_the_column_error():
    # Distinct types, so a caller can handle one without swallowing the other,
    # and neither is a subclass of the other by accident.
    assert not issubclass(InvalidDatabaseIdentifierError, InvalidColumnIdentifierError)
    assert not issubclass(InvalidColumnIdentifierError, InvalidDatabaseIdentifierError)
    assert issubclass(InvalidDatabaseIdentifierError, ValueError)


def test_the_length_limit_is_stated_independently_of_the_column_grammar():
    """The decoupling this module exists for (backend#952).

    ``MAX_DATABASE_IDENTIFIER_LENGTH`` must be its own literal, not an alias of
    the column constant — otherwise loosening the pinned column grammar for a
    customer header silently loosens DB_NAME too, which is the silent-divergence
    class ISSUE #382 was closed to prevent. Asserting the boundary against the
    database constant alone keeps this test honest if the two ever diverge.
    """
    n = MAX_DATABASE_IDENTIFIER_LENGTH
    assert n == 64  # MySQL's identifier limit; ERROR 1059 above it
    assert is_valid_database_identifier("a" * n) is True
    assert is_valid_database_identifier("a" * (n + 1)) is False
