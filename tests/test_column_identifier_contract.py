"""Cross-repo column-identifier contract (ISSUE #382).

This pins the canonical column-name grammar that BOTH the ingestor (here) and
the trainer (``tracebloc-engine`` core/utils/database.py) must agree on. The
trainer mirrors ``tracebloc_ingestor.identifiers`` verbatim and has its own copy
of this table plus a pin test; together they keep the two repos from drifting.

The reconciliation is **quote-and-allow**: a column name is valid iff it can be
safely backtick-quoted for MySQL (non-empty, ≤64 chars, no NUL). Digit-leading,
hyphenated, spaced, dotted and unicode names are all accepted and quoted — only
over-length / empty / NUL names are rejected.

The exact parametrisation called out in the issue is included below. On the
trainer's ``main`` today, the names marked ``accepted`` are REJECTED by its
over-strict ``^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`` regex — that mismatch is the bug
this contract exists to prevent.
"""

import pytest

from tracebloc_ingestor.identifiers import (
    MAX_COLUMN_IDENTIFIER_LENGTH,
    InvalidColumnIdentifierError,
    is_valid_column_identifier,
    quote_column_identifier,
    validate_column_identifier,
)

# The issue's parametrisation, plus the accept/reject verdict under the
# reconciled grammar. (name, should_be_valid)
CONTRACT_CASES = [
    ("123_gene", True),  # digit-leading — quotable
    ("_ok", True),  # leading underscore
    ("feature-1", True),  # hyphen — quotable
    ("col name", True),  # space — quotable
    ("P08254|MMP3", True),  # proteomics pipe header (#184/#185)
    ("Körpergröße", True),  # clinical unicode header (#739)
    ("feature.1", True),  # dot
    ("a" * MAX_COLUMN_IDENTIFIER_LENGTH, True),  # exactly 64 — boundary OK
    ("a" * (MAX_COLUMN_IDENTIFIER_LENGTH + 1), False),  # 65 chars — too long
    ("", False),  # empty
    ("bad\x00col", False),  # NUL — illegal in identifier
]


@pytest.mark.parametrize("name,valid", CONTRACT_CASES)
def test_is_valid_column_identifier_matches_contract(name, valid):
    assert is_valid_column_identifier(name) is valid


@pytest.mark.parametrize("name,valid", CONTRACT_CASES)
def test_validate_column_identifier_matches_contract(name, valid):
    if valid:
        assert validate_column_identifier(name) == name
    else:
        with pytest.raises(InvalidColumnIdentifierError):
            validate_column_identifier(name)


@pytest.mark.parametrize("name", [n for n, ok in CONTRACT_CASES if ok])
def test_valid_names_quote_safely(name):
    """Every accepted name renders to a single backtick-quoted token, with any
    embedded backtick doubled — so quoting alone makes it injection-safe."""
    quoted = quote_column_identifier(name)
    assert quoted.startswith("`") and quoted.endswith("`")
    # Interior backticks are doubled; no lone backtick can break out of the quote.
    assert quoted[1:-1] == name.replace("`", "``")


def test_backtick_header_is_escaped_not_rejected():
    """A header containing a backtick is allowed (quote-and-allow) and escaped
    by doubling — it is never rejected and never breaks out of the identifier."""
    assert is_valid_column_identifier("a`b")
    assert quote_column_identifier("a`b") == "`a``b`"


def test_injection_shaped_header_is_neutralised_by_quoting():
    """A SQL-injection-shaped header is one inert backtick-quoted token, not a
    rejection and not executable SQL — matching the ingestor DDL harness."""
    evil = "x`; DROP TABLE y; --"
    assert quote_column_identifier(evil) == "`x``; DROP TABLE y; --`"
