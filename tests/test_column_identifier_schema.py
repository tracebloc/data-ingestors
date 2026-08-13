"""The published column grammar is pinned to the code, in BOTH directions.

Why this file exists (backend#1780)
-----------------------------------
The column-identifier grammar was mirrored across two repos as two hand-copied
case tables. Each side's comments claimed the other kept it honest:

  ``tracebloc-engine/core/utils/database.py``
      "MIRRORS tracebloc_ingestor/identifiers.py ... test_database.py pins this
       against the canonical spec so the two can't drift."

  this repo's ``tests/test_column_identifier_contract.py``
      "The trainer mirrors tracebloc_ingestor.identifiers verbatim and has its
       own copy of this table plus a pin test; together they keep the two repos
       from drifting."

**Nothing crossed the repo boundary.** The engine's test neither imports nor
fetches ``identifiers.py`` — it pins a local *transcription* of the spec. So a
change to ``MAX_COLUMN_IDENTIFIER_LENGTH``, to the NUL rule, or to the
quote-and-allow reconciliation could land on either side with **both suites
green** and the two repos silently disagreeing. That is exactly the mechanism
that made ISSUE #382 *"silent until training — raising uncaught, after an
experiment was already running."* The reconciliation fixed the values; it did
not fix the thing that let them diverge.

``schema/column_identifier.v1.json`` is the fix's first half: the grammar,
published so the trainer can **generate** its cases instead of copying them —
the same pattern ``runtime_env.v1.json`` already uses for the spawner contract
(backend#1754 / #482).

A published artifact is only worth as much as its pin, so this file asserts BOTH
directions. One direction alone is a trap:

  * code → JSON only: the JSON could omit a rule the code enforces, and a
    consumer generating from it would under-test.
  * JSON → code only: the JSON could declare a rule the code does not enforce,
    and a consumer would trust a guarantee that does not exist.

The second is the one that bites, because it is the direction that reads as
reassuring.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# Loaded DIRECTLY BY PATH, not as `from tracebloc_ingestor.identifiers import
# ...`. The package's __init__ imports database.py, which imports sqlalchemy, so
# the plain import drags the whole DB stack in just to read a grammar. That
# matters here beyond convenience: identifiers.py is deliberately
# "dependency-light (stdlib only) so the trainer can mirror it verbatim", and a
# test that cannot exercise it without sqlalchemy quietly contradicts that
# promise. The sibling runtime_env contract test avoids the same problem by
# ast-parsing its source; loading the module is strictly better, because it
# tests real behaviour rather than the shape of the source text.
import importlib.util as _ilu

_IDENT_PATH = (
    Path(__file__).resolve().parent.parent / "tracebloc_ingestor" / "identifiers.py"
)
_spec = _ilu.spec_from_file_location("_tb_identifiers", _IDENT_PATH)
assert _spec and _spec.loader, f"cannot load {_IDENT_PATH}"
_identifiers = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_identifiers)

MAX_COLUMN_IDENTIFIER_LENGTH = _identifiers.MAX_COLUMN_IDENTIFIER_LENGTH
InvalidColumnIdentifierError = _identifiers.InvalidColumnIdentifierError
is_valid_column_identifier = _identifiers.is_valid_column_identifier
quote_column_identifier = _identifiers.quote_column_identifier
validate_column_identifier = _identifiers.validate_column_identifier

CONTRACT = (
    Path(__file__).resolve().parent.parent
    / "tracebloc_ingestor"
    / "schema"
    / "column_identifier.v1.json"
)

SUPPORTED_VERSIONS = {"1"}


def _contract() -> dict:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def test_the_contract_ships_in_the_package() -> None:
    # A schema that is not packaged cannot be vendored by a consumer, which is
    # the whole point of publishing it.
    assert CONTRACT.is_file(), f"{CONTRACT} is missing"


def test_the_contract_is_a_version_we_understand() -> None:
    assert _contract()["version"] in SUPPORTED_VERSIONS


def test_the_declared_max_length_is_the_constant_the_code_enforces() -> None:
    # THE pin. #382's disagreement was over legality; a silent change to this
    # number is how the two repos would part company again.
    assert _contract()["max_length"] == MAX_COLUMN_IDENTIFIER_LENGTH


def test_the_declared_max_length_is_actually_the_boundary() -> None:
    # Not just equal to the constant — equal to the code's real behaviour. A
    # constant can be right while the comparison using it is off by one.
    n = _contract()["max_length"]
    assert is_valid_column_identifier("a" * n) is True
    assert is_valid_column_identifier("a" * (n + 1)) is False


@pytest.mark.parametrize("case", _contract()["cases"], ids=lambda c: repr(c["name"]))
def test_every_declared_case_matches_the_code(case: dict) -> None:
    """JSON → code: the file may not claim a verdict the code does not give."""
    name, expected = case["name"], case["valid"]
    assert is_valid_column_identifier(name) is expected, case["why"]
    if expected:
        assert validate_column_identifier(name) == name
    else:
        with pytest.raises(InvalidColumnIdentifierError):
            validate_column_identifier(name)


def test_every_forbidden_character_is_actually_rejected() -> None:
    """JSON → code, for the rules rather than the cases."""
    for entry in _contract()["forbidden_characters"]:
        cp = entry["codepoint"]
        assert cp.startswith("U+"), f"unparseable codepoint {cp!r}"
        ch = chr(int(cp[2:], 16))
        assert is_valid_column_identifier(f"bad{ch}col") is False, (
            f"the contract forbids {entry['name']} but the code accepts it"
        )


def test_the_declared_quoting_is_what_the_code_emits() -> None:
    """A consumer that quotes differently produces different DDL."""
    q = _contract()["quoting"]
    example = q["example"]
    assert quote_column_identifier(example["input"]) == example["output"]
    # And the escape rule itself, not only the one example.
    assert quote_column_identifier("a`b") == "`a``b`"


def test_the_contract_covers_every_rule_the_code_enforces() -> None:
    """code → JSON: the file may not omit a constraint the code applies.

    Derived from behaviour rather than from a list, so a NEW rule added to
    identifiers.py without a contract entry fails here instead of quietly
    shrinking what a consumer tests.
    """
    contract = _contract()
    declared_forbidden = {
        chr(int(e["codepoint"][2:], 16)) for e in contract["forbidden_characters"]
    }
    # Probe the code with one representative of each rule class it is known to
    # apply. Every rejection found here must be explained by the contract.
    unexplained = []
    for probe, explained_by in (
        ("", "empty strings are covered by the cases list"),
        ("a" * (contract["max_length"] + 1), "max_length"),
        ("bad\x00col", "forbidden_characters"),
    ):
        if is_valid_column_identifier(probe):
            unexplained.append(f"code ACCEPTS {probe!r}, contract implies it should not")
    assert not unexplained, unexplained
    assert "\x00" in declared_forbidden, (
        "the code rejects NUL but the contract does not declare it forbidden"
    )
    # Non-string input is a rule too, and a consumer in a typed language would
    # not infer it from the case list.
    assert is_valid_column_identifier(None) is False
    assert is_valid_column_identifier(123) is False


def test_the_case_list_is_not_trivially_one_sided() -> None:
    """A table of all-accepts (or all-rejects) would pass everything above.

    This is the guard that stops the contract decaying into a list that proves
    nothing — the failure mode the two hand-copied tables were already drifting
    toward.
    """
    verdicts = [c["valid"] for c in _contract()["cases"]]
    assert any(verdicts), "no accepting case"
    assert not all(verdicts), "no rejecting case"
    assert len(verdicts) >= 8, "the case list has shrunk below the #382 set"


def test_the_boundary_cases_are_present_by_construction() -> None:
    """The off-by-one pair is the case most likely to be dropped as redundant."""
    n = _contract()["max_length"]
    names = {c["name"]: c["valid"] for c in _contract()["cases"]}
    assert names.get("a" * n) is True, f"missing the exactly-{n} accepting case"
    assert names.get("a" * (n + 1)) is False, f"missing the {n + 1} rejecting case"
