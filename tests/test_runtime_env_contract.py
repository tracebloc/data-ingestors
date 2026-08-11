"""Pins the runtime-env contract the spawner has to satisfy (backend#1752).

WHY THIS EXISTS
---------------
#468 made DB_USER/DB_PASSWORD required by removing the edgeuser fallback. The
credentials that replace them are injected by jobs-manager only when
SERVICE_DB_ACCOUNTS is on, and that flag is off everywhere. Staging edges float
on the `:stg` ingestor channel, so they picked the change up within hours and
every ingestion Job began failing at Config() before reading a byte.

NOTHING COULD HAVE CAUGHT IT, and that is the point. This repo's required
`e2e (real MySQL)` check sets `DB_USER: root` in its own workflow env — it
supplies the credentials whose absence is the bug, so it is structurally
incapable of noticing that no one else supplies them. client-runtime's tests
assert the injection is correctly *gated*. Both halves are individually right;
the pair was never tested.

WHAT THIS FILE BUYS
-------------------
It makes the requirement a PUBLISHED FACT rather than an implementation detail
buried in a property getter, so the spawner can assert against it in its own CI
— the same shape as layout.v1.json, which the CLI reads instead of restating
the layout rules.

On its own this file cannot fail the spawner's build; that is client-runtime's
half (backend#1753). What it does guarantee is that the contract can never
drift from the code, so when the spawner does read it, it is reading the truth.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
CONTRACT = _ROOT / "tracebloc_ingestor" / "schema" / "runtime_env.v1.json"
CONFIG_PY = _ROOT / "tracebloc_ingestor" / "config.py"

SUPPORTED_VERSIONS = {"1"}


def _contract() -> dict:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def _required_from_contract() -> set:
    return {entry["name"] for entry in _contract()["required"]}


def _required_from_code() -> set:
    """Every env var `config.py` passes to `_require_env`, read from the AST.

    Parsed rather than imported: importing the package pulls in sqlalchemy and
    the rest of the runtime, and a contract test should depend on the source it
    is pinning, not on the environment it happens to run in.
    """
    tree = ast.parse(CONFIG_PY.read_text(encoding="utf-8"))
    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        name = getattr(fn, "attr", None) or getattr(fn, "id", None)
        if name != "_require_env" or not node.args:
            continue
        arg = node.args[0]
        assert isinstance(arg, ast.Constant) and isinstance(arg.value, str), (
            "_require_env must be called with a literal env-var name so this "
            "contract can be checked statically; got a computed value"
        )
        found.add(arg.value)
    return found


def test_the_contract_is_a_version_we_understand():
    assert _contract()["version"] in SUPPORTED_VERSIONS


def test_every_required_var_in_the_code_is_declared():
    """The direction that matters.

    Adding a `_require_env` call without declaring it is exactly what happened
    in #468: a new hard requirement on the spawner, invisible to the spawner.
    """
    undeclared = _required_from_code() - _required_from_contract()
    assert not undeclared, (
        f"config.py requires {sorted(undeclared)} but runtime_env.v1.json does "
        "not declare them. Whatever spawns this container has no way to know it "
        "must provide them — which is how backend#1752 broke staging ingestion. "
        "Declare them, and check the spawner actually injects them before "
        "merging."
    )


def test_the_contract_does_not_claim_requirements_the_code_dropped():
    """The other direction, so the contract cannot rot into over-claiming.

    A stale entry would make a spawner inject something pointless forever, and
    would make this file untrustworthy in the direction that matters.
    """
    stale = _required_from_contract() - _required_from_code()
    assert not stale, (
        f"runtime_env.v1.json declares {sorted(stale)} as required but nothing "
        "in config.py requires them any more."
    )


def test_each_entry_says_who_is_supposed_to_provide_it():
    """An entry that does not name its provider is not actionable.

    `provided_by` is what turns this from a list of names into something a
    reviewer can check: it is the sentence that would have made the #468
    ordering hazard obvious at review time.
    """
    for entry in _contract()["required"]:
        for field in ("name", "provided_by", "reason"):
            assert entry.get(field), f"{entry.get('name', entry)} lacks {field!r}"


def test_it_currently_pins_the_two_credentials_1528_made_required():
    # A concrete anchor: if these ever stop being required, that is a
    # deliberate decision and this line should be the thing that notices.
    assert _required_from_contract() == {"DB_USER", "DB_PASSWORD"}
