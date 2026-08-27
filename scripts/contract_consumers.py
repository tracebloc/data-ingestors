"""Refuse a schema-version bump this repo's consumers cannot read.

WHY THIS EXISTS, measured rather than imagined
----------------------------------------------
On 2026-08-27 `layout.v1.json` went from version 2 to 3 (#535). `tracebloc/
e2e-test-agent` reads that file and accepts an explicit set of versions, and it
refuses an unknown one BY DESIGN -- correctly, because generating dataset
layouts from a contract nobody has read is the misattribution its harness exists
to prevent.

The bump landed green here. Over there it turned the REQUIRED `unit` context red
on every open PR, and the journey red with it, for 68 minutes. Nothing connected
the two: a contract published by this repo, consumed by another, with a version
the consumer must explicitly accept -- and no check that the two agree. The
version guard caught it at RUN time; nothing caught it at MERGE time.

This closes that. It is deliberately the smallest thing that could: it does not
validate schemas, does not compare shapes, and has no opinion about what a
version means. It answers one question -- can every consumer read what we are
about to publish? -- and it answers it from both repos' real declarations.

BOTH SIDES ARE DERIVED. NEITHER IS RESTATED HERE.
------------------------------------------------
There is no table in this file pairing a contract with a consumer, and there
must never be one: a hand-written pairing agrees with itself while disagreeing
with reality, which is the defect this check is for.

The consumer states BOTH halves itself. Every module that reads a contract
declares `CONTRACT_RELPATH` (which file it reads) and `SUPPORTED_VERSIONS`
(which versions it accepts) at module level. So the pairing is discovered by
walking the consumer for modules carrying both, and the producer side is the
`version` field in the file that module names. Add a third contract on either
side and this check picks it up with no edit here.

READ FROM THE AST, NOT WITH A REGEX. A commented-out `SUPPORTED_VERSIONS`, or
one built at runtime, must not count as a declaration -- a substring search would
accept both and report agreement with a module that declares nothing.

FAILS CLOSED, INCLUDING ON "I COULD NOT TELL"
---------------------------------------------
Zero discovered pairs is an ERROR, not a pass. Zero parsed pairs compares equal
to zero parsed pairs, so a check that shrugged at an empty result would go green
for ever the day the consumer's layout changed -- which is the exact shape this
whole exercise is about. An unreadable consumer, a contract file the consumer
names but this repo does not publish, and a contract with no `version` field are
all findings for the same reason.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

#: Where a consumer's contract-reading modules live, relative to its repo root.
#: A path, not a module list: the point is to find declarations we were not told
#: about.
CONSUMER_MODULE_DIR = "harness"

#: The two names a consumer module must declare to be a contract consumer.
RELPATH_NAME = "CONTRACT_RELPATH"
VERSIONS_NAME = "SUPPORTED_VERSIONS"


@dataclass(frozen=True)
class ConsumerContract:
    """One consumer module's declared dependency on one contract file."""

    module: str  #: e.g. "harness/layout.py", for the message
    relpath: str  #: the contract file, as the consumer names it
    supported: frozenset  #: the versions it will accept, as strings

    def describe(self) -> str:
        return "{} reads {} and accepts {}".format(
            self.module, self.relpath, sorted(self.supported)
        )


class AgreementError(RuntimeError):
    """The check could not be performed, or was performed and failed."""


def _path_from_expr(node: ast.AST) -> Optional[str]:
    """`Path("a") / "b" / "c.json"` -> `"a/b/c.json"`, or None.

    Handles the joined-Path idiom the consumer actually uses. Anything else --
    a name, a call this does not recognise, an f-string -- returns None rather
    than a guess, and the caller treats that as "not a declaration" rather than
    inventing a path.
    """
    parts: List[str] = []

    def walk(n: ast.AST) -> bool:
        if isinstance(n, ast.BinOp) and isinstance(n.op, ast.Div):
            return walk(n.left) and walk(n.right)
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            parts.append(n.value)
            return True
        if (
            isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "Path"
        ):
            return all(walk(a) for a in n.args)
        return False

    return "/".join(parts) if walk(node) and parts else None


def _versions_from_expr(node: ast.AST) -> Optional[frozenset]:
    """`{"2", "3"}` -> `{"2", "3"}`, or None if it is not a literal set of strings.

    Numbers are coerced to strings so a consumer writing `{2, 3}` is still
    understood -- the producer's `version` field is compared as a string, and a
    type mismatch between the two repos is not the drift this check is about.
    """
    if not isinstance(node, ast.Set):
        return None
    out = set()
    for element in node.elts:
        if not isinstance(element, ast.Constant):
            return None
        if isinstance(element.value, (str, int)):
            out.add(str(element.value))
        else:
            return None
    return frozenset(out) if out else None


def consumer_contracts(consumer_root: Path) -> List[ConsumerContract]:
    """Every contract dependency the consumer declares, from its AST.

    Raises rather than returning empty when the module directory is absent: a
    consumer we cannot read is a finding, and an empty list here would sail
    through the comparison below.
    """
    module_dir = consumer_root / CONSUMER_MODULE_DIR
    if not module_dir.is_dir():
        raise AgreementError(
            "no {}/ directory under {} -- the consumer checkout is missing or "
            "its layout changed. Refusing to report agreement with a repo this "
            "could not read.".format(CONSUMER_MODULE_DIR, consumer_root)
        )

    found: List[ConsumerContract] = []
    for path in sorted(module_dir.glob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError) as exc:
            raise AgreementError(
                "could not parse {}: {}. A consumer module that cannot be read "
                "is a finding, not a skip.".format(path, exc)
            )
        relpath: Optional[str] = None
        supported: Optional[frozenset] = None
        for node in tree.body:  # module level only
            if not isinstance(node, ast.Assign):
                continue
            for target in node.targets:
                if not isinstance(target, ast.Name):
                    continue
                if target.id == RELPATH_NAME:
                    relpath = _path_from_expr(node.value)
                elif target.id == VERSIONS_NAME:
                    supported = _versions_from_expr(node.value)
        if relpath and supported:
            found.append(
                ConsumerContract(
                    module="{}/{}".format(CONSUMER_MODULE_DIR, path.name),
                    relpath=relpath,
                    supported=supported,
                )
            )
    return found


def published_version(producer_root: Path, relpath: str) -> str:
    """The `version` this repo publishes for `relpath`, as a string."""
    path = producer_root / relpath
    if not path.is_file():
        raise AgreementError(
            "a consumer reads {!r}, which this repo does not publish. Either the "
            "file moved (update the consumer in the same change) or the consumer "
            "names a path that never existed.".format(relpath)
        )
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise AgreementError("could not read {}: {}".format(relpath, exc))
    if "version" not in doc:
        raise AgreementError(
            "{} carries no `version` field, but a consumer gates on one. An "
            "absent version is not a passing check -- the consumer would refuse "
            "the file at run time.".format(relpath)
        )
    return str(doc["version"])


def disagreements(
    producer_root: Path, contracts: Sequence[ConsumerContract]
) -> List[str]:
    """Human-readable problems; empty means every consumer can read us."""
    problems: List[str] = []
    for contract in contracts:
        version = published_version(producer_root, contract.relpath)
        if version not in contract.supported:
            problems.append(
                "{}: this repo publishes version {!r}, but {} accepts only {}.\n"
                "    The consumer refuses an unknown version BY DESIGN, so "
                "merging this bump turns its required checks red on every open "
                "PR until it is taught to read {!r}.\n"
                "    Fix order: land the consumer change FIRST (read the diff, "
                "then add the version to its {}), then merge this.".format(
                    contract.relpath,
                    version,
                    contract.module,
                    sorted(contract.supported),
                    version,
                    VERSIONS_NAME,
                )
            )
    return problems


def check(producer_root: Path, consumer_root: Path) -> List[str]:
    """The whole check. Raises `AgreementError` when it cannot be performed."""
    contracts = consumer_contracts(consumer_root)
    if not contracts:
        raise AgreementError(
            "found no contract consumers under {}/{} -- zero parsed pairs "
            "compares equal to zero parsed pairs, so reporting agreement here "
            "would be a check that passes for ever. Either the consumer stopped "
            "declaring {} / {} at module level, or its layout moved.".format(
                consumer_root, CONSUMER_MODULE_DIR, RELPATH_NAME, VERSIONS_NAME
            )
        )
    return disagreements(producer_root, contracts)


def _main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--consumer",
        required=True,
        type=Path,
        help="path to a checkout of the consuming repo",
    )
    parser.add_argument(
        "--producer",
        default=Path("."),
        type=Path,
        help="path to this repo (default: cwd)",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        contracts = consumer_contracts(args.consumer)
        for contract in contracts:
            print("  {}".format(contract.describe()))
        # Flushed before anything reaches stderr: the two streams interleave in
        # a terminal otherwise, and the inventory printed AFTER the failure it
        # explains reads as though it belonged to a different run.
        sys.stdout.flush()
        problems = check(args.producer, args.consumer)
    except AgreementError as exc:
        print("contract-consumer check could not run: {}".format(exc), file=sys.stderr)
        return 2
    if problems:
        print("", file=sys.stderr)
        for problem in problems:
            print("  {}".format(problem), file=sys.stderr)
        return 1
    print("every declared consumer can read every contract this repo publishes")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through CI and tests
    raise SystemExit(_main())
