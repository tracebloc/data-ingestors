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

THERE IS MORE THAN ONE CONSUMER, AND IT IS NOT WRITTEN IN PYTHON (backend#3146)
-------------------------------------------------------------------------------
The first cut enumerated exactly one: `e2e-test-agent`. `tracebloc/cli` VENDORS
`layout.v1.json` byte-for-byte into `internal/schema/` and has, since cli#634,
its own `SupportedLayoutVersions` refusing an unknown version at load. So it is
a consumer with the same failure mode and it sat outside the one check designed
to catch it: this repo could bump the contract, watch this job go green, and
break the CLI.

`cli#634` makes that break LOUD rather than silent -- it panics at init instead
of half-applying a shape it does not understand. It does not make it CAUGHT
BEFORE MERGE, which is a different question and this file's question. The
guard fires in the consumer's CI; without this, the producer still has no way
to know.

That consumer is Go, so `ast` cannot read it. `go_consumer_contracts` derives
BOTH halves from the CLI's own source, on the same principle as above -- no
pairing table:

  * WHICH VERSIONS: the `var SupportedLayoutVersions = map[string]bool{...}`
    literal, parsed structurally. A non-literal (built at runtime) or an entry
    mapped to `false` is not a claim of support.
  * WHICH CONTRACT: the `//go:embed` directive bound to the `[]byte` var that
    the same file unmarshals. Go's own rule -- the directive immediately
    precedes the declaration it embeds -- makes that derivable, so the chain
    `SupportedLayoutVersions` -> `schema.LayoutV1Bytes` -> `layout.v1.json` is
    READ rather than assumed.

WORD BOUNDARIES, NOT SUBSTRINGS, AND HERE IT IS LOAD-BEARING. The CLI embeds
`V1Bytes` (ingest.v1.json) and `LayoutV1Bytes` (layout.v1.json), and `V1Bytes`
is a SUFFIX of `LayoutV1Bytes`. A substring search for the first matches the
second and pairs the layout guard with the ingest schema -- a contract whose
version field it does not even gate on. `\b` does not match between `t` and
`V`, which is why the reference scan uses it.

FAILS CLOSED, INCLUDING ON "I COULD NOT TELL"
---------------------------------------------
Zero discovered pairs is an ERROR, not a pass. Zero parsed pairs compares equal
to zero parsed pairs, so a check that shrugged at an empty result would go green
for ever the day the consumer's layout changed -- which is the exact shape this
whole exercise is about. An unreadable consumer, a contract file the consumer
names but this repo does not publish, and a contract with no `version` field are
all findings for the same reason.

Zero is enforced PER CONSUMER, not over the total. A partial miss -- one probe
finding nothing while its sibling keeps the result non-empty -- is the same
defect one layer up, and it is the shape Bugbot found in the Python probe on
#536. Every DECLARED consumer must also be SUPPLIED: a consumer added to
`DECLARED_CONSUMERS` but not checked out by the workflow is an error, not a
silently shorter list.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

#: Where a consumer's contract-reading modules live, relative to its repo root.
#: A path, not a module list: the point is to find declarations we were not told
#: about.
CONSUMER_MODULE_DIR = "harness"

#: The two names a consumer module must declare to be a contract consumer.
RELPATH_NAME = "CONTRACT_RELPATH"
VERSIONS_NAME = "SUPPORTED_VERSIONS"

#: The Go consumer's spelling of `SUPPORTED_VERSIONS` (cli#634,
#: `internal/push/layout_contract.go`). Deliberately the same concept under the
#: destination language's naming convention rather than a second mechanism.
GO_VERSIONS_NAME = "SupportedLayoutVersions"

#: Where THIS repo publishes its contracts. A producer fact, not a pairing: a
#: Go consumer vendors the file under its own path (`internal/schema/`), so the
#: basename its `//go:embed` names is resolved against our directory to get the
#: file whose `version` we are about to publish. `published_version` then fails
#: closed if the result is not a file we actually publish.
PRODUCER_SCHEMA_DIR = "tracebloc_ingestor/schema"


@dataclass(frozen=True)
class ConsumerContract:
    """One consumer module's declared dependency on one contract file."""

    module: str  #: e.g. "harness/layout.py", for the message
    relpath: str  #: the contract file, as the consumer names it
    supported: frozenset  #: the versions it will accept, as strings
    repo: str = ""  #: e.g. "tracebloc/cli"; "" for the legacy single-repo form
    #: The identifier the fix instruction must tell a reader to widen. Carried
    #: per pair because it is spelled differently per language, and a message
    #: naming `SUPPORTED_VERSIONS` to someone editing Go sends them looking for
    #: a symbol that is not there.
    versions_name: str = VERSIONS_NAME

    @property
    def label(self) -> str:
        """How this pair is named in every message. Repo-qualified once there
        is more than one consumer -- "harness/layout.py" alone stopped being
        unambiguous the moment a second repo declared a supported set."""
        return "{} {}".format(self.repo, self.module) if self.repo else self.module

    def describe(self) -> str:
        return "{} reads {} and accepts {}".format(
            self.label, self.relpath, sorted(self.supported)
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


def consumer_contracts(consumer_root: Path, repo: str = "") -> List[ConsumerContract]:
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
            # BOTH ASSIGNMENT FORMS. Only `ast.Assign` was handled, so an
            # ANNOTATED declaration -- `SUPPORTED_VERSIONS: frozenset = {"2"}`,
            # a shape this codebase already uses elsewhere -- was dropped as if
            # the module declared nothing. The zero-pairs guard does not save
            # that case: it fires only when EVERY module is missed, so one
            # still-parseable sibling kept the check green while the module
            # that actually gates the bumped contract was ignored (Bugbot, #536).
            if isinstance(node, ast.Assign):
                targets = [t for t in node.targets if isinstance(t, ast.Name)]
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                # An annotation with no value (`X: frozenset`) declares nothing.
                targets = [node.target] if node.value is not None else []
            else:
                continue
            for target in targets:
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
                    repo=repo,
                )
            )
    return found


# --------------------------------------------------------------------------
# The Go consumer (`tracebloc/cli`, backend#3146)
# --------------------------------------------------------------------------
#
# `ast` cannot read Go, and pulling in a Go toolchain to run `go/parser` would
# mean this job executes the CONSUMER's code against the consumer's checkout --
# the thing the token scoping on #536 was tightened to avoid. So these read the
# two declarations structurally with anchored patterns, and every shape they do
# not recognise returns None, which the zero-pairs rule turns into a failure.
# "I could not parse it" must never leave the walk quietly shorter.

#: `var SupportedLayoutVersions = map[string]bool{ "4": true }`. `[^{}]*` on the
#: body so a nested brace cannot be silently swallowed -- if the literal ever
#: gains structure this stops matching and the zero-pairs rule fires, which is
#: the correct answer to "this is no longer the shape I know how to read".
_GO_VERSION_MAP = re.compile(
    r"\bvar\s+" + GO_VERSIONS_NAME + r"\s*=\s*map\[\s*string\s*\]\s*bool\s*\{"
    r"(?P<body>[^{}]*)\}"
)

#: One `"4": true,` entry. `\b` after the boolean so `truey` is not read as
#: `true`.
_GO_MAP_ENTRY = re.compile(r'"(?P<key>[^"\\]*)"\s*:\s*(?P<value>true|false)\b\s*,?')

#: `//go:embed layout.v1.json`. Go requires the directive to sit in the comment
#: group immediately preceding the declaration it embeds, which is what makes
#: the binding derivable rather than guessed.
_GO_EMBED_DIRECTIVE = re.compile(r"^\s*//go:embed\s+(?P<pattern>\S+)\s*$")

#: `var LayoutV1Bytes []byte`, the declaration a directive above binds to.
_GO_BYTES_VAR = re.compile(r"^\s*var\s+(?P<name>\w+)\s+\[\]byte")

_GO_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_GO_LINE_COMMENT = re.compile(r"//[^\n]*")


def _go_sources(consumer_root: Path) -> List[Path]:
    """Every non-test Go file in the consumer, sorted.

    Test files are excluded because a test may narrow the supported set to
    prove the guard fires -- `cli#634`'s `TestTheGuardIsWiredIntoTheLoader`
    does exactly that -- and reading that as the CLI's real support set would
    report a disagreement that does not exist.
    """
    return sorted(
        path
        for path in consumer_root.rglob("*.go")
        if not path.name.endswith("_test.go")
        # `vendor/` is a copy of someone else's code, not this repo's claim.
        and "vendor" not in path.relative_to(consumer_root).parts
    )


def _strip_go_comments(src: str) -> str:
    """Comments removed, so a REFERENCE scan cannot match prose.

    `layout_contract.go` names `scripts/sync-schema.sh` and the vendored file
    in its doc comments. Matching an identifier inside a comment would pair the
    guard with whatever a sentence happened to mention.
    """
    return _GO_LINE_COMMENT.sub("", _GO_BLOCK_COMMENT.sub("", src))


def _go_embedded_files(sources: Iterable[Path]) -> Dict[str, str]:
    """`{"LayoutV1Bytes": "layout.v1.json", ...}` across the consumer.

    Raises when one name is bound twice: two answers is not a better answer
    than none, and picking either would be a guess.
    """
    bound: Dict[str, str] = {}
    for path in sources:
        lines = path.read_text(encoding="utf-8").splitlines()
        pending: Optional[str] = None
        for line in lines:
            directive = _GO_EMBED_DIRECTIVE.match(line)
            if directive:
                pending = directive.group("pattern")
                continue
            if pending is None:
                continue
            var = _GO_BYTES_VAR.match(line)
            if var:
                name = var.group("name")
                if name in bound and bound[name] != pending:
                    raise AgreementError(
                        "{} binds `{}` to both {!r} and {!r}. Two answers is "
                        "not a better answer than none.".format(
                            path, name, bound[name], pending
                        )
                    )
                bound[name] = pending
                pending = None
            elif line.strip() and not line.strip().startswith("//"):
                # Go only allows blank lines and further comments between the
                # directive and its declaration. Anything else means this
                # directive does not bind here, so drop it rather than carry
                # it down the file and bind it to an unrelated var.
                pending = None
    return bound


def _go_versions_from_source(src: str) -> Optional[frozenset]:
    """The supported set from a Go map literal, or None if it is not one.

    Only keys mapped to `true` are support. `"5": false` is a Go-idiomatic way
    to write "known about, not supported", and reading it as support would
    invert the answer.
    """
    match = _GO_VERSION_MAP.search(_strip_go_comments(src))
    if not match:
        return None
    body = match.group("body")
    out = set()
    for entry in _GO_MAP_ENTRY.finditer(body):
        if entry.group("value") == "true":
            out.add(entry.group("key"))
    # ANYTHING THE ENTRY PATTERN DID NOT ACCOUNT FOR MEANS THIS IS NOT A SHAPE
    # WE READ. A computed key, a variable value, a helper call -- reporting the
    # entries we happened to understand would be a partial answer presented as
    # a whole one. A set quietly too small reports a disagreement that does not
    # exist; one quietly too large reports agreement that does not. So the
    # remainder after removing every recognised entry must be nothing but
    # whitespace and separators, or this is not a declaration.
    remainder = _GO_MAP_ENTRY.sub("", body)
    if remainder.strip(" \t\r\n,"):
        return None
    return frozenset(out) if out else None


def go_consumer_contracts(
    consumer_root: Path, repo: str = ""
) -> List[ConsumerContract]:
    """Every contract dependency the Go consumer declares, from its source.

    The chain, all three links READ from the consumer:
    `var SupportedLayoutVersions` -> the embedded `[]byte` var that file
    references -> the `//go:embed` filename bound to it.
    """
    if not consumer_root.is_dir():
        raise AgreementError(
            "no checkout at {} -- the consumer checkout is missing. Refusing to "
            "report agreement with a repo this could not read.".format(consumer_root)
        )
    sources = _go_sources(consumer_root)
    if not sources:
        raise AgreementError(
            "no Go sources under {} -- the consumer checkout is empty or its "
            "layout changed. An unreadable consumer is a finding, not a "
            "skip.".format(consumer_root)
        )
    embedded = _go_embedded_files(sources)

    found: List[ConsumerContract] = []
    for path in sources:
        src = path.read_text(encoding="utf-8")
        supported = _go_versions_from_source(src)
        if supported is None:
            continue
        module = path.relative_to(consumer_root).as_posix()
        code = _strip_go_comments(src)
        # WORD BOUNDARIES. `V1Bytes` is a suffix of `LayoutV1Bytes`; a substring
        # search pairs the layout guard with ingest.v1.json. See the module
        # docstring -- this is the one place in the chain where the difference
        # changes the answer rather than merely tidying it.
        referenced = sorted(
            name
            for name in embedded
            if re.search(r"\b{}\b".format(re.escape(name)), code)
        )
        if len(referenced) != 1:
            raise AgreementError(
                "{} declares `{}` but references {} embedded contract(s) "
                "({}). Exactly one is derivable; {} is a guess.".format(
                    module,
                    GO_VERSIONS_NAME,
                    len(referenced),
                    ", ".join(referenced) or "none",
                    "zero" if not referenced else "choosing between them",
                )
            )
        found.append(
            ConsumerContract(
                module=module,
                relpath="{}/{}".format(PRODUCER_SCHEMA_DIR, embedded[referenced[0]]),
                supported=supported,
                repo=repo,
                versions_name=GO_VERSIONS_NAME,
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
                    contract.label,
                    sorted(contract.supported),
                    version,
                    contract.versions_name,
                )
            )
    return problems


@dataclass(frozen=True)
class Consumer:
    """One repo that reads a contract this repo publishes, and how to read it.

    `probe` is the discovery for that repo's language. Adding a consumer is
    adding an entry here AND a checkout in the workflow -- and forgetting the
    second is an error, not a shorter list. See `check_consumers`.
    """

    repo: str  #: "tracebloc/cli", as GitHub names it
    probe: Callable[[Path, str], List[ConsumerContract]]
    declares: str  #: what the probe looks for, for the zero-pairs message


#: EVERY reader of a contract this repo publishes. The enumeration the version
#: bump needs and did not have: data-ingestors#535 bumped v2 -> v3 with only the
#: harness enumerated, and backend#3146 is the same omission for the CLI.
DECLARED_CONSUMERS: Tuple[Consumer, ...] = (
    Consumer(
        repo="tracebloc/e2e-test-agent",
        probe=consumer_contracts,
        declares="{} / {} at module level in {}/*.py".format(
            RELPATH_NAME, VERSIONS_NAME, CONSUMER_MODULE_DIR
        ),
    ),
    Consumer(
        repo="tracebloc/cli",
        probe=go_consumer_contracts,
        declares="`var {}` beside a //go:embed'd contract".format(GO_VERSIONS_NAME),
    ),
)


def check_consumers(
    producer_root: Path, checkouts: Dict[str, Path]
) -> List[ConsumerContract]:
    """Every declared consumer's contracts, or `AgreementError`.

    Two count assertions, because a walk that visits nothing passes exactly as
    quietly as one that finds nothing:

      * every DECLARED consumer must be SUPPLIED. A consumer in
        `DECLARED_CONSUMERS` with no checkout is a run that did not ask the
        question, and that must not read as an answer.
      * every consumer must yield at least ONE pair. Enforced PER CONSUMER, not
        over the total: with two probes, one silently finding nothing while the
        other stays non-empty is exactly the partial miss Bugbot found inside
        the Python probe on #536, one layer up.
    """
    declared = {c.repo for c in DECLARED_CONSUMERS}
    supplied = set(checkouts)
    if declared != supplied:
        raise AgreementError(
            "declared consumers {} but this run was given {}. Missing: {}. "
            "Unexpected: {}. A declared consumer with no checkout is a question "
            "this run did not ask, and an unasked question is not a passing "
            "answer.".format(
                sorted(declared),
                sorted(supplied),
                sorted(declared - supplied) or "none",
                sorted(supplied - declared) or "none",
            )
        )

    contracts: List[ConsumerContract] = []
    for consumer in DECLARED_CONSUMERS:
        found = consumer.probe(checkouts[consumer.repo], consumer.repo)
        if not found:
            raise AgreementError(
                "found no contract consumers in {} at {} -- zero parsed pairs "
                "compares equal to zero parsed pairs, so reporting agreement "
                "here would be a check that passes for ever. Either it stopped "
                "declaring {}, or its layout moved.".format(
                    consumer.repo, checkouts[consumer.repo], consumer.declares
                )
            )
        contracts.extend(found)
    return contracts


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


def _checkout_arg(raw: str) -> Tuple[str, Path]:
    """`tracebloc/cli=.consumer-cli` -> `("tracebloc/cli", Path(...))`.

    Repo-qualified rather than positional: two `--consumer` paths in the wrong
    order would run each probe against the other's checkout, which the probes
    would report as an unreadable consumer -- a confusing red for a real
    mistake. Naming them makes the mismatch impossible instead.
    """
    repo, sep, path = raw.partition("=")
    if not sep or not repo.strip() or not path.strip():
        raise argparse.ArgumentTypeError(
            "expected REPO=PATH (e.g. tracebloc/cli=.consumer-cli), got {!r}".format(
                raw
            )
        )
    return repo.strip(), Path(path.strip())


def _main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--consumer",
        required=True,
        action="append",
        metavar="REPO=PATH",
        type=_checkout_arg,
        help=(
            "a checkout of a consuming repo, as REPO=PATH. Repeat once per "
            "declared consumer: {}".format(
                ", ".join(c.repo for c in DECLARED_CONSUMERS)
            )
        ),
    )
    parser.add_argument(
        "--producer",
        default=Path("."),
        type=Path,
        help="path to this repo (default: cwd)",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        contracts = check_consumers(args.producer, dict(args.consumer))
        for contract in contracts:
            print("  {}".format(contract.describe()))
        # THE COUNT, PRINTED. A walk that visited nothing and a walk that found
        # nothing produce the same silence otherwise, and this job's whole
        # subject is checks that pass without looking.
        print(
            "  {} contract pair(s) across {} declared consumer(s)".format(
                len(contracts), len(DECLARED_CONSUMERS)
            )
        )
        # Flushed before anything reaches stderr: the two streams interleave in
        # a terminal otherwise, and the inventory printed AFTER the failure it
        # explains reads as though it belonged to a different run.
        sys.stdout.flush()
        problems = disagreements(args.producer, contracts)
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
