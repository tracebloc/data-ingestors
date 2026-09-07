"""The cross-repo contract check works, and can fail.

`scripts/contract_consumers.py` refuses a schema-version bump the consuming repo
cannot read. The CI job that runs it needs a token and a second checkout, so it
cannot be a required check on a fork PR -- which means the LOGIC has to be
covered here, in the required `pytest` context, or the comparison could rot with
nothing able to notice.

That split is the point: CI supplies the real inputs, these supply the hard ones.
A check whose only exercise is the happy path against today's repos is a check
that has never been shown to fail.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.contract_consumers import (  # noqa: E402
    DECLARED_CONSUMERS,
    AgreementError,
    ConsumerContract,
    check,
    check_consumers,
    consumer_contracts,
    disagreements,
    go_consumer_contracts,
    published_version,
)

#: The env var `tests.yml` sets: one `OWNER/REPO=PATH` per real consumer
#: checkout the REQUIRED `pytest` job supplies, newline- or comma-separated.
#: The same `REPO=PATH` spelling as the script's `--consumer` flag, so the two
#: entry points cannot drift into different notations for the same fact.
CHECKOUTS_ENV = "CONTRACT_CONSUMER_CHECKOUTS"

#: The consumers the REQUIRED `pytest` job compares against for real, as
#: distinct from the ones only the unrequired `contract consumers` job reaches.
#:
#: PUBLIC ONLY, and that is a constraint rather than a preference.
#: `actions/checkout` reaches a public repo with the default read-only
#: `github.token`, which a fork PR DOES receive; a private consumer needs an App
#: token, which GitHub withholds from forks. Listing `e2e-test-agent` here would
#: turn the required context red on every external contribution -- the exact
#: asymmetry #536 split the two jobs over. It stays with the agreement job.
REAL_CONSUMERS_IN_THE_REQUIRED_JOB = ("tracebloc/cli",)

#: The Makefile target `tests.yml` invokes to select the marker below.
REAL_CONSUMERS_TARGET = "real-consumers"

#: The marker that keeps this off a developer machine WITHOUT a skip.
REAL_CONSUMERS_MARKER = "real_consumers"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


#: `REPO=PATH` entries are separated by a comma or a newline, so the workflow
#: can write one per line and a shell can pass them inline.
_CHECKOUT_SEPARATOR = re.compile(r"[,\n]")


def _parse_checkouts(raw: str, source: str) -> Dict[str, str]:
    """`"tracebloc/cli=.consumers/cli"` -> `{"tracebloc/cli": ".consumers/cli"}`.

    ONE parser, used for both the environment variable at run time and the
    workflow's own `env:` block when the suite reads it back. Two spellings of
    the same format is how the two copies come to disagree about an entry
    neither side thinks is malformed.

    Every unreadable entry RAISES. "I could not read this" is a third state and
    it belongs with the failures, not folded into the empty dict that means
    "nobody supplied anything" -- those two have different fixes, and a caller
    that cannot tell them apart will pick the wrong one.
    """
    out: Dict[str, str] = {}
    for entry in (e.strip() for e in _CHECKOUT_SEPARATOR.split(raw)):
        if not entry:
            continue
        repo, sep, path = entry.partition("=")
        if not sep or not repo.strip() or not path.strip():
            raise AssertionError(
                f"{source} entry {entry!r} is not REPO=PATH "
                f"(e.g. tracebloc/cli=.consumers/cli)"
            )
        if repo.strip() in out:
            raise AssertionError(
                f"{source} names {repo.strip()!r} twice; two answers is not a "
                f"better answer than none"
            )
        out[repo.strip()] = path.strip()
    return out


def _supplied_checkouts() -> Dict[str, Path]:
    """`CONTRACT_CONSUMER_CHECKOUTS` parsed, or `{}` when it is unset."""
    return {
        repo: Path(path)
        for repo, path in _parse_checkouts(
            os.environ.get(CHECKOUTS_ENV, ""), CHECKOUTS_ENV
        ).items()
    }


def _consumer(tmp_path: Path, **modules: str) -> Path:
    """A fake consumer checkout: `harness/<name>.py` per keyword."""
    harness = tmp_path / "harness"
    harness.mkdir(parents=True, exist_ok=True)
    for name, body in modules.items():
        (harness / f"{name}.py").write_text(body, encoding="utf-8")
    return tmp_path


def _producer(tmp_path: Path, relpath: str, doc: object) -> Path:
    """A fake producer checkout publishing one contract file."""
    path = tmp_path / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc), encoding="utf-8")
    return tmp_path


MODULE = """
from pathlib import Path
CONTRACT_RELPATH = Path("tracebloc_ingestor") / "schema" / "layout.v1.json"
SUPPORTED_VERSIONS = {"2"}
"""


class TestItDiscoversTheParingRatherThanBeingToldIt:
    def test_a_module_declaring_both_names_is_found(self, tmp_path):
        root = _consumer(tmp_path, layout=MODULE)
        found = consumer_contracts(root)
        assert len(found) == 1
        assert found[0].relpath == "tracebloc_ingestor/schema/layout.v1.json"
        assert found[0].supported == frozenset({"2"})

    def test_several_modules_each_yield_their_own_contract(self, tmp_path):
        other = MODULE.replace("layout.v1.json", "runtime_env.v1.json").replace(
            '{"2"}', '{"1"}'
        )
        root = _consumer(tmp_path, layout=MODULE, runtime_env=other)
        assert {c.relpath for c in consumer_contracts(root)} == {
            "tracebloc_ingestor/schema/layout.v1.json",
            "tracebloc_ingestor/schema/runtime_env.v1.json",
        }

    def test_a_module_with_only_one_of_the_two_names_is_not_a_consumer(self, tmp_path):
        # Half a declaration is not a dependency this check can verify, and
        # guessing the other half is how a pairing table gets born.
        only_path = (
            'from pathlib import Path\nCONTRACT_RELPATH = Path("a") / "b.json"\n'
        )
        only_versions = 'SUPPORTED_VERSIONS = {"1"}\n'
        root = _consumer(tmp_path, a=only_path, b=only_versions)
        assert consumer_contracts(root) == []

    def test_an_ANNOTATED_declaration_is_found_too(self, tmp_path):
        # Only `ast.Assign` was handled, so `SUPPORTED_VERSIONS: frozenset = {...}`
        # -- a shape this codebase uses elsewhere -- was dropped as if the module
        # declared nothing. The zero-pairs guard does NOT cover that: it fires
        # only when every module is missed, so one still-parseable sibling kept
        # the check green while the module gating the bumped contract was
        # ignored (Bugbot, #536).
        annotated = (
            "from pathlib import Path\n"
            "from typing import FrozenSet\n"
            'CONTRACT_RELPATH: Path = Path("tracebloc_ingestor") / "schema" / "layout.v1.json"\n'
            'SUPPORTED_VERSIONS: FrozenSet[str] = {"2"}\n'
        )
        root = _consumer(tmp_path, annotated=annotated)
        found = consumer_contracts(root)
        assert len(found) == 1
        assert found[0].supported == frozenset({"2"})

    def test_a_module_missed_while_a_SIBLING_parses_is_still_caught(self, tmp_path):
        # The precise hole: zero-pairs cannot see a PARTIAL miss. With one plain
        # and one annotated module, dropping the annotated one leaves a non-empty
        # result -- green, while the contract it gates goes unchecked.
        plain = MODULE.replace("layout.v1.json", "runtime_env.v1.json")
        annotated = (
            "from pathlib import Path\n"
            'CONTRACT_RELPATH: Path = Path("tracebloc_ingestor") / "schema" / "layout.v1.json"\n'
            'SUPPORTED_VERSIONS: frozenset = {"2"}\n'
        )
        root = _consumer(tmp_path, runtime_env=plain, layout=annotated)
        paths = {c.relpath for c in consumer_contracts(root)}
        assert paths == {
            "tracebloc_ingestor/schema/layout.v1.json",
            "tracebloc_ingestor/schema/runtime_env.v1.json",
        }, f"an annotated module was dropped while its sibling kept the walk non-empty: {paths}"

    def test_a_bare_annotation_with_no_value_declares_nothing(self, tmp_path):
        # `SUPPORTED_VERSIONS: set` states a type, not a value. Treating it as a
        # declaration would invent a supported set nobody wrote.
        root = _consumer(
            tmp_path,
            layout=(
                "from pathlib import Path\n"
                "CONTRACT_RELPATH: Path\n"
                "SUPPORTED_VERSIONS: set\n"
            ),
        )
        assert consumer_contracts(root) == []

    def test_a_commented_out_declaration_does_not_count(self, tmp_path):
        # THE REASON THIS READS THE AST. A substring search would accept this
        # module and report agreement with something that declares nothing.
        root = _consumer(
            tmp_path,
            layout=(
                "from pathlib import Path\n"
                'CONTRACT_RELPATH = Path("tracebloc_ingestor") / "schema" / "layout.v1.json"\n'
                '# SUPPORTED_VERSIONS = {"2"}\n'
            ),
        )
        assert consumer_contracts(root) == []

    def test_a_runtime_built_version_set_does_not_count(self, tmp_path):
        # Not a literal, so this check cannot know what it will hold. Reporting
        # a guess would be worse than reporting nothing -- and reporting nothing
        # is itself caught, by the zero-pairs rule below.
        root = _consumer(
            tmp_path,
            layout=(
                "from pathlib import Path\n"
                'CONTRACT_RELPATH = Path("a") / "b.json"\n'
                "SUPPORTED_VERSIONS = set(_load_versions())\n"
            ),
        )
        assert consumer_contracts(root) == []


class TestItFailsClosed:
    def test_a_missing_consumer_directory_raises_rather_than_returning_empty(
        self, tmp_path
    ):
        with pytest.raises(AgreementError) as caught:
            consumer_contracts(tmp_path / "nowhere")
        assert "missing" in str(caught.value)

    def test_zero_discovered_pairs_is_an_error_not_a_pass(self, tmp_path):
        # THE HEADLINE RULE. Zero parsed pairs compares equal to zero parsed
        # pairs, so a check that shrugged here would go green for ever the day
        # the consumer's layout moved -- which is the exact defect class this
        # whole thing exists for.
        consumer = _consumer(tmp_path / "c", empty="x = 1\n")
        producer = _producer(tmp_path / "p", "a/b.json", {"version": 1})
        with pytest.raises(AgreementError) as caught:
            check(producer, consumer)
        assert "zero parsed pairs" in str(caught.value)

    def test_an_unparseable_consumer_module_is_a_finding(self, tmp_path):
        root = _consumer(tmp_path, broken="def (:\n")
        with pytest.raises(AgreementError):
            consumer_contracts(root)

    def test_a_contract_the_consumer_names_but_we_do_not_publish_is_a_finding(
        self, tmp_path
    ):
        producer = _producer(tmp_path / "p", "other.json", {"version": 1})
        with pytest.raises(AgreementError) as caught:
            published_version(producer, "tracebloc_ingestor/schema/layout.v1.json")
        assert "does not publish" in str(caught.value)

    def test_a_contract_with_no_version_field_is_a_finding(self, tmp_path):
        # An absent version is not a passing check: the consumer gates on one
        # and would refuse the file at run time.
        producer = _producer(tmp_path / "p", "a/b.json", {"tasks": {}})
        with pytest.raises(AgreementError) as caught:
            published_version(producer, "a/b.json")
        assert "no `version` field" in str(caught.value)


class TestTheComparison:
    def test_a_version_the_consumer_accepts_is_no_problem(self, tmp_path):
        consumer = _consumer(tmp_path / "c", layout=MODULE)
        producer = _producer(
            tmp_path / "p", "tracebloc_ingestor/schema/layout.v1.json", {"version": 2}
        )
        assert check(producer, consumer) == []

    def test_a_version_the_consumer_refuses_is_reported_with_the_fix_order(
        self, tmp_path
    ):
        consumer = _consumer(tmp_path / "c", layout=MODULE)
        producer = _producer(
            tmp_path / "p", "tracebloc_ingestor/schema/layout.v1.json", {"version": 3}
        )
        problems = check(producer, consumer)
        assert len(problems) == 1
        # The message has to say what to do, not just that something is wrong:
        # the fix is ordered (consumer first) and a reader who does not know
        # that will merge these in the order that causes the outage.
        assert "land the consumer change FIRST" in problems[0]
        assert "harness/layout.py" in problems[0]

    def test_the_version_is_compared_as_a_string_either_way_round(self, tmp_path):
        # The producer writes `3` as JSON number; a consumer may write "3" or 3.
        # A type mismatch between two repos is not the drift this check is for,
        # and letting it fail here would produce a confusing red for a real pair.
        numeric = MODULE.replace('{"2"}', "{2, 3}")
        consumer = _consumer(tmp_path / "c", layout=numeric)
        producer = _producer(
            tmp_path / "p", "tracebloc_ingestor/schema/layout.v1.json", {"version": 3}
        )
        assert check(producer, consumer) == []

    def test_one_bad_pair_among_several_is_still_reported(self, tmp_path):
        # BOTH must disagree for this to test what it claims. The first cut
        # published `2` for the second contract -- which its consumer accepts --
        # so the assertion failed for a reason unrelated to the behaviour under
        # test. A fixture that quietly agrees is the fixture bug that makes a
        # "we report all of them" claim untested.
        other = MODULE.replace("layout.v1.json", "runtime_env.v1.json")
        consumer = _consumer(tmp_path / "c", layout=MODULE, runtime_env=other)
        producer = tmp_path / "p"
        _producer(producer, "tracebloc_ingestor/schema/layout.v1.json", {"version": 9})
        _producer(
            producer, "tracebloc_ingestor/schema/runtime_env.v1.json", {"version": 5}
        )
        problems = check(producer, consumer)
        assert len(problems) == 2, "each disagreeing pair should be named"
        assert any("layout.v1.json" in p for p in problems)
        assert any("runtime_env.v1.json" in p for p in problems)


class TestItWouldHaveCaughtTheIncidentItWasWrittenFor:
    """The mutation that matters: replay 2026-08-27 and watch it go red.

    `layout.v1.json` went to version 3 while the consumer accepted only `{"2"}`.
    That turned a REQUIRED context red on every open PR in the consuming repo for
    68 minutes. A check written after an incident that would not have caught the
    incident is decoration.
    """

    def test_the_real_pairing_reddens_on_the_real_bump(self, tmp_path):
        consumer = _consumer(tmp_path / "c", layout=MODULE)  # accepts {"2"}
        producer = _producer(
            tmp_path / "p", "tracebloc_ingestor/schema/layout.v1.json", {"version": 3}
        )  # what #535 published
        problems = check(producer, consumer)
        assert problems, "the check would NOT have caught the outage it exists for"
        assert "'3'" in problems[0] and "'2'" in problems[0]

    def test_and_goes_green_once_the_consumer_is_taught_to_read_it(self, tmp_path):
        # The other direction, so the test above is not passing for a reason
        # unrelated to the version -- #290 is what made this true in reality.
        fixed = MODULE.replace('{"2"}', '{"2", "3"}')
        consumer = _consumer(tmp_path / "c", layout=fixed)
        producer = _producer(
            tmp_path / "p", "tracebloc_ingestor/schema/layout.v1.json", {"version": 3}
        )
        assert check(producer, consumer) == []


class TestTheRealReposAgreeToday:
    """Run against the actual files in this repo, not a fixture.

    The fixtures above prove the logic; this proves the logic is pointed at
    something real.

    HOW IT USED TO ANSWER THAT, AND WHY IT NEVER DID (backend#3338). The real
    comparison looked for the consumer BESIDE this repo::

        consumer = Path(__file__).resolve().parents[2] / "e2e-test-agent"
        if not (consumer / "harness").is_dir():
            pytest.skip("no e2e-test-agent checkout beside this repo")

    and the class docstring said "CI provides one". CI never did. In the
    required `pytest` job `parents[1]` is `$GITHUB_WORKSPACE`
    (`/home/runner/work/data-ingestors/data-ingestors`), so `parents[2]` is
    `/home/runner/work/data-ingestors` -- a directory GitHub creates containing
    exactly the one checkout. `tests.yml` has a single `actions/checkout` and no
    `path:`, so nothing can ever be a sibling; the job that DOES check a
    consumer out puts it at `.consumer`, INSIDE the workspace, and never runs
    pytest. The two halves could not meet, and the test and the workflow were
    added in the SAME commit -- so the hatch fired on its first run and every
    run after it. Measured: `1 skipped` on every sampled `tests.yml` run since,
    with the skip reason phrased as a local-developer convenience.

    It had a second wrong answer too. When a sibling checkout DID exist, it was
    whatever the developer happened to have -- pinned to nothing. A checkout 63
    commits behind its own `develop` produced a red for a disagreement the real
    consumer does not have, while the workflow one directory over insists "PIN
    THE PRODUCER TO THE REF THE CONSUMER ACTUALLY READS".

    So the inputs are now NAMED by the job that supplies them
    (`CONTRACT_CONSUMER_CHECKOUTS`), and every way of not having them is a
    failure with a cause rather than a pass. Absent in CI is red. Absent
    locally is a DESELECTED marker, which prints a count and claims nothing --
    see `pytest.ini`.
    """

    def test_every_contract_this_repo_publishes_parses_and_carries_a_version(self):
        root = _repo_root()
        schemas = sorted((root / "tracebloc_ingestor" / "schema").glob("*.json"))
        assert schemas, "no schema files found — the layout moved"
        for path in schemas:
            doc = json.loads(path.read_text(encoding="utf-8"))
            assert isinstance(doc, dict), f"{path.name} is not an object"

    @pytest.mark.real_consumers
    def test_the_checked_out_consumers_can_read_us(self):
        root = _repo_root()
        supplied = _supplied_checkouts()

        # (1) THE RUN MUST HAVE BEEN GIVEN SOMETHING. An unset variable is
        # "nobody supplied a consumer", which is precisely the state the old
        # `skip` answered with a pass. There is no fallback to fall back to.
        assert supplied, (
            f"{CHECKOUTS_ENV} is unset or empty. This test compares against a "
            f"REAL consumer checkout and has no default: it is selected only by "
            f"`make {REAL_CONSUMERS_TARGET}`, which tests.yml runs with the "
            f"variable set. A run that was given no consumer could not ask the "
            f"question, and that must not read as an answer."
        )

        # (2) THE COUNT, CROSS-CHECKED AGAINST TWO OTHER SOURCES. The env var
        # alone is self-consistent -- comparing it only with itself would let a
        # run that supplied one of two consumers pass as thoroughly as one that
        # supplied both. So the literal claim above and the checkouts tests.yml
        # actually performs are both parsed, and all three must agree on the
        # repos AND on the paths.
        claimed = set(REAL_CONSUMERS_IN_THE_REQUIRED_JOB)
        in_workflow = _real_consumer_checkouts_in_tests_workflow()
        assert set(supplied) == claimed, (
            f"{CHECKOUTS_ENV} supplied {sorted(supplied)}, but the required job "
            f"claims to compare {sorted(claimed)}. Missing: "
            f"{sorted(claimed - set(supplied)) or 'none'}; unexpected: "
            f"{sorted(set(supplied) - claimed) or 'none'}."
        )
        assert supplied == {r: Path(p) for r, p in in_workflow.items()}, (
            f"{CHECKOUTS_ENV} ({ {r: str(p) for r, p in supplied.items()} }) "
            f"does not match the checkouts tests.yml performs ({in_workflow}). "
            f"A variable pointing somewhere the workflow does not check out is "
            f"an unreadable path, not a comparison."
        )
        assert len(supplied) == len(REAL_CONSUMERS_IN_THE_REQUIRED_JOB)

        # (3) EVERY SUPPLIED CHECKOUT MUST BE READABLE AND MUST YIELD PAIRS,
        # per consumer rather than over the total: one probe silently finding
        # nothing while a sibling keeps the list non-empty is the partial miss
        # Bugbot found inside the Python probe on #536.
        by_repo = {c.repo: c for c in DECLARED_CONSUMERS}
        contracts: List[ConsumerContract] = []
        compared: List[str] = []
        for repo in sorted(supplied):
            path = supplied[repo]
            assert repo in by_repo, (
                f"{repo} is supplied to this test but is not in "
                f"DECLARED_CONSUMERS, so nothing knows how to read it"
            )
            assert path.is_dir(), (
                f"{CHECKOUTS_ENV} names {repo} at {path}, which is not a "
                f"directory. The checkout did not happen -- that is a failure "
                f"of this run, not a reason to report agreement."
            )
            found = by_repo[repo].probe(path, repo)
            assert found, (
                f"read {repo} at {path} and found no contract declarations "
                f"({by_repo[repo].declares}). Zero parsed pairs compares equal "
                f"to zero parsed pairs, so this would pass for ever."
            )
            contracts.extend(found)
            compared.append(repo)

        assert len(compared) == len(REAL_CONSUMERS_IN_THE_REQUIRED_JOB), (
            f"compared {compared}, expected "
            f"{list(REAL_CONSUMERS_IN_THE_REQUIRED_JOB)}"
        )
        assert len(contracts) >= len(compared), (
            f"{len(contracts)} contract pair(s) across {len(compared)} "
            f"consumer(s) -- fewer pairs than consumers means one was walked "
            f"and produced nothing"
        )

        # (4) And only now the question the test is named for.
        assert disagreements(root, contracts) == []


def _tests_workflow_steps() -> List[dict]:
    """The steps of the required `pytest` job, from tests.yml."""
    import yaml

    parsed = yaml.safe_load((_repo_root() / ".github/workflows/tests.yml").read_text())
    return list(parsed["jobs"]["pytest"]["steps"])


def _real_consumer_checkouts_in_tests_workflow() -> Dict[str, str]:
    """`{"tracebloc/cli": ".consumers/cli"}` -- what tests.yml checks out.

    Read from the workflow rather than restated, for the same reason the
    producer/consumer pairing is: a hand-kept copy agrees with itself while
    disagreeing with the job that actually runs.
    """
    out: Dict[str, str] = {}
    for step in _tests_workflow_steps():
        with_ = step.get("with") or {}
        if "checkout" in step.get("uses", "") and with_.get("repository"):
            # A cross-repo checkout with no `path:` lands ON TOP of this repo.
            # Refused here rather than returned as `None`, which the caller
            # would turn into `Path(None)` -- a TypeError whose message names
            # neither the repo nor the mistake.
            if not with_.get("path"):
                raise AssertionError(
                    f"tests.yml checks out {with_['repository']} with no "
                    f"`path:`, so it would land on top of this repo instead of "
                    f"its own directory"
                )
            out[with_["repository"]] = with_["path"]
    return out


class TestTheCheckoutListIsParsedOrRefused:
    """`_parse_checkouts` reads both the env var and the workflow's `env:` block.

    Its refusal paths are the ones that decide whether "I could not read this"
    reaches the caller or is quietly folded into "nobody supplied anything" --
    two states with different fixes. So they are exercised rather than assumed.
    """

    def test_a_comma_or_newline_separated_list_is_read(self):
        parsed = _parse_checkouts(" a/one=p1 ,\n b/two=p2 \n\n", "test")
        assert parsed == {"a/one": "p1", "b/two": "p2"}
        assert len(parsed) == 2

    def test_an_unset_variable_is_empty_rather_than_an_error(self):
        # The one benign empty: nothing was supplied. The CALLER decides what
        # that means, and in the real-consumer test it means failure.
        assert _parse_checkouts("", "test") == {}

    def test_an_entry_without_a_path_is_refused(self):
        with pytest.raises(AssertionError, match="is not REPO=PATH"):
            _parse_checkouts("tracebloc/cli", "test")

    def test_an_entry_with_an_empty_half_is_refused(self):
        with pytest.raises(AssertionError, match="is not REPO=PATH"):
            _parse_checkouts("tracebloc/cli=", "test")
        with pytest.raises(AssertionError, match="is not REPO=PATH"):
            _parse_checkouts("=.consumers/cli", "test")

    def test_the_same_repo_named_twice_is_refused(self):
        # Silently keeping the last would compare one checkout while the run
        # believes it compared the other.
        with pytest.raises(AssertionError, match="twice"):
            _parse_checkouts("a/one=p1,a/one=p2", "test")


class TestTheRequiredJobSuppliesTheRealConsumers:
    """The other half of the fix, and the half that can rot silently.

    Assertions (3) and (4) above are only as good as the inputs the workflow
    hands them, and a job that stops supplying them would not fail -- the test
    simply would not be SELECTED, and a marker that is never selected is the
    permanently-skipped test again under a different name. So the wiring is
    pinned here, in the required suite, where the fix is.
    """

    def test_the_claim_is_not_empty_and_names_only_declared_consumers(self):
        assert REAL_CONSUMERS_IN_THE_REQUIRED_JOB, (
            "no real consumer is compared in the required context, which is the "
            "state backend#3338 is about"
        )
        declared = {c.repo for c in DECLARED_CONSUMERS}
        assert set(REAL_CONSUMERS_IN_THE_REQUIRED_JOB) <= declared, (
            f"{sorted(set(REAL_CONSUMERS_IN_THE_REQUIRED_JOB) - declared)} is "
            f"compared here but not declared in DECLARED_CONSUMERS"
        )

    def test_tests_yml_checks_out_every_consumer_the_required_job_claims(self):
        checkouts = _real_consumer_checkouts_in_tests_workflow()
        assert set(checkouts) == set(REAL_CONSUMERS_IN_THE_REQUIRED_JOB), (
            f"tests.yml checks out {sorted(checkouts)} but the required job "
            f"claims to compare {sorted(REAL_CONSUMERS_IN_THE_REQUIRED_JOB)}"
        )

    def test_every_consumer_checkout_here_is_pinned_to_a_named_ref(self):
        # Same rule the agreement job states and the old sibling lookup broke:
        # compare the ref that actually reads us, not whatever a repo setting
        # currently points at.
        offenders = [
            (step.get("with") or {})["repository"]
            for step in _tests_workflow_steps()
            if "checkout" in step.get("uses", "")
            and (step.get("with") or {}).get("repository")
            and not (step.get("with") or {}).get("ref")
        ]
        assert not offenders, (
            f"these consumer checkouts inherit the repo default branch instead "
            f"of naming the ref that reads us: {offenders}"
        )

    def _real_consumers_step(self) -> dict:
        for step in _tests_workflow_steps():
            if REAL_CONSUMERS_TARGET in (step.get("run") or ""):
                return step
        raise AssertionError(
            f"no step in tests.yml runs `make {REAL_CONSUMERS_TARGET}`, so the "
            f"marked comparison is never SELECTED -- which is the "
            f"permanently-skipped test of backend#3338 wearing a marker"
        )

    def test_the_required_job_selects_the_marker_and_names_the_checkouts(self):
        step = self._real_consumers_step()
        env = step.get("env") or {}
        assert CHECKOUTS_ENV in env, (
            f"the `make {REAL_CONSUMERS_TARGET}` step does not set "
            f"{CHECKOUTS_ENV}, so the comparison would fail for want of inputs"
        )
        supplied = _parse_checkouts(
            str(env[CHECKOUTS_ENV]), f"{CHECKOUTS_ENV} in tests.yml"
        )
        assert supplied == _real_consumer_checkouts_in_tests_workflow(), (
            f"{CHECKOUTS_ENV} in tests.yml ({env[CHECKOUTS_ENV]!r}) does not "
            f"match the checkouts the same job performs"
        )

    def test_the_marker_is_registered_and_deselected_by_default(self):
        # The local half of the design: `pytest.ini` deselects the marker, so a
        # developer without a checkout sees "deselected" -- a count, not a pass.
        # Delete the addopts line and `make coverage` starts hard-failing for
        # want of the env var, which is loud; delete the MARKER registration and
        # --strict-markers turns the decorator into an error. Both directions
        # are red, and this pins the intended one.
        ini = (_repo_root() / "pytest.ini").read_text(encoding="utf-8")
        assert f"not {REAL_CONSUMERS_MARKER}" in ini, (
            "pytest.ini no longer deselects the marker by default, so a plain "
            "local run collects a test whose inputs only CI supplies"
        )
        assert f"{REAL_CONSUMERS_MARKER}:" in ini, (
            "the marker is not registered in pytest.ini; with --strict-markers "
            "that is an error, and without it a typo silently selects nothing"
        )

    def test_the_makefile_target_selects_the_marker(self):
        makefile = (_repo_root() / "Makefile").read_text(encoding="utf-8")
        assert f"\n{REAL_CONSUMERS_TARGET}:" in makefile, (
            f"tests.yml runs `make {REAL_CONSUMERS_TARGET}` but the Makefile "
            f"has no such target"
        )
        body = makefile.split(f"\n{REAL_CONSUMERS_TARGET}:", 1)[1].split("\n\n", 1)[0]
        assert f"-m {REAL_CONSUMERS_MARKER}" in body, (
            f"`make {REAL_CONSUMERS_TARGET}` does not select the "
            f"{REAL_CONSUMERS_MARKER} marker, so the required job would run the "
            f"default (deselected) set and compare nothing: {body!r}"
        )


class TestTheWorkflowScopeStepFailsClosed:
    """The scope step decides whether a missing token is tolerable.

    Every path that cannot answer must choose "a contract might have changed".
    The first cut chose the opposite by accident -- it ran `git diff` against a
    depth-1 merge checkout, the diff failed, `2>/dev/null` ate the error and the
    output defaulted to `false`, so a fork schema bump would have passed with a
    notice (Bugbot, #536). That is the exact defect this workflow exists to
    prevent, which is why it is pinned here rather than left to review.
    """

    @staticmethod
    def _scope_step() -> str:
        text = (
            Path(__file__).resolve().parents[1]
            / ".github/workflows/contract-consumers.yml"
        ).read_text()
        start = text.index("- name: Decide whether a contract is in play")
        end = text.index("- name: Mint a token for the consumer checkouts")
        return text[start:end]

    def test_it_does_not_infer_the_diff_from_git(self):
        # `git diff` cannot answer here: checkout gives a depth-1 MERGE commit,
        # so the head SHA is frequently absent from the local repo.
        step = self._scope_step()
        code = "\n".join(
            line for line in step.splitlines() if not line.strip().startswith("#")
        )
        assert "git diff" not in code, (
            "the scope step infers the diff from git again; a depth-1 merge "
            "checkout cannot answer this and the failure is silent"
        )

    def test_every_cannot_tell_branch_chooses_in_play(self):
        # Derived from the step itself: every `schema-touched=` it writes must be
        # `true` except exactly one -- the branch that positively established no
        # contract changed. A second `false` is a new way to fail open.
        step = self._scope_step()
        writes = [
            line.strip()
            for line in step.splitlines()
            if "schema-touched=" in line and "echo" in line
        ]
        assert writes, "the scope step writes no output at all"
        false_writes = [w for w in writes if "schema-touched=false" in w]
        assert len(false_writes) == 1, (
            f"expected exactly one `false` path (the positive 'no contract "
            f"changed' answer); found {len(false_writes)}: {false_writes}"
        )

    def test_the_refusal_branch_still_exists_and_is_fatal(self):
        # The other half: having decided a contract IS in play, an unreadable
        # consumer must end the run rather than warn.
        text = (
            Path(__file__).resolve().parents[1]
            / ".github/workflows/contract-consumers.yml"
        ).read_text()
        start = text.index("- name: Refuse to guess when a contract changed")
        end = text.index("- name: Check out the consumer")
        refusal = text[start:end]
        assert 'SCHEMA_TOUCHED" = "true"' in refusal
        assert "exit 1" in refusal, "the refusal branch does not fail the run"


class TestTheWorkflowDoesNotReintroduceTheClassesBugbotFound:
    """Four findings on #536, each turned into something that can fail again.

    All four were fail-open or over-privilege on a path no test could see, in a
    workflow whose entire purpose is to fail closed. They are pinned as CLASSES
    rather than as the reported lines: a sweep of the file after the first fix
    found a fourth `printf | grep -q` that the review had not flagged, which is
    the argument for checking the class rather than the instance.
    """

    @staticmethod
    def _workflow():
        return (
            Path(__file__).resolve().parents[1]
            / ".github/workflows/contract-consumers.yml"
        )

    @staticmethod
    def _parsed():
        import yaml

        return yaml.safe_load(
            (
                Path(__file__).resolve().parents[1]
                / ".github/workflows/contract-consumers.yml"
            ).read_text()
        )

    def test_nothing_pipes_into_an_early_exiting_reader(self):
        # `printf | grep -q` under `set -o pipefail`: grep exits on the first
        # match, printf takes SIGPIPE, the pipeline returns 141, and an `if`
        # reads that as "no match". In this workflow that answers
        # `schema-touched=false` -- the fail-open branch it exists to close.
        import re

        code = [
            line
            for line in self._workflow().read_text().splitlines()
            if not line.strip().startswith("#")
        ]
        offenders = [
            line.strip()
            for line in code
            if re.search(r"\|\s*(grep\s+-[A-Za-z]*q|head\b)", line)
        ]
        assert not offenders, (
            "a pipe into an early-exiting reader is back; under pipefail it "
            f"returns 141 and reads as 'no match': {offenders}"
        )

    def test_the_app_token_is_scoped_rather_than_inheriting_the_installation(self):
        # Unscoped, it inherits the App's full grant -- and checkout stores it in
        # the consumer's .git, after which this job runs the PR's own script
        # against that checkout.
        for step in self._parsed()["jobs"]["agreement"]["steps"]:
            if "create-github-app-token" in step.get("uses", ""):
                perms = {
                    k: v
                    for k, v in (step.get("with") or {}).items()
                    if k.startswith("permission-")
                }
                assert perms == {
                    "permission-contents": "read"
                }, f"the token mint is not scoped to contents:read: {perms}"
                return
        raise AssertionError("no token mint found — the step was renamed or removed")

    def test_the_consumer_checkout_does_not_persist_the_token(self):
        for step in self._parsed()["jobs"]["agreement"]["steps"]:
            with_ = step.get("with") or {}
            if with_.get("repository", "").endswith("e2e-test-agent"):
                assert with_.get("persist-credentials") is False, (
                    "the consumer checkout leaves the token in .consumer/.git, "
                    "which the PR's own script then runs against"
                )
                return
        raise AssertionError("no consumer checkout found")

    def test_the_producer_is_pinned_to_the_ref_the_consumer_reads(self):
        # A scheduled run checks out the DEFAULT branch unless told otherwise,
        # while the consumer is pinned at `develop` — so the nightly would
        # compare a released snapshot against the head that actually reads us.
        steps = self._parsed()["jobs"]["agreement"]["steps"]
        producer = next(
            s
            for s in steps
            if "checkout" in s.get("uses", "")
            and not (s.get("with") or {}).get("repository")
        )
        ref = (producer.get("with") or {}).get("ref", "")
        assert "schedule" in ref and "develop" in ref, (
            "the producer checkout does not pin `develop` on scheduled runs: "
            f"ref={ref!r}"
        )


# ==========================================================================
# The SECOND consumer: tracebloc/cli, which is Go (backend#3146)
# ==========================================================================

GO_GUARD = """
package push

import (
\t"encoding/json"

\t"github.com/tracebloc/cli/internal/schema"
)

// Doc comment naming scripts/sync-schema.sh and layout.v1.json in prose.
var SupportedLayoutVersions = map[string]bool{
\t"4": true,
}

func mustLoadLayoutContract() *LayoutContract {
\tvar c LayoutContract
\tif err := json.Unmarshal(schema.LayoutV1Bytes, &c); err != nil {
\t\tpanic("boom")
\t}
\treturn &c
}
"""

GO_EMBED = """
package schema

import _ "embed"

//go:embed ingest.v1.json
var V1Bytes []byte

//go:embed layout.v1.json
var LayoutV1Bytes []byte
"""


def _go_consumer(root: Path, **files: str) -> Path:
    """A fake Go consumer checkout: `<dotted/path>.go` per keyword.

    Keyword `internal__push__layout_contract` -> `internal/push/layout_contract.go`,
    so the fixtures carry the real repo's nesting rather than a flat directory
    the probe would never meet.
    """
    for name, body in files.items():
        path = root / (name.replace("__", "/") + ".go")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
    return root


def _real_cli_shaped(tmp_path: Path) -> Path:
    """The CLI's actual declaration shape, at its actual paths."""
    return _go_consumer(
        tmp_path,
        internal__push__layout_contract=GO_GUARD,
        internal__schema__embed=GO_EMBED,
    )


class TestTheGoProbeDerivesBothHalves:
    def test_it_finds_exactly_the_one_pair_the_cli_declares(self, tmp_path):
        # THE COUNT, ASSERTED. A walk that visited zero Go files passes exactly
        # as quietly as one that found nothing, and that is the defect class
        # this whole file is about. `== 1` is the CLI's real number: one
        # embedded contract whose version it gates on.
        found = go_consumer_contracts(_real_cli_shaped(tmp_path), "tracebloc/cli")
        assert len(found) == 1, f"expected exactly one declared pair, got {found}"
        assert found[0].module == "internal/push/layout_contract.go"
        assert found[0].relpath == "tracebloc_ingestor/schema/layout.v1.json"
        assert found[0].supported == frozenset({"4"})

    def test_the_contract_is_read_off_the_embed_directive_not_guessed(self, tmp_path):
        # Rename the embedded file and the derived contract path must follow. If
        # it does not, the path is coming from a hardcoded guess somewhere.
        root = _go_consumer(
            tmp_path,
            internal__push__layout_contract=GO_GUARD,
            internal__schema__embed=GO_EMBED.replace(
                "//go:embed layout.v1.json", "//go:embed layout.v2.json"
            ),
        )
        found = go_consumer_contracts(root, "tracebloc/cli")
        assert len(found) == 1
        assert found[0].relpath == "tracebloc_ingestor/schema/layout.v2.json"

    def test_V1Bytes_is_a_SUFFIX_of_LayoutV1Bytes_and_must_not_match_it(self, tmp_path):
        # THE ONE PLACE SUBSTRING vs WORD BOUNDARY CHANGES THE ANSWER, not just
        # the tidiness. The CLI embeds both `V1Bytes` (ingest.v1.json) and
        # `LayoutV1Bytes` (layout.v1.json). A substring scan for the first
        # matches inside the second, so the guard file "references" TWO
        # contracts, and the probe either raises or -- worse, if it picked the
        # first sorted name -- pairs the LAYOUT version guard with the INGEST
        # schema, whose version it does not gate on at all.
        found = go_consumer_contracts(_real_cli_shaped(tmp_path), "tracebloc/cli")
        assert found[0].relpath.endswith("layout.v1.json"), (
            "the layout guard was paired with the wrong embedded contract -- "
            f"substring matching is back: {found[0].relpath}"
        )

    def test_a_test_file_narrowing_the_set_is_not_read_as_the_real_set(self, tmp_path):
        # `cli#634`'s TestTheGuardIsWiredIntoTheLoader reassigns
        # SupportedLayoutVersions to prove the guard fires. Reading that as the
        # CLI's support would report a disagreement that does not exist.
        root = _real_cli_shaped(tmp_path)
        (root / "internal/push/layout_version_guard_test.go").write_text(
            'package push\n\nvar SupportedLayoutVersions = map[string]bool{"__none__": true}\n',
            encoding="utf-8",
        )
        found = go_consumer_contracts(root, "tracebloc/cli")
        assert len(found) == 1
        assert found[0].supported == frozenset({"4"})

    def test_a_commented_out_declaration_does_not_count(self, tmp_path):
        # The Go half of the AST rule: a substring search would accept a module
        # that declares nothing.
        root = _go_consumer(
            tmp_path,
            internal__push__layout_contract=GO_GUARD.replace(
                "var SupportedLayoutVersions = map[string]bool{",
                "// var SupportedLayoutVersions = map[string]bool{",
            ).replace('\t"4": true,\n}', '//\t"4": true,\n// }'),
            internal__schema__embed=GO_EMBED,
        )
        assert go_consumer_contracts(root, "tracebloc/cli") == []

    def test_a_runtime_built_set_does_not_count(self, tmp_path):
        root = _go_consumer(
            tmp_path,
            internal__push__layout_contract=GO_GUARD.replace(
                'map[string]bool{\n\t"4": true,\n}', "loadVersions()"
            ),
            internal__schema__embed=GO_EMBED,
        )
        assert go_consumer_contracts(root, "tracebloc/cli") == []

    def test_an_entry_the_pattern_cannot_read_voids_the_whole_declaration(
        self, tmp_path
    ):
        # A partial read is worse than no read: a set quietly too small reports
        # a disagreement that does not exist, one quietly too large reports
        # agreement that does not. So an unrecognised entry means "not a shape
        # I read", which the zero-pairs rule then turns into a failure.
        root = _go_consumer(
            tmp_path,
            internal__push__layout_contract=GO_GUARD.replace(
                '\t"4": true,', '\t"4": true,\n\tlatestVersion: true,'
            ),
            internal__schema__embed=GO_EMBED,
        )
        assert go_consumer_contracts(root, "tracebloc/cli") == []

    def test_a_version_mapped_to_false_is_not_support(self, tmp_path):
        # `"5": false` is a Go-idiomatic "known about, not supported". Reading
        # it as support inverts the answer this check exists to give.
        root = _go_consumer(
            tmp_path,
            internal__push__layout_contract=GO_GUARD.replace(
                '\t"4": true,', '\t"4": true,\n\t"5": false,'
            ),
            internal__schema__embed=GO_EMBED,
        )
        found = go_consumer_contracts(root, "tracebloc/cli")
        assert found[0].supported == frozenset(
            {"4"}
        ), "a version explicitly mapped to false was read as supported"

    def test_the_fix_instruction_names_the_GO_symbol_not_the_python_one(self, tmp_path):
        # A message telling someone editing Go to widen `SUPPORTED_VERSIONS`
        # sends them looking for a symbol that is not in the repo.
        consumer = _real_cli_shaped(tmp_path / "c")
        producer = _producer(
            tmp_path / "p", "tracebloc_ingestor/schema/layout.v1.json", {"version": 5}
        )
        problems = disagreements(producer, go_consumer_contracts(consumer, "x"))
        assert len(problems) == 1
        assert "SupportedLayoutVersions" in problems[0]
        assert "SUPPORTED_VERSIONS" not in problems[0]


class TestTheGoProbeFailsClosed:
    def test_a_missing_checkout_raises_rather_than_returning_empty(self, tmp_path):
        with pytest.raises(AgreementError) as caught:
            go_consumer_contracts(tmp_path / "nowhere", "tracebloc/cli")
        assert "missing" in str(caught.value)

    def test_a_checkout_with_no_go_sources_is_a_finding_not_a_skip(self, tmp_path):
        (tmp_path / "README.md").write_text("hi", encoding="utf-8")
        with pytest.raises(AgreementError) as caught:
            go_consumer_contracts(tmp_path, "tracebloc/cli")
        assert "not a" in str(caught.value)

    def test_a_guard_referencing_NO_embedded_contract_is_a_finding(self, tmp_path):
        # It declares a supported set but nothing says which contract that set
        # is about. Picking one would be the pairing table this file refuses.
        root = _go_consumer(
            tmp_path,
            internal__push__layout_contract=GO_GUARD.replace(
                "schema.LayoutV1Bytes", "someLocalBytes"
            ),
            internal__schema__embed=GO_EMBED,
        )
        with pytest.raises(AgreementError) as caught:
            go_consumer_contracts(root, "tracebloc/cli")
        assert "references 0" in str(caught.value)

    def test_a_guard_referencing_TWO_embedded_contracts_is_a_finding(self, tmp_path):
        root = _go_consumer(
            tmp_path,
            internal__push__layout_contract=GO_GUARD.replace(
                "&c); err != nil", "&c); err != nil || len(schema.V1Bytes) == 0"
            ),
            internal__schema__embed=GO_EMBED,
        )
        with pytest.raises(AgreementError) as caught:
            go_consumer_contracts(root, "tracebloc/cli")
        assert "references 2" in str(caught.value)

    def test_a_prose_mention_of_an_embedded_var_is_not_a_reference(self, tmp_path):
        # Comments are stripped before the reference scan. `layout_contract.go`
        # names the vendored file and the sync script in its doc comments; a
        # scan that matched prose would pair the guard with whatever a sentence
        # happened to mention.
        root = _go_consumer(
            tmp_path,
            internal__push__layout_contract=GO_GUARD.replace(
                "// Doc comment naming",
                "// V1Bytes is mentioned here in prose. Doc comment naming",
            ),
            internal__schema__embed=GO_EMBED,
        )
        found = go_consumer_contracts(root, "tracebloc/cli")
        assert len(found) == 1
        assert found[0].relpath.endswith("layout.v1.json")


class TestEveryDeclaredConsumerIsActuallyChecked:
    """The count assertion one layer up: a probe that ran on nothing.

    Two probes now. One silently finding nothing while the other keeps the
    result non-empty is the partial miss Bugbot found INSIDE the Python probe on
    #536, repeated at the level of consumers -- so it is enforced per consumer,
    and every declared consumer must be supplied at all.
    """

    def test_both_consumers_are_declared(self):
        assert {c.repo for c in DECLARED_CONSUMERS} == {
            "tracebloc/e2e-test-agent",
            "tracebloc/cli",
        }

    def test_a_declared_consumer_with_no_checkout_is_an_error(self, tmp_path):
        # The hole this closes: adding a consumer to DECLARED_CONSUMERS without
        # checking it out in the workflow would otherwise produce a shorter
        # list and a green check.
        producer = _producer(
            tmp_path / "p", "tracebloc_ingestor/schema/layout.v1.json", {"version": 4}
        )
        consumer = _consumer(tmp_path / "c", layout=MODULE.replace('{"2"}', '{"4"}'))
        with pytest.raises(AgreementError) as caught:
            check_consumers(producer, {"tracebloc/e2e-test-agent": consumer})
        assert "tracebloc/cli" in str(caught.value)
        assert "did not ask" in str(caught.value)

    def test_an_undeclared_consumer_being_supplied_is_also_an_error(self, tmp_path):
        producer = tmp_path / "p"
        with pytest.raises(AgreementError) as caught:
            check_consumers(
                producer,
                {
                    "tracebloc/e2e-test-agent": tmp_path,
                    "tracebloc/cli": tmp_path,
                    "tracebloc/nobody": tmp_path,
                },
            )
        assert "tracebloc/nobody" in str(caught.value)

    def test_ONE_consumer_finding_nothing_fails_even_when_the_other_finds_pairs(
        self, tmp_path
    ):
        # THE HEADLINE. The Python consumer declares a real pair; the Go one
        # declares nothing. A total-count rule would see a non-empty list and
        # pass, leaving the CLI unchecked -- exactly the state this ticket is
        # about, reintroduced through the back door.
        producer = _producer(
            tmp_path / "p", "tracebloc_ingestor/schema/layout.v1.json", {"version": 4}
        )
        py = _consumer(tmp_path / "py", layout=MODULE.replace('{"2"}', '{"4"}'))
        go = _go_consumer(
            tmp_path / "go",
            internal__push__layout_contract="package push\n\nvar x = 1\n",
            internal__schema__embed=GO_EMBED,
        )
        with pytest.raises(AgreementError) as caught:
            check_consumers(
                producer,
                {"tracebloc/e2e-test-agent": py, "tracebloc/cli": go},
            )
        message = str(caught.value)
        assert "tracebloc/cli" in message, (
            "a consumer that found nothing did not name itself: " + message
        )
        assert "zero parsed pairs" in message

    def test_the_happy_path_returns_every_pair_from_every_consumer(self, tmp_path):
        producer = _producer(
            tmp_path / "p", "tracebloc_ingestor/schema/layout.v1.json", {"version": 4}
        )
        py = _consumer(tmp_path / "py", layout=MODULE.replace('{"2"}', '{"4"}'))
        go = _real_cli_shaped(tmp_path / "go")
        contracts = check_consumers(
            producer, {"tracebloc/e2e-test-agent": py, "tracebloc/cli": go}
        )
        assert len(contracts) == 2, f"expected one pair per consumer, got {contracts}"
        assert {c.repo for c in contracts} == {
            "tracebloc/e2e-test-agent",
            "tracebloc/cli",
        }
        assert disagreements(producer, contracts) == []


class TestItWouldCatchTheBumpBackend3146IsAbout:
    """The mutation that matters for the CLI, and it must be SEEN to refuse.

    `data-ingestors#555` bumped `layout.v1.json` to v4 because
    `object_detection`'s manifest became `kind: "none"` -- a new value a reader
    must grow a branch for. The CLI accepts `{"4"}` today. Publish a v5 and the
    producer's own CI must go red BEFORE merge, naming the CLI.
    """

    def test_a_version_the_cli_does_not_support_is_refused(self, tmp_path):
        producer = _producer(
            tmp_path / "p", "tracebloc_ingestor/schema/layout.v1.json", {"version": 5}
        )
        py = _consumer(
            tmp_path / "py", layout=MODULE.replace('{"2"}', '{"4", "5"}')
        )  # the harness HAS been taught v5
        go = _real_cli_shaped(tmp_path / "go")  # the CLI has not
        problems = disagreements(
            producer,
            check_consumers(
                producer, {"tracebloc/e2e-test-agent": py, "tracebloc/cli": go}
            ),
        )
        assert len(problems) == 1, (
            "exactly the CLI should disagree; the harness accepts v5. "
            f"got {problems}"
        )
        assert "tracebloc/cli" in problems[0]
        assert "'5'" in problems[0] and "['4']" in problems[0]
        assert "land the consumer change FIRST" in problems[0]

    def test_and_goes_green_once_the_cli_is_taught_to_read_it(self, tmp_path):
        # The other direction, so the test above is not passing for a reason
        # unrelated to the version.
        producer = _producer(
            tmp_path / "p", "tracebloc_ingestor/schema/layout.v1.json", {"version": 5}
        )
        py = _consumer(tmp_path / "py", layout=MODULE.replace('{"2"}', '{"4", "5"}'))
        go = _go_consumer(
            tmp_path / "go",
            internal__push__layout_contract=GO_GUARD.replace(
                '\t"4": true,', '\t"4": true,\n\t"5": true,'
            ),
            internal__schema__embed=GO_EMBED,
        )
        contracts = check_consumers(
            producer, {"tracebloc/e2e-test-agent": py, "tracebloc/cli": go}
        )
        assert len(contracts) == 2
        assert disagreements(producer, contracts) == []


class TestTheWorkflowChecksOutBothConsumers:
    @staticmethod
    def _parsed():
        import yaml

        return yaml.safe_load(
            (
                Path(__file__).resolve().parents[1]
                / ".github/workflows/contract-consumers.yml"
            ).read_text()
        )

    def _cross_repo_checkouts(self):
        return [
            (step.get("with") or {})
            for step in self._parsed()["jobs"]["agreement"]["steps"]
            if "checkout" in step.get("uses", "")
            and (step.get("with") or {}).get("repository")
        ]

    def test_one_checkout_per_declared_consumer(self):
        # A declared consumer with no checkout makes the script refuse the run
        # (TestEveryDeclaredConsumerIsActuallyChecked); this catches it here,
        # where the fix is, instead of as a red on an unrelated schema PR.
        checkouts = self._cross_repo_checkouts()
        assert len(checkouts) == len(DECLARED_CONSUMERS), (
            f"{len(DECLARED_CONSUMERS)} consumers declared but "
            f"{len(checkouts)} checked out"
        )
        assert {w["repository"] for w in checkouts} == {
            c.repo for c in DECLARED_CONSUMERS
        }

    def test_no_consumer_checkout_persists_the_token(self):
        # Was asserted for `e2e-test-agent` only, and returned on the first
        # match -- so a second consumer checkout could persist its credential
        # into a directory the PR's own script then runs against, with the test
        # still green.
        checkouts = self._cross_repo_checkouts()
        assert checkouts, "no consumer checkout found"
        offenders = [
            w["repository"]
            for w in checkouts
            if w.get("persist-credentials") is not False
        ]
        assert not offenders, (
            "these consumer checkouts leave the token in their .git, which the "
            f"PR's own script then runs against: {offenders}"
        )

    def test_every_consumer_is_pinned_to_a_named_ref(self):
        offenders = [
            w["repository"] for w in self._cross_repo_checkouts() if not w.get("ref")
        ]
        assert not offenders, (
            "these consumer checkouts inherit the repo default branch instead "
            f"of naming the ref that reads us: {offenders}"
        )

    def test_the_token_mint_covers_every_declared_consumer(self):
        # A checkout without the token in scope fails with a clone error rather
        # than the deliberate refusal, which reads as a broken workflow.
        for step in self._parsed()["jobs"]["agreement"]["steps"]:
            if "create-github-app-token" in step.get("uses", ""):
                scoped = set((step["with"]["repositories"] or "").split())
                assert scoped == {
                    c.repo.split("/", 1)[1] for c in DECLARED_CONSUMERS
                }, f"the token mint does not cover every declared consumer: {scoped}"
                return
        raise AssertionError("no token mint found — the step was renamed or removed")
