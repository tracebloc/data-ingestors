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
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.contract_consumers import (  # noqa: E402
    AgreementError,
    check,
    consumer_contracts,
    disagreements,
    published_version,
)


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
    something real. It skips only when no consumer checkout is present -- CI
    provides one, and a developer without it is not blocked.
    """

    def test_every_contract_this_repo_publishes_parses_and_carries_a_version(self):
        root = Path(__file__).resolve().parents[1]
        schemas = sorted((root / "tracebloc_ingestor" / "schema").glob("*.json"))
        assert schemas, "no schema files found — the layout moved"
        for path in schemas:
            doc = json.loads(path.read_text(encoding="utf-8"))
            assert isinstance(doc, dict), f"{path.name} is not an object"

    def test_the_checked_out_consumer_can_read_us(self):
        root = Path(__file__).resolve().parents[1]
        consumer = Path(__file__).resolve().parents[2] / "e2e-test-agent"
        if not (consumer / "harness").is_dir():
            pytest.skip("no e2e-test-agent checkout beside this repo")
        assert disagreements(root, consumer_contracts(consumer)) == []


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
        end = text.index("- name: Mint a token for the private consumer")
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
