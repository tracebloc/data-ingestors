"""The release-artefact guard works, and can fail.

`scripts/check_dist_contents.py` refuses a wheel or sdist that ships anything
but `tracebloc_ingestor`. It runs in the publish workflows, after the build and
before the install and upload steps, where a test cannot see it -- so the LOGIC
is covered here, in the required `pytest` context.

Three shapes of evidence, none of which restates the rule:

* hand-built artefacts in tmp_path, one defect each, every refusal asserted by
  the path it names -- a refusal that cannot say which refusal is a coin toss;
* a REAL `setup.py sdist bdist_wheel` of a project with the defect this guard
  exists for (`find_packages()` with no `include=`, no `prune tests`), driven
  through the guard's own `check_dist` and shown to redden -- and the same
  project with the fix shown to pass, so the red is the mutation's and not the
  harness's;
* the two publish workflows, parsed, with the guard step found AFTER the build
  and BEFORE anything that installs or uploads `dist/`. The set of workflows is
  derived from which ones build a dist, not listed here.
"""

from __future__ import annotations

import io
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.check_dist_contents import (  # noqa: E402
    EXPECTED_TOP_LEVEL,
    REFUSE,
    check_dist,
    forbidden_members,
    main,
    required_schema_files,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
NAME = "tracebloc_ingestor"
VERSION = "9.9.9"
ROOT = f"{NAME}-{VERSION}"

# What a clean release carries, written down independently of the guard: the
# package, its schema sub-package and the two JSON contracts the fake source
# tree below declares. `required_schema_files` must derive exactly these.
SCHEMA_JSON = ("alpha.v1.json", "beta.v1.json")
PACKAGE_MEMBERS: Dict[str, bytes] = {
    f"{NAME}/__init__.py": b"__version__ = '9.9.9'\n",
    f"{NAME}/cli.py": b"def main():\n    pass\n",
    f"{NAME}/schema/__init__.py": b"",
    **{f"{NAME}/schema/{j}": b"{}\n" for j in SCHEMA_JSON},
}


# --------------------------------------------------------------------------
# Hand-built artefacts
# --------------------------------------------------------------------------


@pytest.fixture
def source(tmp_path: Path) -> Path:
    """A fake source tree the guard derives the required schema files from."""
    src = tmp_path / "src"
    for j in SCHEMA_JSON:
        p = src / NAME / "schema" / j
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("{}\n")
    # A non-JSON neighbour that must NOT become a requirement.
    (src / NAME / "schema" / "__init__.py").write_text("")
    return src


def make_wheel(
    dist: Path,
    members: Dict[str, bytes] = PACKAGE_MEMBERS,
    top_level: Optional[str] = f"{NAME}\n",
    filename: str = f"{ROOT}-py3-none-any.whl",
) -> Path:
    dist.mkdir(exist_ok=True)
    path = dist / filename
    with zipfile.ZipFile(path, "w") as zf:
        for name, body in members.items():
            zf.writestr(name, body)
        zf.writestr(f"{ROOT}.dist-info/METADATA", f"Name: {NAME}\n")
        if top_level is not None:
            zf.writestr(f"{ROOT}.dist-info/top_level.txt", top_level)
    return path


def make_sdist(
    dist: Path,
    members: Dict[str, bytes] = PACKAGE_MEMBERS,
    top_level: Optional[str] = f"{NAME}\n",
    filename: str = f"{ROOT}.tar.gz",
) -> Path:
    dist.mkdir(exist_ok=True)
    path = dist / filename

    def add(tf: tarfile.TarFile, name: str, body: bytes) -> None:
        info = tarfile.TarInfo(f"{ROOT}/{name}")
        info.size = len(body)
        tf.addfile(info, io.BytesIO(body))

    with tarfile.open(path, "w:gz") as tf:
        for name, body in members.items():
            add(tf, name, body)
        add(tf, "PKG-INFO", f"Name: {NAME}\n".encode())
        if top_level is not None:
            add(tf, f"{NAME}.egg-info/top_level.txt", top_level.encode())
    return path


def with_extra(extra: Dict[str, bytes]) -> Dict[str, bytes]:
    return {**PACKAGE_MEMBERS, **extra}


def without(*names: str) -> Dict[str, bytes]:
    return {k: v for k, v in PACKAGE_MEMBERS.items() if k not in names}


def findings_naming(findings: List[str], needle: str) -> List[str]:
    return [f for f in findings if needle in f]


class TestTheRequiredSchemaFilesAreDerived:
    def test_from_the_fake_tree(self, source: Path):
        assert required_schema_files(source) == sorted(
            f"{NAME}/schema/{j}" for j in SCHEMA_JSON
        )

    def test_from_this_repo(self):
        # Non-empty is the only thing that can be said without restating the
        # directory listing; the guard refuses an empty answer (below).
        found = required_schema_files(REPO_ROOT)
        assert found, "the real source tree derived no schema files"
        assert all(
            f.startswith(f"{NAME}/schema/") and f.endswith(".json") for f in found
        )

    def test_an_empty_tree_is_a_refusal_not_a_pass(self, tmp_path: Path):
        dist = tmp_path / "dist"
        make_wheel(dist)
        make_sdist(dist)
        bare = tmp_path / "bare"
        bare.mkdir()
        findings = check_dist(dist, bare)
        assert findings_naming(findings, "no *.json schema files"), findings


class TestCleanArtefactsPass:
    def test_check_dist_is_empty(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        make_wheel(dist)
        make_sdist(dist)
        assert check_dist(dist, source) == []

    def test_main_exits_zero_and_says_ok(self, tmp_path: Path, source: Path, capsys):
        dist = tmp_path / "dist"
        make_wheel(dist)
        make_sdist(dist)
        assert main(["--dist", str(dist), "--source", str(source)]) == 0
        out = capsys.readouterr().out
        assert out.startswith("OK:")
        assert sorted(EXPECTED_TOP_LEVEL) == [NAME]


class TestTheDistDirectoryMustHoldExactlyOneOfEach:
    def test_missing_directory(self, tmp_path: Path, source: Path):
        findings = check_dist(tmp_path / "nope", source)
        assert findings_naming(findings, "not a directory"), findings

    def test_no_wheel(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        make_sdist(dist)
        findings = check_dist(dist, source)
        assert findings_naming(findings, "expected exactly one .whl, found 0"), findings

    def test_two_wheels(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        make_wheel(dist)
        make_wheel(dist, filename=f"{NAME}-9.9.8-py3-none-any.whl")
        make_sdist(dist)
        findings = check_dist(dist, source)
        hits = findings_naming(findings, "expected exactly one .whl, found 2")
        assert hits and "9.9.8" in hits[0], findings

    def test_no_sdist(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        make_wheel(dist)
        findings = check_dist(dist, source)
        assert findings_naming(
            findings, "expected exactly one .tar.gz, found 0"
        ), findings

    def test_a_stray_file_is_named(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        make_wheel(dist)
        make_sdist(dist)
        (dist / f"{ROOT}.egg").write_bytes(b"x")
        findings = check_dist(dist, source)
        hits = findings_naming(findings, "unexpected entries")
        assert hits and f"{ROOT}.egg" in hits[0], findings

    def test_shape_findings_stop_the_check(self, tmp_path: Path, source: Path):
        # With no wheel there is nothing to read top_level.txt from; the guard
        # must say so and not also emit a misleading "no top_level.txt".
        dist = tmp_path / "dist"
        make_sdist(dist)
        findings = check_dist(dist, source)
        assert not findings_naming(findings, "top_level.txt"), findings


class TestTopLevelMustBeExactlyThePackage:
    def test_wheel_naming_tests(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        make_wheel(dist, top_level=f"tests\n{NAME}\n")
        make_sdist(dist)
        hits = findings_naming(check_dist(dist, source), "top_level.txt is")
        assert hits and "'tests'" in hits[0] and ".whl" in hits[0], hits

    def test_wheel_without_top_level_txt(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        make_wheel(dist, top_level=None)
        make_sdist(dist)
        findings = check_dist(dist, source)
        assert findings_naming(findings, "no *.dist-info/top_level.txt"), findings

    def test_sdist_naming_tests(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        make_wheel(dist)
        make_sdist(dist, top_level=f"tests\n{NAME}\n")
        hits = findings_naming(check_dist(dist, source), "top_level.txt is")
        assert hits and "'tests'" in hits[0] and ".tar.gz" in hits[0], hits

    def test_sdist_without_top_level_txt(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        make_wheel(dist)
        make_sdist(dist, top_level=None)
        findings = check_dist(dist, source)
        assert findings_naming(findings, "no *.egg-info/top_level.txt"), findings

    def test_a_different_single_name_is_also_refused(
        self, tmp_path: Path, source: Path
    ):
        dist = tmp_path / "dist"
        make_wheel(dist, top_level="tracebloc_ingestor_renamed\n")
        make_sdist(dist)
        hits = findings_naming(check_dist(dist, source), "top_level.txt is")
        assert hits and "tracebloc_ingestor_renamed" in hits[0], hits


class TestNoTestCodeInEitherArtefact:
    @pytest.mark.parametrize(
        "offender",
        [
            "tests/__init__.py",
            "tests/test_database.py",
            f"{NAME}/tests/test_nested.py",
            "conftest.py",
            f"{NAME}/conftest.py",
        ],
    )
    def test_wheel_member(self, tmp_path: Path, source: Path, offender: str):
        dist = tmp_path / "dist"
        make_wheel(dist, members=with_extra({offender: b""}))
        make_sdist(dist)
        findings = check_dist(dist, source)
        hits = findings_naming(findings, f"ships test code: {offender}")
        assert hits and ".whl" in hits[0], findings
        # Only the offender is named; the package itself is not collateral.
        assert not findings_naming(findings, f"ships test code: {NAME}/__init__.py")

    @pytest.mark.parametrize(
        "offender",
        ["tests/__init__.py", "tests/test_database.py", "conftest.py"],
    )
    def test_sdist_member(self, tmp_path: Path, source: Path, offender: str):
        dist = tmp_path / "dist"
        make_wheel(dist)
        make_sdist(dist, members=with_extra({offender: b""}))
        findings = check_dist(dist, source)
        hits = findings_naming(findings, f"ships test code: {ROOT}/{offender}")
        assert hits and ".tar.gz" in hits[0], findings

    def test_the_rule_itself_on_independent_inputs(self):
        # Inputs written down here, not read from the guard's constants: a
        # typo in FORBIDDEN_COMPONENTS must redden this, not be mirrored.
        members = [
            "tracebloc_ingestor/__init__.py",
            "tracebloc_ingestor/testsuite_helpers.py",  # not `tests`
            "tracebloc_ingestor/contest.py",  # not `conftest.py`
            "tests/test_x.py",
            "a/b/tests/c.py",
            "deep/conftest.py",
        ]
        assert forbidden_members(members) == [
            "tests/test_x.py",
            "a/b/tests/c.py",
            "deep/conftest.py",
        ]


class TestTheSchemaFilesMustShip:
    def test_missing_from_the_wheel(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        gone = f"{NAME}/schema/{SCHEMA_JSON[0]}"
        make_wheel(dist, members=without(gone))
        make_sdist(dist)
        findings = check_dist(dist, source)
        hits = findings_naming(findings, f"missing required file: {gone}")
        assert len(hits) == 1 and ".whl" in hits[0], findings

    def test_missing_from_the_sdist(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        gone = f"{NAME}/schema/{SCHEMA_JSON[1]}"
        make_wheel(dist)
        make_sdist(dist, members=without(gone))
        findings = check_dist(dist, source)
        hits = findings_naming(findings, f"missing required file: {gone}")
        assert len(hits) == 1 and ".tar.gz" in hits[0], findings

    def test_every_missing_file_is_named(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        make_wheel(dist, members=without(*(f"{NAME}/schema/{j}" for j in SCHEMA_JSON)))
        make_sdist(dist)
        findings = check_dist(dist, source)
        for j in SCHEMA_JSON:
            assert findings_naming(
                findings, f"missing required file: {NAME}/schema/{j}"
            )


class TestUnreadableArtefactsAreRefusedNotSkipped:
    def test_garbage_wheel(self, tmp_path: Path, source: Path, capsys):
        dist = tmp_path / "dist"
        dist.mkdir()
        (dist / f"{ROOT}-py3-none-any.whl").write_bytes(b"this is not a zip")
        make_sdist(dist)
        findings = check_dist(dist, source)
        assert findings_naming(findings, "unreadable wheel"), findings
        assert main(["--dist", str(dist), "--source", str(source)]) == REFUSE
        out = capsys.readouterr().out
        assert out.startswith("REFUSED:") and "unreadable wheel" in out

    def test_garbage_sdist(self, tmp_path: Path, source: Path):
        dist = tmp_path / "dist"
        make_wheel(dist)
        (dist / f"{ROOT}.tar.gz").write_bytes(b"this is not a tarball")
        findings = check_dist(dist, source)
        assert findings_naming(findings, "unreadable sdist"), findings

    def test_refusal_exit_code_names_every_offender(
        self, tmp_path: Path, source: Path, capsys
    ):
        dist = tmp_path / "dist"
        make_wheel(
            dist, members=with_extra({"tests/test_a.py": b"", "conftest.py": b""})
        )
        make_sdist(dist)
        assert main(["--dist", str(dist), "--source", str(source)]) == REFUSE
        out = capsys.readouterr().out
        assert "tests/test_a.py" in out and "conftest.py" in out
        assert REFUSE == 2


# --------------------------------------------------------------------------
# A real build, with and without the defect
# --------------------------------------------------------------------------

SETUP_PY = """\
from setuptools import setup, find_packages

setup(
    name="tracebloc_ingestor",
    version="9.9.9",
    packages={packages},
    package_data={{"tracebloc_ingestor.schema": ["*.json"]}},
    include_package_data=True,
)
"""

MANIFEST_IN = "recursive-include tracebloc_ingestor/schema *.json\n"


def _write_project(root: Path, packages: str, manifest_extra: str = "") -> Path:
    """A project with this repo's shape: the package, a schema sub-package with
    JSON contracts, and a `tests/` package beside it with a conftest.py."""
    root.mkdir(parents=True)
    (root / "setup.py").write_text(SETUP_PY.format(packages=packages))
    (root / "MANIFEST.in").write_text(MANIFEST_IN + manifest_extra)
    for name, body in PACKAGE_MEMBERS.items():
        p = root / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(body)
    (root / "tests").mkdir()
    (root / "tests" / "__init__.py").write_text("")
    (root / "tests" / "conftest.py").write_text("import pytest\n")
    (root / "tests" / "test_something.py").write_text("def test_x():\n    pass\n")
    return root


def _build(project: Path) -> Path:
    """`python setup.py sdist bdist_wheel`, the command the publish workflows run."""
    try:
        import setuptools  # noqa: F401
    except ImportError:  # pragma: no cover - environment, not logic
        pytest.fail(
            "setuptools is not importable in this interpreter; "
            "install requirements-dev.txt (this test builds a real wheel)"
        )
    proc = subprocess.run(
        [sys.executable, "setup.py", "-q", "sdist", "bdist_wheel"],
        cwd=project,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"build failed:\n{proc.stdout}\n{proc.stderr}"
    return project / "dist"


DEFECT = "find_packages()"
FIX = 'find_packages(include=["tracebloc_ingestor", "tracebloc_ingestor.*"])'


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> Dict[str, Tuple[Path, Path]]:
    """(source, dist) for each variant, built once per module -- a real build
    is ~2 s and there are three of them."""
    base = tmp_path_factory.mktemp("realbuild")
    variants = {
        "defect": _write_project(base / "defect", DEFECT),
        "include_only": _write_project(base / "include_only", FIX),
        "fixed": _write_project(base / "fixed", FIX, manifest_extra="prune tests\n"),
    }
    return {k: (src, _build(src)) for k, src in variants.items()}


class TestAgainstARealBuild:
    """Mutation proof through the guard's real `check_dist`, not a copy of it."""

    def test_the_mutation_applied(self, built):
        # Rule 5: an inert mutation and good coverage look identical in a log.
        # Read the artefact directly: the defect really does put `tests` in.
        _, dist = built["defect"]
        with zipfile.ZipFile(next(dist.glob("*.whl"))) as zf:
            names = zf.namelist()
            top = [n for n in names if n.endswith(".dist-info/top_level.txt")]
            assert top, names
            top_level = zf.read(top[0]).decode()
        assert "tests/test_something.py" in names
        assert "tests" in top_level.split()

    def test_find_packages_without_include_reddens(self, built):
        src, dist = built["defect"]
        findings = check_dist(dist, src)
        assert findings, "the guard passed an artefact that ships the test suite"
        # The wheel: top_level.txt names tests, and the members are listed.
        assert any(
            "top_level.txt is" in f and "'tests'" in f and ".whl" in f for f in findings
        )
        assert any(
            "ships test code: tests/test_something.py" in f and ".whl" in f
            for f in findings
        )
        assert any(
            "ships test code: tests/conftest.py" in f and ".whl" in f for f in findings
        )
        # The sdist too.
        assert any(
            "ships test code:" in f
            and "/tests/test_something.py" in f
            and ".tar.gz" in f
            for f in findings
        )
        # And nothing about the schema files, which the defect does ship.
        assert not any("missing required file" in f for f in findings), findings

    def test_include_alone_leaves_the_sdist_red(self, built):
        # Measured on this repo: `include=` cleaned the wheel and the sdist
        # still carried 100 tests/ members, because distutils' sdist defaults
        # add tests/test*.py by themselves. The guard must see that half.
        src, dist = built["include_only"]
        findings = check_dist(dist, src)
        assert not any(".whl" in f for f in findings), findings
        assert any(
            "ships test code:" in f
            and "/tests/test_something.py" in f
            and ".tar.gz" in f
            for f in findings
        ), findings

    def test_the_fix_is_green(self, built):
        # Positive control: the same harness, the same guard, no defect.
        src, dist = built["fixed"]
        assert check_dist(dist, src) == []
        with zipfile.ZipFile(next(dist.glob("*.whl"))) as zf:
            names = zf.namelist()
        for j in SCHEMA_JSON:
            assert f"{NAME}/schema/{j}" in names


# --------------------------------------------------------------------------
# The publish workflows run the guard, in the right place
# --------------------------------------------------------------------------

WORKFLOWS = REPO_ROOT / ".github" / "workflows"


def _dist_building_jobs() -> List[Tuple[str, str, List[dict]]]:
    """Every (workflow, job, steps) in the repo whose steps build a dist.

    Derived by reading every workflow, not by naming publish-*.yml: a third
    workflow that starts building a dist inherits the requirement.
    """
    out = []
    for path in sorted(WORKFLOWS.glob("*.yml")):
        parsed = yaml.safe_load(path.read_text()) or {}
        for job_name, job in (parsed.get("jobs") or {}).items():
            steps = job.get("steps") or []
            if any(_builds_dist(s) for s in steps):
                out.append((path.name, job_name, steps))
    return out


def _run(step: dict) -> str:
    return step.get("run") or ""


def _builds_dist(step: dict) -> bool:
    run = _run(step)
    return "sdist" in run and "bdist_wheel" in run


def _is_guard(step: dict) -> bool:
    return "scripts/check_dist_contents.py" in _run(step)


def _consumes_dist(step: dict) -> bool:
    """Installs from or uploads dist/ -- anything the guard must precede."""
    run = _run(step)
    uses = step.get("uses") or ""
    return "dist/" in run or "pypi-publish" in uses or "twine" in run


@pytest.fixture(scope="module")
def dist_jobs():
    jobs = _dist_building_jobs()
    assert len(jobs) >= 2, (
        "expected the two publish workflows to build a dist; derived "
        f"{[(w, j) for w, j, _ in jobs]} -- an empty derivation is not a pass"
    )
    return jobs


class TestThePublishWorkflowsRunTheGuard:
    def test_the_script_exists_where_the_workflows_call_it(self):
        assert (REPO_ROOT / "scripts" / "check_dist_contents.py").is_file()

    def test_both_publish_workflows_are_derived(self, dist_jobs):
        names = {w for w, _, _ in dist_jobs}
        assert {"publish-main.yml", "publish-dev.yml"} <= names, names

    def test_every_dist_build_is_guarded_before_install_or_upload(self, dist_jobs):
        for workflow, job, steps in dist_jobs:
            build = [i for i, s in enumerate(steps) if _builds_dist(s)]
            guard = [i for i, s in enumerate(steps) if _is_guard(s)]
            consume = [i for i, s in enumerate(steps) if _consumes_dist(s)]
            where = f"{workflow}:{job}"
            assert len(guard) == 1, f"{where}: expected one guard step, found {guard}"
            assert (
                consume
            ), f"{where}: nothing installs or uploads dist/, so the guard protects nothing"
            assert build[-1] < guard[0], f"{where}: the guard runs before the build"
            assert guard[0] < min(consume), (
                f"{where}: the guard (step {guard[0]}) runs after a step that "
                f"consumes dist/ (steps {consume})"
            )

    def test_the_guard_step_cannot_be_skipped_or_softened(self, dist_jobs):
        for workflow, job, steps in dist_jobs:
            step = next(s for s in steps if _is_guard(s))
            where = f"{workflow}:{job}"
            assert not step.get("continue-on-error"), f"{where}: guard is advisory"
            assert "if" not in step, f"{where}: guard is conditional"
            assert (
                _run(step).strip().startswith("python ")
            ), f"{where}: the guard should run under the job's python: {_run(step)!r}"
