"""Refuse a wheel or sdist that ships anything but ``tracebloc_ingestor``.

WHY THIS EXISTS
---------------
``setup.py`` used to declare ``packages=find_packages()`` with no ``include=``
or ``exclude=``. ``tests/`` has an ``__init__.py``, so ``find_packages()``
returned ``["tests", "tracebloc_ingestor", ...]`` and every published release
carried the whole unit-test suite: 100 members and 51 % of the compressed wheel
in 0.8.18, plus a top-level package literally named ``tests`` that
``pip install`` dropped into site-packages, where it collides with (and is
silently overwritten by) any other distribution making the same mistake.

Nothing noticed, because nothing looked at the artefact. The publish workflows
built ``dist/``, installed the sdist, smoke-imported one function and uploaded.
A test that parsed ``setup.py`` would only have restated the fix; the built
artefact is the truth, so this reads the artefact.

WHAT IT CHECKS, ON THE BUILT FILES
----------------------------------
1. ``dist/`` holds exactly one ``.whl`` and exactly one ``.tar.gz``. Anything
   else -- zero, two, a stray egg -- is a refusal, because the workflow's
   ``pip install dist/*.tar.gz`` and the upload glob would otherwise pick one
   at random.
2. The wheel's ``*.dist-info/top_level.txt`` (and the sdist's
   ``*.egg-info/top_level.txt``) names exactly ``tracebloc_ingestor``.
3. No member of either artefact has a path component ``tests`` or is a
   ``conftest.py``.
4. Every ``tracebloc_ingestor/schema/*.json`` in the source tree is present in
   both artefacts. The fix narrows ``find_packages`` with an ``include=``, and
   an over-eager include is exactly how the schema files could silently stop
   shipping -- the same class as the v0.3.0-rc1 schema-missing bug.

FAIL CLOSED. An artefact that cannot be opened, a ``top_level.txt`` that is
missing, a source tree with no schema files to compare against: each is a
refusal, never a pass. "Cannot tell" is a finding.

Exit 0 when clean, exit 2 with every offending path named otherwise. Stdlib
only, so it runs in the publish job before any dependency is installed.
"""

from __future__ import annotations

import argparse
import sys
import tarfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Iterable, List, Optional, Set

#: The one importable top-level name a release may carry.
EXPECTED_TOP_LEVEL = frozenset({"tracebloc_ingestor"})

#: A path component that marks a member as test code, wherever it sits.
FORBIDDEN_COMPONENTS = frozenset({"tests"})

#: A basename that marks a member as pytest plumbing, wherever it sits.
FORBIDDEN_BASENAMES = frozenset({"conftest.py"})

#: Where the JSON contracts live, relative to the package root. Read from the
#: source tree at run time -- the set of files is derived, not listed here.
SCHEMA_DIR = PurePosixPath("tracebloc_ingestor/schema")

REFUSE = 2


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def required_schema_files(source_dir: Path) -> List[str]:
    """``tracebloc_ingestor/schema/*.json`` as found in the source tree.

    Derived from disk rather than restated, so a schema added later is required
    of the artefacts automatically. An empty answer is the caller's problem to
    refuse -- a tree with no schema files cannot be what this repo looks like.
    """
    schema_dir = source_dir / SCHEMA_DIR
    return sorted(
        (SCHEMA_DIR / p.name).as_posix()
        for p in schema_dir.glob("*.json")
        if p.is_file()
    )


def _wheel_members(path: Path) -> List[str]:
    with zipfile.ZipFile(path) as zf:
        return [n for n in zf.namelist() if not n.endswith("/")]


def _wheel_top_level(path: Path) -> Optional[Set[str]]:
    """The names in ``*.dist-info/top_level.txt``; ``None`` if there is none."""
    with zipfile.ZipFile(path) as zf:
        for name in zf.namelist():
            parts = PurePosixPath(name).parts
            if (
                len(parts) == 2
                and parts[0].endswith(".dist-info")
                and parts[1] == "top_level.txt"
            ):
                return _parse_top_level(zf.read(name))
    return None


def _sdist_members(path: Path) -> List[str]:
    with tarfile.open(path, "r:gz") as tf:
        return [m.name.rstrip("/") for m in tf.getmembers() if m.name not in (".", "")]


def _sdist_top_level(path: Path) -> Optional[Set[str]]:
    """The names in ``<root>/*.egg-info/top_level.txt``; ``None`` if none."""
    with tarfile.open(path, "r:gz") as tf:
        for member in tf.getmembers():
            parts = PurePosixPath(member.name).parts
            if (
                len(parts) == 3
                and parts[1].endswith(".egg-info")
                and parts[2] == "top_level.txt"
                and member.isfile()
            ):
                fh = tf.extractfile(member)
                if fh is None:
                    return None
                return _parse_top_level(fh.read())
    return None


def _parse_top_level(raw: bytes) -> Set[str]:
    return {line.strip() for line in raw.decode("utf-8").splitlines() if line.strip()}


def forbidden_members(members: Iterable[str]) -> List[str]:
    """Every member path that is test code by component or by basename."""
    out = []
    for name in members:
        parts = PurePosixPath(name).parts
        if not parts:
            continue
        if FORBIDDEN_COMPONENTS.intersection(parts) or parts[-1] in FORBIDDEN_BASENAMES:
            out.append(name)
    return out


def _strip_sdist_root(members: List[str]) -> List[str]:
    """Drop the single ``name-version/`` directory every sdist member sits under."""
    return [
        PurePosixPath(*PurePosixPath(m).parts[1:]).as_posix()
        for m in members
        if len(PurePosixPath(m).parts) > 1
    ]


def check_dist(dist_dir: Path, source_dir: Optional[Path] = None) -> List[str]:
    """Return every reason to refuse the artefacts in ``dist_dir``.

    Empty means clean. Every string names the file and the offending path or
    the thing that could not be read. This is the one function the CLI and the
    tests call; there is no second copy of the rule anywhere.
    """
    source_dir = source_dir or _repo_root()
    findings: List[str] = []

    if not dist_dir.is_dir():
        return [f"{dist_dir}: not a directory -- nothing was built"]

    wheels = sorted(dist_dir.glob("*.whl"))
    sdists = sorted(dist_dir.glob("*.tar.gz"))
    others = sorted(
        p for p in dist_dir.iterdir() if p not in wheels and p not in sdists
    )
    if len(wheels) != 1:
        findings.append(
            f"{dist_dir}: expected exactly one .whl, found {len(wheels)}: "
            f"{[p.name for p in wheels]}"
        )
    if len(sdists) != 1:
        findings.append(
            f"{dist_dir}: expected exactly one .tar.gz, found {len(sdists)}: "
            f"{[p.name for p in sdists]}"
        )
    if others:
        findings.append(
            f"{dist_dir}: unexpected entries beside the wheel and sdist: "
            f"{[p.name for p in others]}"
        )
    if findings:
        # No point reading artefacts we cannot even identify.
        return findings

    required = required_schema_files(source_dir)
    if not required:
        findings.append(
            f"{source_dir / SCHEMA_DIR}: no *.json schema files found in the "
            "source tree, so their presence in the artefacts cannot be checked"
        )

    wheel, sdist = wheels[0], sdists[0]

    # ---- wheel -----------------------------------------------------------
    try:
        wheel_members = _wheel_members(wheel)
        wheel_top = _wheel_top_level(wheel)
    except (zipfile.BadZipFile, OSError, UnicodeDecodeError) as exc:
        findings.append(f"{wheel.name}: unreadable wheel ({exc!r})")
    else:
        if wheel_top is None:
            findings.append(f"{wheel.name}: no *.dist-info/top_level.txt in the wheel")
        elif wheel_top != set(EXPECTED_TOP_LEVEL):
            findings.append(
                f"{wheel.name}: top_level.txt is {sorted(wheel_top)}, "
                f"expected {sorted(EXPECTED_TOP_LEVEL)}"
            )
        for name in forbidden_members(wheel_members):
            findings.append(f"{wheel.name}: ships test code: {name}")
        present = set(wheel_members)
        for name in required:
            if name not in present:
                findings.append(f"{wheel.name}: missing required file: {name}")

    # ---- sdist -----------------------------------------------------------
    try:
        sdist_members = _sdist_members(sdist)
        sdist_top = _sdist_top_level(sdist)
    except (tarfile.TarError, OSError, EOFError, UnicodeDecodeError) as exc:
        findings.append(f"{sdist.name}: unreadable sdist ({exc!r})")
    else:
        roots = {
            PurePosixPath(m).parts[0] for m in sdist_members if PurePosixPath(m).parts
        }
        if len(roots) != 1:
            findings.append(
                f"{sdist.name}: expected one top directory, found {sorted(roots)}"
            )
        if sdist_top is None:
            findings.append(f"{sdist.name}: no *.egg-info/top_level.txt in the sdist")
        elif sdist_top != set(EXPECTED_TOP_LEVEL):
            findings.append(
                f"{sdist.name}: top_level.txt is {sorted(sdist_top)}, "
                f"expected {sorted(EXPECTED_TOP_LEVEL)}"
            )
        for name in forbidden_members(sdist_members):
            findings.append(f"{sdist.name}: ships test code: {name}")
        present = set(_strip_sdist_root(sdist_members))
        for name in required:
            if name not in present:
                findings.append(f"{sdist.name}: missing required file: {name}")

    return findings


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Refuse a wheel/sdist that ships anything but tracebloc_ingestor."
    )
    parser.add_argument(
        "--dist",
        type=Path,
        default=None,
        help="directory holding the built artefacts (default: <repo>/dist)",
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=None,
        help="source tree to derive the required schema files from (default: <repo>)",
    )
    args = parser.parse_args(argv)
    source_dir = args.source or _repo_root()
    dist_dir = args.dist or (source_dir / "dist")

    findings = check_dist(dist_dir, source_dir)
    if findings:
        print(f"REFUSED: {dist_dir} is not a clean release of tracebloc_ingestor")
        for finding in findings:
            print(f"  - {finding}")
        return REFUSE

    wheel = next(dist_dir.glob("*.whl"))
    sdist = next(dist_dir.glob("*.tar.gz"))
    print(
        f"OK: {wheel.name} ({len(_wheel_members(wheel))} members) and "
        f"{sdist.name} ({len(_sdist_members(sdist))} members) carry only "
        f"{sorted(EXPECTED_TOP_LEVEL)}; {len(required_schema_files(source_dir))} "
        "schema files present in both"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
