"""Guard the single-sourced package version.

``tracebloc_ingestor/__init__.py`` holds the one and only version literal;
``setup.py`` parses it at build time (``_read_version``) so the importable
``__version__`` and the packaged/PyPI version cannot drift. They drifted once
(``setup.py`` 0.3.5 vs ``__version__`` 0.3.4, #171/#175) precisely because the
bump touched only one file — these tests fail loudly if that contract breaks
again, e.g. someone re-hardcodes a literal back into ``setup.py``.

Pure file/process checks — no DB, no network.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import tracebloc_ingestor

REPO_ROOT = Path(__file__).resolve().parent.parent
INIT_PY = REPO_ROOT / "tracebloc_ingestor" / "__init__.py"

# The exact pattern setup.py uses to extract the version. Kept in sync on
# purpose: this test pins the contract setup.py's _read_version() relies on.
_VERSION_RE = re.compile(r'^__version__\s*=\s*["\']([^"\']+)["\']', re.M)


def _parse_init_version() -> str:
    match = _VERSION_RE.search(INIT_PY.read_text())
    assert match is not None, "__version__ literal not found in __init__.py"
    return match.group(1)


def test_version_is_a_pep440_ish_literal() -> None:
    """The runtime attribute is a non-empty, regex-parseable version string."""
    assert isinstance(tracebloc_ingestor.__version__, str)
    assert re.fullmatch(r"\d+\.\d+\.\d+([.\-+].+)?", tracebloc_ingestor.__version__)
    # The literal setup.py will parse must equal the runtime attribute.
    assert _parse_init_version() == tracebloc_ingestor.__version__


def test_setup_py_resolves_to_init_version() -> None:
    """``python setup.py --version`` derives the same version as __init__.py.

    This is the real anti-drift guard: if setup.py ever stops deriving the
    version (e.g. a hardcoded ``version="x.y.z"`` creeps back in), the two
    diverge and this fails.
    """
    result = subprocess.run(
        [sys.executable, "setup.py", "--version"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    # setuptools prints the version to stdout; deprecation noise goes to stderr.
    reported = result.stdout.strip().splitlines()[-1].strip()
    assert reported == tracebloc_ingestor.__version__
