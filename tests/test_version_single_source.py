"""Guard the single-sourced package version.

``tracebloc_ingestor/__init__.py`` holds the one and only version literal;
``setup.py`` parses it at build time (``_read_version``) so the importable
``__version__`` and the packaged/PyPI version cannot drift. They drifted once
(``setup.py`` 0.3.5 vs ``__version__`` 0.3.4, #171/#175) precisely because the
bump touched only one file — these tests fail loudly if that contract breaks
again, e.g. someone re-hardcodes a literal back into ``setup.py``.

Pure file checks — no DB, no network, no subprocess.
"""

from __future__ import annotations

import re
from pathlib import Path

import tracebloc_ingestor

REPO_ROOT = Path(__file__).resolve().parent.parent
INIT_PY = REPO_ROOT / "tracebloc_ingestor" / "__init__.py"
SETUP_PY = REPO_ROOT / "setup.py"

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


def test_setup_py_derives_version_not_hardcoded() -> None:
    """``setup.py`` must derive ``version=`` from ``__init__.py``, not hardcode it.

    This is the real anti-drift guard: if a hardcoded ``version="x.y.z"`` ever
    creeps back into ``setup.py`` (the exact drift #171/#175 hit), the two
    files diverge and this fails.

    Static analysis (not a subprocess) so the test does not require setuptools
    at runtime — Python 3.12 dropped the auto-bundled setuptools, so the
    previous ``python setup.py --version`` subprocess broke under CI's bare
    interpreter even though the contract itself was fine.
    """
    source = SETUP_PY.read_text()
    # Anything matching version="literal" / version='literal' is hardcoded.
    hardcoded = re.search(r'version\s*=\s*["\'][^"\']+["\']', source)
    assert hardcoded is None, (
        f"setup.py hardcodes a version literal ({hardcoded.group(0)!r}); "
        "it must derive from tracebloc_ingestor/__init__.py via _read_version()."
    )
    # And it must actually call the derivation helper (or an equivalent).
    assert "_read_version()" in source, (
        "setup.py no longer calls _read_version(); the single-source contract "
        "is broken."
    )
