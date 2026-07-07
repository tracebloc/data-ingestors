"""Import-check over ``examples/*.py`` (#331).

``Readme.md`` points customers at ``examples/`` for working scripts, but the
scripts aren't imported anywhere in the suite, so API refactors could break
them silently — three of them crashed on import for months after the
``processors/`` package was removed. This test imports every example module
so a stale import (or module-level side effect that requires a live DB /
backend) fails CI instead of the first customer who copies the script.

Example scripts must therefore keep real work (``Database`` / ``APIClient``
construction, ingestion) inside ``main()`` — module level is only for
imports, ``Config()``, logging setup, and option dicts, matching the
``templates/*.py`` convention.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_ROOT / "examples"

EXAMPLE_SCRIPTS = sorted(EXAMPLES_DIR.glob("*.py"))


def test_examples_dir_has_scripts():
    """Guard the parametrization: an empty glob must fail, not pass silently."""
    assert EXAMPLE_SCRIPTS, f"no example scripts found under {EXAMPLES_DIR}"


@pytest.mark.parametrize("script", EXAMPLE_SCRIPTS, ids=lambda p: p.name)
def test_example_imports_cleanly(script: Path, clean_env, tmp_path, monkeypatch):
    """Every script in examples/ must import against the current package API."""
    # Imports must succeed in a bare environment (no backend creds, no DB) —
    # exactly what a customer gets when they first open the examples.
    monkeypatch.chdir(tmp_path)

    module_name = f"_example_import_check_{script.stem}"
    spec = importlib.util.spec_from_file_location(module_name, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(module_name, None)


def test_example_data_paths_exist():
    """Every `... / "data" / "<file>"` an example references must ship in the
    repo — a dangling path means the documented example cannot run as
    shipped (Bugbot on #330: blob_documents_sample.csv was missing)."""
    import re
    from pathlib import Path

    examples_dir = Path(__file__).resolve().parent.parent / "examples"
    missing = []
    for script in sorted(examples_dir.glob("*.py")):
        for name in re.findall(r'/\s*"data"\s*/\s*"([^"]+)"', script.read_text()):
            if not (examples_dir / "data" / name).is_file():
                missing.append(f"{script.name} -> data/{name}")
    assert not missing, f"examples reference data files that don't exist: {missing}"
