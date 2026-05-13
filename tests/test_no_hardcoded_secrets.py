"""Guard against re-introducing hardcoded credentials in the source tree.

Per the #43 acceptance criteria: *"No hardcoded passwords remain in the
source tree (grep confirms)."* This test enforces that programmatically
so the next regression is caught at PR time, not in production.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


# Strings that previously shipped as hardcoded defaults for the **backend**
# (tracebloc API) credentials in `tracebloc_ingestor/config.py`. These are
# real per-customer credentials — shipping a default value made every install
# ship the same secret. If this reappears anywhere in the package source, the
# test fails, even in a comment.
#
# NOTE: `DB_PASSWORD` ("Edg9@Tr@ce") and `DB_USER` ("edgeuser") are
# intentionally **not** in these lists. They're connection conventions for
# the cluster-internal MySQL container, which bakes the same values into its
# own image. They never vary per customer and don't leave the cluster, so
# shipping them as defaults in `config.py` is correct, not a leak. See the
# database-section comment in `config.py` and `Config.validate()` for the
# rationale.
KNOWN_LEAKED_SECRETS = (
    "&6edg*D9e16",
)

# Username default for the backend (tracebloc API) credential. Not secret on
# its own, but paired with the leaked password above it formed a working
# credential in legacy local dev — shouldn't be baked into source either.
LEGACY_USERNAME_DEFAULTS = (
    "testedge",
)


PACKAGE_ROOT = Path(__file__).resolve().parent.parent / "tracebloc_ingestor"


def _iter_source_files() -> list[Path]:
    """All Python sources under the package; tests/examples excluded."""
    return [p for p in PACKAGE_ROOT.rglob("*.py") if p.is_file()]


@pytest.mark.parametrize("secret", KNOWN_LEAKED_SECRETS)
def test_known_leaked_passwords_absent(secret: str):
    offenders = [
        str(p.relative_to(PACKAGE_ROOT.parent))
        for p in _iter_source_files()
        if secret in p.read_text(encoding="utf-8")
    ]
    assert not offenders, (
        f"Hardcoded password {secret!r} reappeared in: {offenders}. "
        "Read it from an env var instead and let Config.validate() fail fast."
    )


@pytest.mark.parametrize("username", LEGACY_USERNAME_DEFAULTS)
def test_legacy_username_defaults_absent(username: str):
    pattern = re.compile(rf'["\']{re.escape(username)}["\']')
    offenders = [
        str(p.relative_to(PACKAGE_ROOT.parent))
        for p in _iter_source_files()
        if pattern.search(p.read_text(encoding="utf-8"))
    ]
    assert not offenders, (
        f"Legacy username default {username!r} reappeared as a string literal "
        f"in: {offenders}. Read from env (CLIENT_ID) instead."
    )
