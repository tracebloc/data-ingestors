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
# `Edg9@Tr@ce` is the legacy root-equivalent `edgeuser` password that DB_USER/
# DB_PASSWORD used to fall back to. That fallback was removed in backend#1528
# (D10 close-out): jobs-manager now injects per-Job tb_ingest credentials, so
# the ingestor no longer connects as edgeuser and this password must never be
# baked into source again. (`edgeuser` the username is guarded below.)
KNOWN_LEAKED_SECRETS = (
    "&6edg*D9e16",
    "Edg9@Tr@ce",
)

# Usernames that must not be baked into source. `testedge` was a legacy
# backend (tracebloc API) default that — paired with the leaked password
# above — formed a working credential in local dev. `edgeuser` is the
# root-equivalent MySQL identity retired in backend#1528; DB_USER now comes
# from the injected per-Job env, never a hardcoded default.
LEGACY_USERNAME_DEFAULTS = (
    "testedge",
    "edgeuser",
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
