"""The one definition of "this category stages no manifest".

TWO GUARDS DISAGREED ABOUT THIS AND ONE OF THEM LIED.
`test_cross_schema_agreement` treated a `null`/absent manifest as
manifest-less -- deliberately, since `e2e-test-agent#428` accepts either
spelling -- while `test_sidecar_trait_agrees_with_dispatch` recognised only
`kind == "none"`. So a `null` manifest read as *declares a manifest* in one
file and *declares none* in the other.

Measured on this branch (LukasWodka, data-ingestors#557): make `_manifest()`
return `None` for `spec.records_from_sidecar` -- the alternative shape the
sibling file documents as legal -- and `test_cross_schema_agreement` stays
GREEN while the trait test fails with

    object_detection: layout says kind=None but ingest.v1.json forbids a
    csv source

Two guards over one fact, one red and one green, and the red one's message
pointing at the wrong thing. Worse than a single wrong guard, because the
disagreement makes the green one look like corroboration.

So the predicate lives here once. Both spellings count as manifest-less:
`{"kind": "none"}` is what this repo publishes (backend#3076), `null` was the
alternative considered on backend#3110 and rejected -- but the e2e reader
accepts either, so a consumer can legally be handed either and a guard that
knew only one would be asserting on our preference.
"""

from __future__ import annotations

from typing import Optional

#: The sentinel `kind` meaning "no manifest". A new *value* rather than a
#: dropped field, so a consumer switching on `kind` must grow a branch.
MANIFEST_LESS_KIND = "none"


def declares_a_manifest(entry: dict) -> bool:
    """Does this task-layout entry describe a manifest the user stages?

    False for BOTH spellings of manifest-less. Absent counts as absent, not as
    a declaration -- an entry with no `manifest` key is a contract shape this
    reader does not know, and treating it as "declares one" would make a
    truncated contract look normal.
    """
    manifest = entry.get("manifest")
    if manifest is None:
        return False
    return manifest.get("kind") != MANIFEST_LESS_KIND


def manifest_kind(entry: dict) -> Optional[str]:
    """The declared kind, or None for either manifest-less spelling."""
    if not declares_a_manifest(entry):
        return None
    return (entry.get("manifest") or {}).get("kind")
