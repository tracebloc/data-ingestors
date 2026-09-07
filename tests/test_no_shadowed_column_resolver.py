"""Structural guard: no validator may carry its own column resolver.

``tracebloc_ingestor.utils.columns.resolve_column`` is the single column-name
matching rule, reached by validators through
``BaseValidator._match_column``. Three validators once carried private
``_resolve_column`` copies instead, and two of them dropped the ``.strip()`` —
so ``token_classification`` / ``sentence_pair_classification`` / ``embeddings``
false-rejected the header Excel writes by default (``filename, label``) as a
missing column, while sibling validators in the SAME preflight run resolved it
(backend#1828, reintroducing #340 / bugbot #252).

The per-category regressions live next to each validator's own tests. This file
guards the *class* of bug rather than the three instances: a shadowing copy is
invisible to a reader — nothing about a call to ``self._resolve_column(...)``
suggests it bypasses the canonical rule — so the guard has to be mechanical.
It fails on ANY new private resolver, including a faithful one, because a
faithful copy is exactly how the broken ones got there (the third copy, in
``LabelDiversityValidator``, was faithful and was still deleted).
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil

import pytest

import tracebloc_ingestor.validators as validators_pkg
from tracebloc_ingestor.utils.columns import resolve_column
from tracebloc_ingestor.validators.base import BaseValidator


def _validator_classes():
    """Every BaseValidator subclass defined under tracebloc_ingestor.validators."""
    found = {}
    for mod_info in pkgutil.iter_modules(validators_pkg.__path__):
        module = importlib.import_module(f"{validators_pkg.__name__}.{mod_info.name}")
        for name, obj in vars(module).items():
            if (
                inspect.isclass(obj)
                and issubclass(obj, BaseValidator)
                and obj.__module__.startswith(validators_pkg.__name__)
            ):
                found[f"{obj.__module__}.{obj.__qualname__}"] = obj
    return found


# Names a private resolver has historically been spelled with. ``_match_column``
# is deliberately absent — that IS the sanctioned accessor.
_FORBIDDEN = ("_resolve_column", "_resolve_col", "_find_column", "_lookup_column")


def test_validator_classes_are_discovered():
    """Guard the guard: an import error or a renamed package would otherwise
    make the shadowing assertions below vacuously pass."""
    classes = _validator_classes()
    assert len(classes) >= 20, sorted(classes)
    assert any(c.endswith("BIOLabelValidator") for c in classes), sorted(classes)
    assert any(c.endswith("TabSeparatedRecordValidator") for c in classes), sorted(
        classes
    )
    assert any(c.endswith("LabelDiversityValidator") for c in classes), sorted(classes)


@pytest.mark.parametrize("attr", _FORBIDDEN)
def test_no_validator_defines_a_private_column_resolver(attr):
    """No validator class may define its own column resolver — route through
    ``BaseValidator._match_column`` (backend#1828)."""
    offenders = [
        qualname for qualname, cls in _validator_classes().items() if attr in vars(cls)
    ]
    assert not offenders, (
        f"{offenders} define a private {attr!r}. The column-matching rule is "
        f"single-sourced in tracebloc_ingestor.utils.columns.resolve_column; "
        f"call self._match_column(df.columns, name) instead. A private copy "
        f"silently diverges (backend#1828: two copies dropped the .strip() and "
        f"false-rejected a padded CSV header on 3 of 16 categories)."
    )


def test_match_column_delegates_to_the_canonical_rule():
    """``_match_column`` must BE the canonical rule, not a reimplementation of
    it — otherwise deleting the copies just moved the divergence up one level.

    Includes the padded-header case (``" label"`` resolved from ``"label"``)
    that the deleted copies got wrong, and the non-string column label that
    made them raise ``AttributeError`` where the canonical rule returns a hit.
    """
    cases = [
        (["filename", " label"], "label", " label"),
        (["label", " filename"], "filename", " filename"),
        (["filename ", " label"], "filename", "filename "),
        (["Label"], " label ", "Label"),
        (["label"], "LABEL", "label"),
        (["a", "b"], "missing", None),
        ([0, 1], "0", 0),
    ]
    for columns, name, expected in cases:
        assert BaseValidator._match_column(columns, name) == expected, (
            columns,
            name,
        )
        assert resolve_column(columns, name) == expected, (columns, name)
