"""Regression tests for #104 — tuple-vs-list comparison in
``_resolution_matches``.

Before the fix, ``ImageResolutionValidator._resolution_matches`` did
``actual == expected`` when ``tolerance == 0``. ``actual`` comes from
PIL's ``image.size`` (a tuple); ``expected`` comes from YAML's
``target_size: [H, W]`` (a list). Python's sequence equality is
type-strict — ``(256, 256) == [256, 256]`` is False — so the
default-tolerance path rejected every image whose dimensions matched
exactly.

These tests pin the tolerance==0 path against the four shape
combinations (tuple/list × actual/expected) so the regression can't
sneak back via either side switching types.
"""

from __future__ import annotations

import pytest

from tracebloc_ingestor.validators.image_validator import ImageResolutionValidator


@pytest.fixture
def validator():
    """Tolerance==0 validator pre-seeded with a 512x512 expected size.

    Each test overrides ``expected_resolution`` on this instance as
    needed; sharing the fixture keeps the tests focused on the
    comparison behavior rather than constructor plumbing.
    """
    v = ImageResolutionValidator(expected_resolution=(512, 512))
    assert v.tolerance == 0, "Default tolerance changed; tests need an update"
    return v


# ---------------------------------------------------------------------------
# Exact matches across every tuple/list combination — these are the
# regression cases. Before the fix only tuple==tuple returned True.
# ---------------------------------------------------------------------------


def test_tuple_actual_list_expected_matches(validator):
    """The exact shape combo that failed in cluster validation 2026-05-19:
    PIL gives a tuple, YAML gives a list."""
    assert validator._resolution_matches((256, 256), [256, 256]) is True


def test_list_actual_tuple_expected_matches(validator):
    """Reverse — defensive in depth in case a caller pre-converts the
    PIL side to a list before passing in."""
    assert validator._resolution_matches([256, 256], (256, 256)) is True


def test_tuple_tuple_matches(validator):
    """The path that worked even before the fix — pinned so a
    regression in normalization doesn't break it."""
    assert validator._resolution_matches((256, 256), (256, 256)) is True


def test_list_list_matches(validator):
    """All-list path — exercises the tuple(actual)==tuple(expected)
    normalization on both sides."""
    assert validator._resolution_matches([256, 256], [256, 256]) is True


# ---------------------------------------------------------------------------
# Genuine mismatches — must still return False after the fix. (The bug
# was over-rejection, so the risk of an over-correction is False
# positives going green.)
# ---------------------------------------------------------------------------


def test_mismatch_returns_false_at_tolerance_zero(validator):
    """A real dimension mismatch must still be rejected."""
    assert validator._resolution_matches((512, 512), [256, 256]) is False
    assert validator._resolution_matches((256, 256), [512, 512]) is False


def test_mismatch_off_by_one_returns_false_at_tolerance_zero(validator):
    """At tolerance 0, even a 1-pixel difference is a mismatch — the
    tolerance>0 branch is a separate code path."""
    assert validator._resolution_matches((256, 256), [256, 257]) is False
    assert validator._resolution_matches((256, 257), [256, 256]) is False


# ---------------------------------------------------------------------------
# Tolerance>0 path — pinned so we don't regress the branch that was
# already working before the fix.
# ---------------------------------------------------------------------------


def test_tolerance_branch_still_works_for_within_tolerance():
    """The tolerance>0 branch already worked because it uses index
    access. Pin that it still does."""
    v = ImageResolutionValidator(expected_resolution=(256, 256))
    v.tolerance = 5
    assert v._resolution_matches((258, 254), [256, 256]) is True


def test_tolerance_branch_rejects_outside_tolerance():
    """And rejects when the difference exceeds the tolerance."""
    v = ImageResolutionValidator(expected_resolution=(256, 256))
    v.tolerance = 5
    assert v._resolution_matches((270, 256), [256, 256]) is False
