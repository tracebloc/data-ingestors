"""Regression tests for #105 — ``_has_extension`` must recognize valid
suffixes.

Before the fix, ``_has_extension`` did:

    ext = filename.split(".")[-1]   # "jpeg" — no leading dot
    return ext in allowed_extensions  # [".jpeg", ".jpg", ...]

The membership check never matched (the allowed list has leading
dots; the suffix doesn't), so the function always returned False.
``_find_src`` then appended the configured extension a SECOND time,
producing ``cat1.jpeg.jpeg`` paths that didn't exist on disk and
silently failed every file_transfer.

These tests pin the three classes of input that need to be handled:

1. Filename with a valid extension (lowercase) → True
2. Filename with a valid extension (uppercase) → True (case-insensitive)
3. Filename without any extension → False
4. Filename with a non-image extension (e.g. ``.tar``) → False
5. Edge cases: empty string, leading dot only, multiple dots
"""

from __future__ import annotations

import pytest

from tracebloc_ingestor.file_transfer import _has_extension


# ---------------------------------------------------------------------------
# Positive cases — the regression that broke real ingestion runs.
# ---------------------------------------------------------------------------


def test_lowercase_jpeg_recognized():
    """The exact filename pattern that failed in cluster validation
    2026-05-19: ``cat1.jpeg`` from a CSV with ``filename,label``
    columns."""
    assert _has_extension("cat1.jpeg") is True


def test_lowercase_jpg_recognized():
    """``.jpg`` and ``.jpeg`` are both in FileExtension; both must work."""
    assert _has_extension("photo.jpg") is True


def test_lowercase_png_recognized():
    assert _has_extension("mask.png") is True


def test_uppercase_extension_recognized():
    """Case-insensitive matching, consistent with
    ImageResolutionValidator._is_image_file. Customers don't always
    name their files lowercase, especially when sourced from a
    DSLR or scanner."""
    assert _has_extension("CAT1.JPEG") is True
    assert _has_extension("Photo.Jpg") is True


def test_xml_annotation_recognized():
    """Object detection ships Pascal VOC XML — also in FileExtension."""
    assert _has_extension("annotation.xml") is True


def test_txt_recognized():
    """Text classification sidecars."""
    assert _has_extension("review_001.txt") is True


# ---------------------------------------------------------------------------
# Negative cases — must still return False.
# ---------------------------------------------------------------------------


def test_no_extension_returns_false():
    """A bare name with no dots — ``_find_src`` will correctly append
    the configured extension."""
    assert _has_extension("cat1") is False


def test_unknown_extension_returns_false():
    """Extensions outside FileExtension's allowlist (e.g. archive
    formats, video) must return False so ``_find_src`` rejects them
    rather than appending a second extension."""
    assert _has_extension("archive.tar") is False
    assert _has_extension("video.mp4") is False


def test_empty_string_returns_false():
    """Defensive: caller might pass empty string from a malformed
    CSV row."""
    assert _has_extension("") is False


# ---------------------------------------------------------------------------
# Tricky paths with multiple dots — the original ``split(".")``
# logic was correct on these for length reasons; ``rsplit(".", 1)``
# preserves that.
# ---------------------------------------------------------------------------


def test_dotted_basename_recognized():
    """``my.file.jpeg`` — multiple dots but a valid trailing
    extension. ``rsplit(".", 1)`` gives the last suffix, which is
    what we want."""
    assert _has_extension("my.file.jpeg") is True


def test_dotted_basename_with_unknown_suffix_returns_false():
    """Trailing suffix is the only one that counts. If the last
    segment isn't in the allowlist, the function correctly returns
    False even if an earlier segment looks like an extension."""
    assert _has_extension("data.jpeg.tar") is False
