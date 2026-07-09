"""Tests for the minimum-image-size floor (#348, RFC-0002 §12.9 + Principle 6).

Before this change there was no lower bound on image dimensions: a uniform
dataset of 8x8 (or 1x1) images sailed through ``ImageResolutionValidator``
because the only size check was uniformity against ``target_size``. These tests
pin the absolute floor — rejection below the minimum, a normal image passing,
the exact boundary, the ``min_size`` override, and that a ``file_options``
override reaches the validator (the discovery/preview contract cli#183 relies
on).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from PIL import Image

from tracebloc_ingestor.utils.validators_mapping import map_validators
from tracebloc_ingestor.validators.image_validator import (
    MIN_IMAGE_SIZE,
    ImageResolutionValidator,
)


@pytest.fixture
def images_dir(tmp_path):
    """Return a factory that creates <tmp>/images/<name> at a given size."""
    d = tmp_path / "images"
    d.mkdir()

    def _add(name, size, color=(120, 120, 120)):
        Image.new("RGB", size, color).save(d / name)
        return d / name

    return tmp_path, _add


# --- default floor: reject too-small, pass normal, hold the boundary --------


def test_too_small_uniform_dataset_is_rejected(clean_env, images_dir):
    """The regression this ticket targets: a uniform 8x8 dataset used to pass
    (auto-detected expected == 8x8, all match). It must now be rejected with an
    actionable 'too small' message naming the file, its dimensions and the floor."""
    src, add = images_dir
    add("tiny.jpg", (8, 8))
    clean_env.setenv("SRC_PATH", str(src))
    result = ImageResolutionValidator().validate(None)
    assert not result.is_valid
    (msg,) = result.errors
    assert "below the minimum size" in msg
    assert "(32, 32)" in msg  # the floor
    assert "tiny.jpg" in msg  # the offending file
    assert "(8, 8)" in msg  # its dimensions
    assert result.metadata["min_size"] == (32, 32)
    assert result.metadata["too_small"]


def test_normal_image_passes(clean_env, images_dir):
    """A comfortably-above-floor dataset is unaffected by the gate."""
    src, add = images_dir
    add("a.jpg", (64, 64))
    add("b.jpg", (64, 64))
    clean_env.setenv("SRC_PATH", str(src))
    result = ImageResolutionValidator().validate(None)
    assert result.is_valid, result.errors
    assert result.metadata["min_size"] == (32, 32)


def test_boundary_exactly_at_floor_passes(clean_env, images_dir):
    """An image whose sides equal the floor exactly is accepted (>=, not >)."""
    src, add = images_dir
    add("edge.jpg", (32, 32))
    clean_env.setenv("SRC_PATH", str(src))
    result = ImageResolutionValidator().validate(None)
    assert result.is_valid, result.errors


@pytest.mark.parametrize("size", [(31, 32), (32, 31)])
def test_boundary_one_below_floor_on_either_axis_is_rejected(
    clean_env, images_dir, size
):
    """One pixel below the floor on EITHER axis is rejected — the floor is
    checked per-dimension, independently of target_size uniformity."""
    src, add = images_dir
    add("just_under.jpg", size)
    clean_env.setenv("SRC_PATH", str(src))
    result = ImageResolutionValidator().validate(None)
    assert not result.is_valid
    assert "below the minimum size" in result.errors[0]


# --- override -----------------------------------------------------------------


def test_lower_override_admits_smaller_images(clean_env, images_dir):
    """A per-model override may lower the floor for models that accept smaller
    inputs — 16x16 images pass once min_size is dropped to (16, 16)."""
    src, add = images_dir
    add("a.png", (16, 16))
    clean_env.setenv("SRC_PATH", str(src))
    result = ImageResolutionValidator(min_size=(16, 16)).validate(None)
    assert result.is_valid, result.errors


def test_higher_override_rejects_below_it(clean_env, images_dir):
    """A raised floor rejects images that clear the default but not the override."""
    src, add = images_dir
    add("a.png", (32, 32))
    clean_env.setenv("SRC_PATH", str(src))
    result = ImageResolutionValidator(min_size=(64, 64)).validate(None)
    assert not result.is_valid
    assert "(64, 64)" in result.errors[0]


def test_falsy_override_falls_back_to_default_floor():
    """A None / empty override never silently disables the gate."""
    assert ImageResolutionValidator(min_size=None).min_size == MIN_IMAGE_SIZE
    assert ImageResolutionValidator().min_size == MIN_IMAGE_SIZE


@pytest.mark.parametrize("bad", [32, (16,), (16, 16, 16)])
def test_malformed_min_size_raises_at_construction(bad):
    """A non-iterable or non-2-element override is a config error surfaced at
    construction — not swallowed mid-scan as a per-file image-read error."""
    with pytest.raises(ValueError, match="min_size must be a"):
        ImageResolutionValidator(min_size=bad)


# --- floor takes precedence over a uniformity/target mismatch -----------------


def test_floor_precedes_uniformity_error(clean_env, images_dir):
    """When a dataset is both too-small and non-uniform, the floor error wins —
    it names the more fundamental problem (the images can't be trained on)."""
    src, add = images_dir
    add("small.jpg", (8, 8))
    add("big.jpg", (64, 64))
    clean_env.setenv("SRC_PATH", str(src))
    result = ImageResolutionValidator().validate(None)
    assert not result.is_valid
    assert "below the minimum size" in result.errors[0]
    assert "small.jpg" in result.errors[0]


def test_too_small_result_still_reports_corrupt_files(clean_env, images_dir):
    """When a dataset is both too-small and partly corrupt, the floor error
    wins but the corrupt files are still surfaced in metadata — otherwise they
    stay hidden until the user fixes the tiny images and re-runs."""
    src, add = images_dir
    add("tiny.jpg", (8, 8))
    (src / "images" / "broken.jpg").write_bytes(b"not a real jpeg")
    clean_env.setenv("SRC_PATH", str(src))
    result = ImageResolutionValidator().validate(None)
    assert not result.is_valid
    assert "below the minimum size" in result.errors[0]
    assert any("broken.jpg" in f for f in result.metadata["invalid_files"])


# --- _meets_min_size unit ----------------------------------------------------


def test_meets_min_size_normalizes_list_override():
    """A list-typed override (as parsed from JSON/YAML file_options) compares by
    value, like _resolution_matches — (32, 32) meets a [32, 32] floor."""
    v = ImageResolutionValidator(min_size=[32, 32])
    assert v._meets_min_size((32, 32)) is True
    assert v._meets_min_size((31, 32)) is False
    assert v._meets_min_size((32, 31)) is False
    assert v._meets_min_size((1920, 1080)) is True


# --- file_options.min_size reaches the validator (cli#183 discovery/preview) --


def test_file_options_min_size_reaches_validator():
    """map_validators must thread ``file_options["min_size"]`` into the image
    validator so the CLI's preview reads the SAME floor the ingestor enforces."""
    validators = map_validators(
        "image_classification",
        {"target_size": [256, 256], "extension": ".jpg", "min_size": [64, 64]},
    )
    image_validators = [
        v for v in validators if isinstance(v, ImageResolutionValidator)
    ]
    assert image_validators, "expected an ImageResolutionValidator in the set"
    assert all(v.min_size == (64, 64) for v in image_validators)


def test_default_when_no_file_option():
    """Without an override the validator uses the documented default floor."""
    validators = map_validators(
        "image_classification",
        {"target_size": [256, 256], "extension": ".jpg"},
    )
    image_validators = [
        v for v in validators if isinstance(v, ImageResolutionValidator)
    ]
    assert image_validators
    assert all(v.min_size == MIN_IMAGE_SIZE for v in image_validators)


# --- schema documents the field (the CLI's discovery surface) -----------------


def test_schema_documents_min_size():
    schema_path = (
        Path(__file__).resolve().parents[1]
        / "tracebloc_ingestor"
        / "schema"
        / "ingest.v1.json"
    )
    schema = json.loads(schema_path.read_text())
    file_options = schema["properties"]["spec"]["properties"]["file_options"]
    assert "min_size" in file_options["properties"]
