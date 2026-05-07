"""Schema validation for the declarative ``ingest.yaml`` config (v1).

Covers the acceptance criteria that come from #44:

- Every example in ``examples/yaml/`` validates against ``schema/ingest.v1.json``
  (positive coverage for all task categories + the custom-processor escape
  hatch).
- Invalid ``category`` is rejected at validation, listing the valid options
  via the JSON Schema enum.
- Regression-class tasks missing ``label.policy`` are rejected at validation.
- Unknown top-level fields are rejected (catches typos like ``catagory:`` or
  ``lable:``).
- Image-based categories without ``images``, ``object_detection`` without
  ``annotations``, etc., are rejected.
- Exactly one of ``csv`` / ``json`` is required.

The tests don't run the entrypoint — that comes in a later commit's tests.
This file's job is to lock the shape of the YAML surface in isolation.
"""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft7Validator, ValidationError


REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "tracebloc_ingestor" / "schema" / "ingest.v1.json"
EXAMPLES_DIR = REPO_ROOT / "examples" / "yaml"


@pytest.fixture(scope="module")
def schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def validator(schema) -> Draft7Validator:
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema)


def _load_example(name: str) -> dict:
    return yaml.safe_load((EXAMPLES_DIR / name).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Positive coverage: every shipped example must validate.
# ---------------------------------------------------------------------------

EXAMPLES = sorted(p.name for p in EXAMPLES_DIR.glob("*.yaml"))


@pytest.mark.parametrize("example_name", EXAMPLES)
def test_example_validates(validator: Draft7Validator, example_name: str):
    config = _load_example(example_name)
    errors = sorted(validator.iter_errors(config), key=lambda e: e.path)
    assert not errors, (
        f"{example_name} failed schema validation:\n  "
        + "\n  ".join(f"{list(e.absolute_path) or '<root>'}: {e.message}" for e in errors)
    )


def test_image_classification_is_eight_lines():
    """The dominant case has to fit in ~8 lines, per #44 design constraint."""
    body = (EXAMPLES_DIR / "image_classification.yaml").read_text(encoding="utf-8")
    non_blank_non_comment = [
        line for line in body.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert len(non_blank_non_comment) == 8, (
        f"image_classification.yaml has {len(non_blank_non_comment)} payload "
        f"lines, expected 8. Customers should not need more for the dominant case."
    )


def test_all_task_categories_covered():
    """Every value in the schema's category enum has a corresponding example,
    except instance_segmentation which has no template/validator support yet."""
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    enum_values = set(schema["properties"]["category"]["enum"])

    covered = {
        yaml.safe_load((EXAMPLES_DIR / name).read_text(encoding="utf-8"))["category"]
        for name in EXAMPLES
        if name != "custom_processor.yaml"  # uses tabular_classification, already covered
    }

    # instance_segmentation is in the enum but has no map_validators branch
    # yet (no template either) — tracked separately.
    expected = enum_values - {"instance_segmentation"}
    missing = expected - covered
    assert not missing, f"No example for categories: {missing}"


# ---------------------------------------------------------------------------
# Negative coverage: each rejection path the schema is supposed to enforce.
# ---------------------------------------------------------------------------

def _ic_base() -> dict:
    """A known-good image_classification config to mutate per test."""
    return _load_example("image_classification.yaml")


def test_unknown_top_level_field_rejected(validator):
    config = _ic_base()
    config["lable"] = "image_label"  # typo
    with pytest.raises(ValidationError):
        validator.validate(config)


def test_invalid_category_rejected_with_enum_in_message(validator):
    config = _ic_base()
    config["category"] = "image_klassification"  # typo
    with pytest.raises(ValidationError) as exc_info:
        validator.validate(config)
    msg = str(exc_info.value.message)
    # The error must point users at the valid set so they can fix it.
    assert "image_classification" in msg


def test_missing_table_rejected(validator):
    config = _ic_base()
    del config["table"]
    with pytest.raises(ValidationError):
        validator.validate(config)


def test_missing_intent_rejected(validator):
    config = _ic_base()
    del config["intent"]
    with pytest.raises(ValidationError):
        validator.validate(config)


def test_neither_csv_nor_json_rejected(validator):
    config = _ic_base()
    del config["csv"]
    with pytest.raises(ValidationError):
        validator.validate(config)


def test_both_csv_and_json_rejected(validator):
    config = _ic_base()
    config["json"] = "/data/labels.json"
    with pytest.raises(ValidationError):
        validator.validate(config)


def test_image_category_without_images_rejected(validator):
    config = _ic_base()
    del config["images"]
    with pytest.raises(ValidationError):
        validator.validate(config)


def test_object_detection_without_annotations_rejected(validator):
    config = _load_example("object_detection.yaml")
    del config["annotations"]
    with pytest.raises(ValidationError):
        validator.validate(config)


def test_semantic_segmentation_without_masks_rejected(validator):
    config = _load_example("semantic_segmentation.yaml")
    del config["masks"]
    with pytest.raises(ValidationError):
        validator.validate(config)


def test_text_classification_without_texts_rejected(validator):
    config = _load_example("text_classification.yaml")
    del config["texts"]
    with pytest.raises(ValidationError):
        validator.validate(config)


def test_tabular_without_schema_rejected(validator):
    config = _load_example("tabular_classification.yaml")
    del config["schema"]
    with pytest.raises(ValidationError):
        validator.validate(config)


# Regression-class tasks must specify label.policy explicitly; the shorthand
# string form must be rejected for these categories.
@pytest.mark.parametrize(
    "example_name",
    [
        "tabular_regression.yaml",
        "time_series_forecasting.yaml",
        "time_to_event_prediction.yaml",
    ],
)
def test_regression_class_string_label_rejected(validator, example_name: str):
    config = _load_example(example_name)
    # Replace the object-form label with the shorthand string.
    config["label"] = config["label"]["column"]
    with pytest.raises(ValidationError):
        validator.validate(config)


@pytest.mark.parametrize(
    "example_name",
    [
        "tabular_regression.yaml",
        "time_series_forecasting.yaml",
        "time_to_event_prediction.yaml",
    ],
)
def test_regression_class_missing_policy_rejected(validator, example_name: str):
    config = _load_example(example_name)
    del config["label"]["policy"]
    with pytest.raises(ValidationError):
        validator.validate(config)


# Classification-class tasks must accept either form: the shorthand string
# (dominant case) or the explicit object form.
def test_classification_accepts_string_label(validator):
    validator.validate(_load_example("image_classification.yaml"))


def test_classification_accepts_object_label_passthrough(validator):
    config = _load_example("image_classification.yaml")
    config["label"] = {"column": "image_label", "policy": "passthrough"}
    validator.validate(config)


def test_data_id_column_strategy_requires_column(validator):
    config = _ic_base()
    config["data_id"] = {"strategy": "column"}  # missing `column`
    with pytest.raises(ValidationError):
        validator.validate(config)


def test_data_id_uuid_strategy_alone(validator):
    config = _ic_base()
    config["data_id"] = {"strategy": "uuid"}
    validator.validate(config)


def test_processor_requires_script_and_class(validator):
    config = _ic_base()
    config["spec"] = {"processors": [{"script": "/custom/x.py"}]}  # missing `class`
    with pytest.raises(ValidationError):
        validator.validate(config)


def test_apiversion_locked_to_v1(validator):
    config = _ic_base()
    config["apiVersion"] = "tracebloc.io/v2"
    with pytest.raises(ValidationError):
        validator.validate(config)


def test_kind_locked_to_ingestconfig(validator):
    config = _ic_base()
    config["kind"] = "Ingest"
    with pytest.raises(ValidationError):
        validator.validate(config)
