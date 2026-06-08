"""Tests for the convention-defaults resolver in ``cli/conventions.py``.

These tests pin the contract between the YAML schema (#44 commit 1) and the
ingestor constructor surface. They run as pure-function tests — no DB, no
network, no env reads — so they catch resolver regressions in milliseconds.

What they cover:

- Every example in ``examples/yaml/`` round-trips through ``resolve()``.
- Each category's ``data_format`` matches the framework's existing
  ``DataFormat`` enum.
- Convention defaults (csv_options, image target_size, default extensions)
  are filled in when the customer doesn't override.
- Customer overrides win over defaults.
- ``label`` shorthand and object forms produce equivalent ``ResolvedConfig``
  for classification, and the explicit object form's policy carries through
  for regression-class.
- ``data_id.strategy`` correctly drives ``unique_id_column``.
- The ``processor_specs`` pass through verbatim.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

import pytest
import yaml

from tracebloc_ingestor.cli.conventions import (
    DEFAULT_CSV_OPTIONS,
    DEFAULT_IMAGE_FILE_OPTIONS_BY_CATEGORY,
    DEFAULT_TEXT_FILE_OPTIONS,
    IMAGE_CATEGORIES,
    REGRESSION_CLASS_CATEGORIES,
    ResolvedConfig,
    resolve,
)
from tracebloc_ingestor.utils.constants import DataFormat, TaskCategory


REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_ROOT / "examples" / "yaml"


def _load(name: str) -> Dict[str, Any]:
    return yaml.safe_load((EXAMPLES_DIR / name).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Round-trip: every shipped example resolves without error.
# ---------------------------------------------------------------------------

EXAMPLES = sorted(p.name for p in EXAMPLES_DIR.glob("*.yaml"))


@pytest.mark.parametrize("name", EXAMPLES)
def test_example_resolves(name: str):
    resolved = resolve(_load(name))
    assert isinstance(resolved, ResolvedConfig)
    assert resolved.category
    assert resolved.table_name
    assert resolved.intent in {"train", "test"}
    assert resolved.data_format in {
        DataFormat.IMAGE,
        DataFormat.TEXT,
        DataFormat.TABULAR,
    }


# ---------------------------------------------------------------------------
# Identity & source dispatch
# ---------------------------------------------------------------------------

def test_image_classification_data_format():
    r = resolve(_load("image_classification.yaml"))
    assert r.data_format == DataFormat.IMAGE
    assert r.source_type == "csv"
    assert r.source_path == "/data/shared/chest-xrays/labels.csv"
    assert r.images == "/data/shared/chest-xrays/images/"


def test_text_classification_data_format():
    r = resolve(_load("text_classification.yaml"))
    assert r.data_format == DataFormat.TEXT
    assert r.texts == "/data/shared/support-tickets/texts/"


def test_token_classification_data_format():
    r = resolve(_load("token_classification.yaml"))
    assert r.data_format == DataFormat.TEXT
    assert r.file_options == DEFAULT_TEXT_FILE_OPTIONS
    assert r.label_column == "label"


def test_tabular_classification_data_format():
    r = resolve(_load("tabular_classification.yaml"))
    assert r.data_format == DataFormat.TABULAR


def test_time_series_data_format_is_tabular():
    """Time-series flows through the tabular ingestor path; data_format reflects that."""
    r = resolve(_load("time_series_forecasting.yaml"))
    assert r.data_format == DataFormat.TABULAR


def test_object_detection_sidecars_set():
    r = resolve(_load("object_detection.yaml"))
    assert r.images == "/data/shared/visdrone/images/"
    assert r.annotations == "/data/shared/visdrone/annotations/"
    assert r.masks is None
    assert r.texts is None


def test_semantic_segmentation_sidecars_set():
    r = resolve(_load("semantic_segmentation.yaml"))
    assert r.images == "/data/shared/tumors/images/"
    assert r.masks == "/data/shared/tumors/masks/"
    assert r.annotations is None


# ---------------------------------------------------------------------------
# Default options
# ---------------------------------------------------------------------------

def test_image_classification_gets_512x512_default():
    r = resolve(_load("image_classification.yaml"))
    assert r.file_options == DEFAULT_IMAGE_FILE_OPTIONS_BY_CATEGORY[
        TaskCategory.IMAGE_CLASSIFICATION
    ]


def test_object_detection_gets_448x448_default():
    """Object detection's template historically uses 448×448, not 512×512."""
    r = resolve(_load("object_detection.yaml"))
    assert r.file_options == DEFAULT_IMAGE_FILE_OPTIONS_BY_CATEGORY[
        TaskCategory.OBJECT_DETECTION
    ]
    assert r.file_options["target_size"] == [448, 448]


def test_keypoint_detection_bridges_top_level_fields():
    """Keypoint detection has no convention defaults for target_size or
    number_of_keypoints — both are dataset-specific. The example YAML
    supplies them top-level; resolve() must bridge them into file_options
    so validators see them at the same key the template path uses."""
    r = resolve(_load("keypoint_detection.yaml"))
    assert r.file_options["extension"] == ".jpg"
    assert r.file_options["target_size"] == [256, 256]
    assert r.file_options["number_of_keypoints"] == 9


def test_text_classification_gets_text_file_option_defaults():
    r = resolve(_load("text_classification.yaml"))
    assert r.file_options == DEFAULT_TEXT_FILE_OPTIONS


def test_tabular_has_no_file_options():
    r = resolve(_load("tabular_classification.yaml"))
    assert r.file_options == {}


def test_csv_options_default_when_unspecified():
    r = resolve(_load("image_classification.yaml"))
    assert r.csv_options == DEFAULT_CSV_OPTIONS


def test_customer_csv_options_override_defaults():
    config = _load("image_classification.yaml")
    config["spec"] = {"csv_options": {"chunk_size": 50, "delimiter": "\t"}}
    r = resolve(config)
    # chunk_size and delimiter overridden, quotechar/escapechar still defaulted.
    assert r.csv_options["chunk_size"] == 50
    assert r.csv_options["delimiter"] == "\t"
    assert r.csv_options["quotechar"] == DEFAULT_CSV_OPTIONS["quotechar"]
    assert r.csv_options["escapechar"] == DEFAULT_CSV_OPTIONS["escapechar"]


def test_customer_file_options_override_defaults():
    config = _load("image_classification.yaml")
    config["spec"] = {"file_options": {"target_size": [224, 224]}}
    r = resolve(config)
    assert r.file_options["target_size"] == [224, 224]
    # extension still defaulted.
    assert r.file_options["extension"] == DEFAULT_IMAGE_FILE_OPTIONS_BY_CATEGORY[
        TaskCategory.IMAGE_CLASSIFICATION
    ]["extension"]


# ---------------------------------------------------------------------------
# Label resolution
# ---------------------------------------------------------------------------

def test_string_label_resolves_to_passthrough_policy():
    r = resolve(_load("image_classification.yaml"))
    assert r.label_column == "image_label"
    assert r.label_policy == "passthrough"


def test_object_label_with_passthrough_policy():
    config = _load("image_classification.yaml")
    config["label"] = {"column": "image_label", "policy": "passthrough"}
    r = resolve(config)
    assert r.label_column == "image_label"
    assert r.label_policy == "passthrough"


def test_regression_label_carries_bucket_policy():
    r = resolve(_load("tabular_regression.yaml"))
    assert r.label_column == "price"
    assert r.label_policy == "bucket"


def test_object_label_without_policy_defaults_to_passthrough():
    """Non-regression categories may omit policy on the object form."""
    config = _load("image_classification.yaml")
    config["label"] = {"column": "image_label"}  # no policy key
    r = resolve(config)
    assert r.label_policy == "passthrough"


# ---------------------------------------------------------------------------
# data_id resolution
# ---------------------------------------------------------------------------

def test_no_data_id_block_means_uuid_generation():
    r = resolve(_load("image_classification.yaml"))
    assert r.unique_id_column is None


def test_uuid_strategy_explicit():
    config = _load("image_classification.yaml")
    config["data_id"] = {"strategy": "uuid"}
    r = resolve(config)
    assert r.unique_id_column is None


def test_column_strategy_sets_unique_id_column():
    config = _load("image_classification.yaml")
    config["data_id"] = {"strategy": "column", "column": "image_id"}
    r = resolve(config)
    assert r.unique_id_column == "image_id"


# ---------------------------------------------------------------------------
# Schema, time_column, processors
# ---------------------------------------------------------------------------

def test_tabular_schema_passes_through():
    r = resolve(_load("tabular_classification.yaml"))
    assert "age" in r.schema
    assert r.schema["age"] == "INT"


def test_time_to_event_carries_time_column():
    r = resolve(_load("time_to_event_prediction.yaml"))
    assert r.time_column == "tenure_days"
    # Step 7a bridges the top-level field into file_options where the
    # validator expects it.
    assert r.file_options["time_column"] == "tenure_days"


def test_time_to_event_spec_file_options_time_column_wins_over_top_level():
    """spec.file_options is the advanced override surface; an explicit
    spec value must beat the top-level shorthand. Regression guard against
    overwriting the spec value with the top-level field."""
    config = _load("time_to_event_prediction.yaml")
    config.setdefault("spec", {}).setdefault("file_options", {})["time_column"] = "override_time"
    r = resolve(config)
    assert r.file_options["time_column"] == "override_time"


def test_processor_specs_pass_through_verbatim():
    r = resolve(_load("custom_processor.yaml"))
    assert len(r.processor_specs) == 1
    spec = r.processor_specs[0]
    assert spec["script"] == "/custom/decrypt.py"
    assert spec["class"] == "AESDecryptor"
    assert spec["args"]["column"] == "encrypted_diagnosis"


def test_no_processors_means_empty_list():
    r = resolve(_load("image_classification.yaml"))
    assert r.processor_specs == []


# ---------------------------------------------------------------------------
# Per-category convention details
# ---------------------------------------------------------------------------

def test_keypoint_detection_uses_annotation_column_convention():
    """The existing keypoint_detection template uses column "Annotation";
    the resolver bakes that convention in so customers don't have to."""
    r = resolve(_load("keypoint_detection.yaml"))
    assert r.annotation_column == "Annotation"


def test_other_categories_have_no_annotation_column():
    r = resolve(_load("image_classification.yaml"))
    assert r.annotation_column is None


def test_regression_class_set_matches_schema_requirements():
    """Sanity-check that REGRESSION_CLASS_CATEGORIES matches the schema's
    `if/then` block requiring object-form label.policy."""
    expected = {
        TaskCategory.TABULAR_REGRESSION,
        TaskCategory.TIME_SERIES_FORECASTING,
        TaskCategory.TIME_TO_EVENT_PREDICTION,
    }
    assert REGRESSION_CLASS_CATEGORIES == expected


def test_image_categories_set_matches_schema_requirements():
    """The set of image-based categories must match the schema's
    `if/then` block requiring `images:`."""
    expected = {
        TaskCategory.IMAGE_CLASSIFICATION,
        TaskCategory.OBJECT_DETECTION,
        TaskCategory.KEYPOINT_DETECTION,
        TaskCategory.SEMANTIC_SEGMENTATION,
        TaskCategory.INSTANCE_SEGMENTATION,
    }
    assert IMAGE_CATEGORIES == expected


# ---------------------------------------------------------------------------
# JSON source path
# ---------------------------------------------------------------------------

def test_json_source_dispatches_correctly():
    """No JSON example ships in examples/yaml/ today (CSV is dominant);
    construct one inline to verify the dispatch path."""
    config = {
        "apiVersion": "tracebloc.io/v1",
        "kind": "IngestConfig",
        "category": TaskCategory.TABULAR_CLASSIFICATION,
        "table": "events_train",
        "intent": "train",
        "json": "/data/events.json",
        "schema": {"event_type": "VARCHAR(64)", "outcome": "VARCHAR(8)"},
        "label": "outcome",
    }
    r = resolve(config)
    assert r.source_type == "json"
    assert r.source_path == "/data/events.json"
