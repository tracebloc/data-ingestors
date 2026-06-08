"""Tests for map_validators — the per-category validator factory."""

from __future__ import annotations

import pytest

from tracebloc_ingestor.utils.validators_mapping import map_validators
from tracebloc_ingestor.utils.constants import TaskCategory, FileExtension
from tracebloc_ingestor.validators.file_validator import FileTypeValidator
from tracebloc_ingestor.validators.image_validator import ImageResolutionValidator
from tracebloc_ingestor.validators.data_validator import DataValidator
from tracebloc_ingestor.validators.table_name_validator import TableNameValidator
from tracebloc_ingestor.validators.duplicate_validator import DuplicateValidator
from tracebloc_ingestor.validators.xml_validator import PascalVOCXMLValidator
from tracebloc_ingestor.validators.time_to_event_validator import TimeToEventValidator
from tracebloc_ingestor.validators.time_format_validator import TimeFormatValidator
from tracebloc_ingestor.validators.numeric_columns_validator import NumericColumnsValidator
from tracebloc_ingestor.validators.keypoint_annotation_validator import KeypointAnnotationValidator
from tracebloc_ingestor.validators.keypoint_visibility_validator import KeypointVisibilityValidator
from tracebloc_ingestor.validators.tokenizer_validator import TokenizerValidator


IMAGE_OPTS = {"extension": FileExtension.JPG, "target_size": [224, 224]}


def _types(validators):
    return [type(v) for v in validators]


def test_image_classification():
    v = map_validators(TaskCategory.IMAGE_CLASSIFICATION, IMAGE_OPTS)
    assert _types(v) == [
        FileTypeValidator, ImageResolutionValidator, TableNameValidator, DuplicateValidator
    ]


def test_object_detection_includes_xml_validator():
    v = map_validators(TaskCategory.OBJECT_DETECTION, IMAGE_OPTS)
    types = _types(v)
    assert PascalVOCXMLValidator in types
    # two FileTypeValidators: images + annotations
    assert types.count(FileTypeValidator) == 2


def test_semantic_segmentation():
    v = map_validators(TaskCategory.SEMANTIC_SEGMENTATION, IMAGE_OPTS)
    types = _types(v)
    assert types.count(FileTypeValidator) == 2
    assert ImageResolutionValidator in types


def test_keypoint_detection_includes_keypoint_validators():
    v = map_validators(TaskCategory.KEYPOINT_DETECTION, IMAGE_OPTS)
    types = _types(v)
    assert KeypointAnnotationValidator in types
    assert KeypointVisibilityValidator in types


def test_tabular_classification_with_schema():
    v = map_validators(TaskCategory.TABULAR_CLASSIFICATION, {"schema": {"a": "INT"}})
    types = _types(v)
    assert DataValidator in types
    assert types[-2:] == [TableNameValidator, DuplicateValidator]


def test_tabular_classification_without_schema_omits_data_validator():
    v = map_validators(TaskCategory.TABULAR_CLASSIFICATION, {})
    assert DataValidator not in _types(v)


def test_tabular_regression_with_schema():
    v = map_validators(TaskCategory.TABULAR_REGRESSION, {"schema": {"x": "FLOAT"}})
    assert DataValidator in _types(v)


def test_text_classification_defaults_extension():
    v = map_validators(TaskCategory.TEXT_CLASSIFICATION, {})
    assert _types(v)[0] is FileTypeValidator


def test_token_classification_includes_bio_validator():
    from tracebloc_ingestor.validators.bio_label_validator import BIOLabelValidator

    v = map_validators(TaskCategory.TOKEN_CLASSIFICATION, {})
    types = _types(v)
    assert types[0] is FileTypeValidator
    assert BIOLabelValidator in types
    assert TableNameValidator in types and DuplicateValidator in types


def test_token_classification_with_schema_adds_data_validator():
    v = map_validators(TaskCategory.TOKEN_CLASSIFICATION, {"schema": {"a": "INT"}})
    assert DataValidator in _types(v)


def test_time_series_forecasting_validator_set():
    v = map_validators(
        TaskCategory.TIME_SERIES_FORECASTING,
        {"schema": {"timestamp": "TIMESTAMP", "value": "FLOAT"}},
    )
    types = _types(v)
    assert TimeFormatValidator in types
    assert NumericColumnsValidator in types
    # schema minus timestamp is non-empty -> a DataValidator is added
    assert DataValidator in types


def test_time_series_forecasting_timestamp_only_schema_no_data_validator():
    v = map_validators(
        TaskCategory.TIME_SERIES_FORECASTING,
        {"schema": {"timestamp": "TIMESTAMP"}},
    )
    assert DataValidator not in _types(v)


def test_time_to_event_with_schema():
    v = map_validators(
        TaskCategory.TIME_TO_EVENT_PREDICTION,
        {"schema": {"time": "INT"}, "time_column": "time"},
    )
    types = _types(v)
    assert TimeToEventValidator in types
    assert DataValidator in types


def test_time_to_event_without_schema():
    v = map_validators(TaskCategory.TIME_TO_EVENT_PREDICTION, {})
    types = _types(v)
    assert TimeToEventValidator in types
    assert DataValidator not in types


def test_masked_language_modeling_includes_tokenizer():
    v = map_validators(TaskCategory.MASKED_LANGUAGE_MODELING, {})
    assert TokenizerValidator in _types(v)


def test_unknown_category_returns_empty():
    assert map_validators("not_a_category", {}) == []
