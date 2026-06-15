"""Per-category validator factories (structural refactor — backend#796, P3b).

One factory per task category, each ``(options) -> [validators]``. These are
the bodies of the old ``utils.validators_mapping.map_validators`` if/elif arms,
moved verbatim so the validator sets are byte-for-byte identical — now attached
to each ModalitySpec (``build_validators``) instead of dispatched by a ladder.
``map_validators`` is now a thin lookup over the registry.
"""

from __future__ import annotations

from typing import Any, Dict, List

from ..utils.constants import FileExtension
from ..validators.base import BaseValidator
from ..validators.bio_label_validator import BIOLabelValidator
from ..validators.data_validator import DataValidator
from ..validators.duplicate_validator import DuplicateValidator
from ..validators.file_pairing_validator import FilePairingValidator
from ..validators.file_validator import FileTypeValidator
from ..validators.image_validator import ImageResolutionValidator
from ..validators.keypoint_annotation_validator import KeypointAnnotationValidator
from ..validators.keypoint_visibility_validator import KeypointVisibilityValidator
from ..validators.label_diversity_validator import LabelDiversityValidator
from ..validators.numeric_columns_validator import NumericColumnsValidator
from ..validators.table_name_validator import TableNameValidator
from ..validators.time_before_today_validator import TimeBeforeTodayValidator
from ..validators.time_format_validator import TimeFormatValidator
from ..validators.time_ordered_validator import TimeOrderedValidator
from ..validators.time_to_event_validator import TimeToEventValidator
from ..validators.tokenizer_validator import TokenizerValidator
from ..validators.xml_validator import PascalVOCXMLValidator


def _label_diversity_validator(options: Dict[str, Any]) -> LabelDiversityValidator:
    """Construct a LabelDiversityValidator using the user-configured label
    column name (or the framework default ``label``). Centralised so every
    classification-family factory wires the same instance shape.

    Issue #251: a classification dataset with one distinct label value is
    unlearnable and the backend rejects it at ``/global_meta/prepare/`` with
    ``HTTP 400: "Please provide atleast 2 labels."``. Catching it at preflight
    surfaces the actual cause (and lists the offending label value(s)) instead
    of cascading to a misleading "Backend failed to prepare the dataset"
    message after the rows have already landed in MySQL.
    """
    return LabelDiversityValidator(
        label_column=options.get("label_column") or "label",
        # Read the label column with the SAME NA / dtype rules CSVIngestor
        # uses, or the distinct-label count disagrees with what's actually
        # ingested (bugbot #252). Prefer ``full_schema`` (base.py passes the
        # UNSTRIPPED schema here): ``schema``/``file_options["schema"]`` has the
        # label column removed, so it can't carry the label's type — the very
        # column this validator reads. Fall back to ``schema`` for direct
        # callers / tests that pass an unstripped map.
        schema=options.get("full_schema") or options.get("schema"),
    )


def image_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    return [
        FileTypeValidator(allowed_extension=options["extension"], path="images"),
        ImageResolutionValidator(expected_resolution=options["target_size"]),
        _label_diversity_validator(options),
        TableNameValidator(),
        DuplicateValidator(),
    ]


def object_detection(options: Dict[str, Any]) -> List[BaseValidator]:
    return [
        FileTypeValidator(allowed_extension=options["extension"], path="images"),
        FileTypeValidator(allowed_extension=".xml", path="annotations"),
        PascalVOCXMLValidator(),
        FilePairingValidator(
            image_path="images",
            sidecar_path="annotations",
            sidecar_label="annotation",
        ),
        ImageResolutionValidator(expected_resolution=options["target_size"]),
        _label_diversity_validator(options),
        TableNameValidator(),
        DuplicateValidator(),
    ]


def tabular_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Add data validator if schema is provided
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    validators.append(_label_diversity_validator(options))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators


def text_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Add text file validator
    validators.append(
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="texts",
        ),
    )
    # Optional user-supplied tokenizer.json — warn (don't fail) if absent;
    # if present, it must contain [PAD] (text classification pads batches).
    validators.append(TokenizerValidator(required_tokens=("[PAD]",), optional=True))
    # Add data validator if schema is provided
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    validators.append(_label_diversity_validator(options))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators


def token_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Validate text file extensions (one .txt of whitespace-tokenized words
    # per sample, same layout as text classification).
    validators.append(
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="texts",
        ),
    )
    # Validate BIO labels: one tag per word, valid BIO/IOB2 format. Honor a
    # custom label column name when one is configured in the YAML.
    validators.append(
        BIOLabelValidator(
            texts_path="texts",
            extension=options.get("extension", FileExtension.TXT),
            label_column=options.get("label_column") or "label",
        )
    )
    # Optional user-supplied tokenizer.json — warn (don't fail) if absent;
    # if present, it must contain [PAD].
    validators.append(TokenizerValidator(required_tokens=("[PAD]",), optional=True))
    # Add data validator if schema is provided
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators


def time_series_forecasting(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    schema = options.get("schema", {})
    validators.append(TimeFormatValidator(schema=schema))
    validators.append(TimeOrderedValidator())
    validators.append(TimeBeforeTodayValidator())
    validators.append(NumericColumnsValidator(schema=schema))
    if options.get("schema"):
        schema_without_timestamp = {
            k: v for k, v in options["schema"].items() if k.lower() != "timestamp"
        }
        if schema_without_timestamp:
            validators.append(DataValidator(schema=schema_without_timestamp))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators


def tabular_regression(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Add data validator if schema is provided
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators


def time_to_event_prediction(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Add time to event validator with schema to identify time column
    if options.get("schema"):
        validators.append(
            TimeToEventValidator(
                schema=options["schema"],
                time_column=options.get("time_column"),
            )
        )
    else:
        # If no schema, use default time column name
        validators.append(
            TimeToEventValidator(time_column=options.get("time_column", "time"))
        )
    # Add data validator if schema is provided
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators


def semantic_segmentation(options: Dict[str, Any]) -> List[BaseValidator]:
    return [
        FileTypeValidator(allowed_extension=options["extension"], path="images"),
        FileTypeValidator(allowed_extension=FileExtension.PNG, path="masks"),
        FilePairingValidator(
            image_path="images",
            sidecar_path="masks",
            sidecar_label="mask",
            # Documented + shipped convention for semantic_segmentation masks
            # is `<filename>_mask.png` (#196). Strip the suffix before matching
            # so image_001.jpg pairs with image_001_mask.png. object_detection's
            # pairing is plain stem (no suffix) — the default.
            sidecar_suffix="_mask",
        ),
        ImageResolutionValidator(expected_resolution=options["target_size"]),
        _label_diversity_validator(options),
        TableNameValidator(),
        DuplicateValidator(),
    ]


def keypoint_detection(options: Dict[str, Any]) -> List[BaseValidator]:
    # ``number_of_keypoints`` is required by the ingest schema for
    # keypoint_detection (see ``schema/ingest.v1.json``) and plumbed into
    # ``file_options`` by ``cli/conventions.py``. Passing it to
    # ``KeypointAnnotationValidator`` enables the per-row count check that
    # rejects datasets whose annotations drift from the declared K.
    return [
        FileTypeValidator(allowed_extension=options["extension"], path="images"),
        ImageResolutionValidator(expected_resolution=options["target_size"]),
        KeypointAnnotationValidator(num_keypoints=options.get("number_of_keypoints")),
        KeypointVisibilityValidator(),
        _label_diversity_validator(options),
        TableNameValidator(),
        DuplicateValidator(),
    ]


def masked_language_modeling(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Validate text file extensions
    validators.append(
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="sequences",
        ),
    )
    # Validate tokenizer.json has required special tokens ([MASK], [PAD])
    validators.append(TokenizerValidator())
    # Add data validator if schema is provided
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators
