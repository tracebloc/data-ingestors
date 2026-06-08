from typing import Dict, Any, List
from tracebloc_ingestor.validators.file_validator import FileTypeValidator
from tracebloc_ingestor.validators.base import BaseValidator
from tracebloc_ingestor.validators.image_validator import ImageResolutionValidator
from tracebloc_ingestor.validators.data_validator import DataValidator
from tracebloc_ingestor.validators.table_name_validator import TableNameValidator
from tracebloc_ingestor.validators.duplicate_validator import DuplicateValidator
from tracebloc_ingestor.validators.xml_validator import PascalVOCXMLValidator
from tracebloc_ingestor.validators.time_to_event_validator import TimeToEventValidator
from tracebloc_ingestor.validators.time_format_validator import TimeFormatValidator
from tracebloc_ingestor.validators.time_ordered_validator import TimeOrderedValidator
from tracebloc_ingestor.validators.time_before_today_validator import TimeBeforeTodayValidator
from tracebloc_ingestor.validators.numeric_columns_validator import NumericColumnsValidator
from tracebloc_ingestor.validators.keypoint_annotation_validator import KeypointAnnotationValidator
from tracebloc_ingestor.validators.keypoint_visibility_validator import KeypointVisibilityValidator
from tracebloc_ingestor.validators.tokenizer_validator import TokenizerValidator
from tracebloc_ingestor.validators.file_pairing_validator import FilePairingValidator
from tracebloc_ingestor.validators.bio_label_validator import BIOLabelValidator
from tracebloc_ingestor.utils.constants import TaskCategory, FileExtension


def map_validators(
    task_category: TaskCategory, options: Dict[str, Any]
) -> List[BaseValidator]:
    if task_category == TaskCategory.IMAGE_CLASSIFICATION:
        return [
            FileTypeValidator(allowed_extension=options["extension"], path="images"),
            ImageResolutionValidator(expected_resolution=options["target_size"]),
            TableNameValidator(),
            DuplicateValidator(),
        ]
    elif task_category == TaskCategory.OBJECT_DETECTION:
        return [
            FileTypeValidator(allowed_extension=options["extension"], path="images"),
            FileTypeValidator(allowed_extension=".xml", path="annotations"),
            PascalVOCXMLValidator(),
            FilePairingValidator(
                image_path="images", sidecar_path="annotations", sidecar_label="annotation"
            ),
            ImageResolutionValidator(expected_resolution=options["target_size"]),
            TableNameValidator(),
            DuplicateValidator(),
        ]
    elif task_category == TaskCategory.TABULAR_CLASSIFICATION:
        validators = []

        # Add data validator if schema is provided
        if options.get("schema"):
            validators.append(DataValidator(schema=options["schema"]))
        validators.append(TableNameValidator())
        validators.append(DuplicateValidator())

        return validators
    elif task_category == TaskCategory.TEXT_CLASSIFICATION:
        validators = []

        # Add text file validator
        validators.append(
            FileTypeValidator(
                allowed_extension=options.get("extension", FileExtension.TXT),
                path="texts",
            ),
        )

        # Add data validator if schema is provided
        if options.get("schema"):
            validators.append(DataValidator(schema=options["schema"]))

        validators.append(TableNameValidator())
        validators.append(DuplicateValidator())

        return validators
    elif task_category == TaskCategory.TOKEN_CLASSIFICATION:
        validators = []

        # Validate text file extensions (one .txt of whitespace-tokenized words
        # per sample, same layout as text classification).
        validators.append(
            FileTypeValidator(
                allowed_extension=options.get("extension", FileExtension.TXT),
                path="texts",
            ),
        )

        # Validate BIO labels: one tag per word, valid BIO/IOB2 format.
        # Honor a custom label column name when one is configured in the YAML.
        validators.append(
            BIOLabelValidator(
                texts_path="texts",
                extension=options.get("extension", FileExtension.TXT),
                label_column=options.get("label_column") or "label",
            )
        )

        # Add data validator if schema is provided
        if options.get("schema"):
            validators.append(DataValidator(schema=options["schema"]))

        validators.append(TableNameValidator())
        validators.append(DuplicateValidator())

        return validators
    elif task_category == TaskCategory.TIME_SERIES_FORECASTING:
        validators = []

        schema = options.get("schema", {})
        
        validators.append(TimeFormatValidator(schema=schema))
        validators.append(TimeOrderedValidator())
        validators.append(TimeBeforeTodayValidator())
        validators.append(NumericColumnsValidator(schema=schema))
        
        if options.get("schema"):
            schema_without_timestamp = {
                k: v for k, v in options["schema"].items() 
                if k.lower() != "timestamp"
            }
            if schema_without_timestamp:
                validators.append(DataValidator(schema=schema_without_timestamp))
        
        validators.append(TableNameValidator())
        validators.append(DuplicateValidator())

        return validators
    elif task_category == TaskCategory.TABULAR_REGRESSION:
        validators = []

        # Add data validator if schema is provided
        if options.get("schema"):
            validators.append(DataValidator(schema=options["schema"]))
        validators.append(TableNameValidator())
        validators.append(DuplicateValidator())

        return validators
    elif task_category == TaskCategory.TIME_TO_EVENT_PREDICTION:
        validators = []

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
    elif task_category == TaskCategory.SEMANTIC_SEGMENTATION:
        return [
            FileTypeValidator(allowed_extension=options["extension"], path="images"),
            FileTypeValidator(allowed_extension=FileExtension.PNG, path="masks"),
            FilePairingValidator(
                image_path="images", sidecar_path="masks", sidecar_label="mask"
            ),
            ImageResolutionValidator(expected_resolution=options["target_size"]),
            TableNameValidator(),
            DuplicateValidator(),
        ]
    elif task_category == TaskCategory.KEYPOINT_DETECTION:
        # ``number_of_keypoints`` is required by the ingest schema for
        # keypoint_detection (see ``schema/ingest.v1.json``) and
        # plumbed into ``file_options`` by ``cli/conventions.py``.
        # Passing it to ``KeypointAnnotationValidator`` enables the
        # per-row count check that rejects datasets whose annotations
        # drift from the declared K.
        validators = [
            FileTypeValidator(allowed_extension=options["extension"], path="images"),
            ImageResolutionValidator(expected_resolution=options["target_size"]),
            KeypointAnnotationValidator(
                num_keypoints=options.get("number_of_keypoints")
            ),
            KeypointVisibilityValidator(),
            TableNameValidator(),
            DuplicateValidator(),
        ]
        return validators
    elif task_category == TaskCategory.MASKED_LANGUAGE_MODELING:
        validators = []

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
    else:
        return []
