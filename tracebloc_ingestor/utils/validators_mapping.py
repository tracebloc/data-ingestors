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
        allowed_extension = options.get("allowed_extension", FileExtension.TXT)

        validators.append(
            FileTypeValidator(allowed_extension=allowed_extension, path="texts"),
        )

        # Add data validator if schema is provided
        if options.get("schema"):
            validators.append(DataValidator(schema=options["schema"]))

        validators.append(TableNameValidator())
        validators.append(DuplicateValidator())

        return validators
    elif task_category == TaskCategory.TIME_SERIES_FORECASTING:
        validators = []

        # Add time validators
        validators.append(TimeFormatValidator())
        validators.append(TimeOrderedValidator())
        validators.append(TimeBeforeTodayValidator())
        
        # Add data validator if schema is provided (excluding timestamp column)
        if options.get("schema"):
            schema_without_timestamp = {
                k: v for k, v in options["schema"].items() 
                if k.lower() != "timestamp"
            }
            if schema_without_timestamp:  # Only add if there are other columns to validate
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
    else:
        return []
