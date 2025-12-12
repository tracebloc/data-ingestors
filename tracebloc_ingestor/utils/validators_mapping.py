from typing import Dict, Any, List
from tracebloc_ingestor.validators.file_validator import FileTypeValidator
from tracebloc_ingestor.validators.base import BaseValidator
from tracebloc_ingestor.validators.image_validator import ImageResolutionValidator
from tracebloc_ingestor.validators.data_validator import DataValidator
from tracebloc_ingestor.validators.table_name_validator import TableNameValidator
from tracebloc_ingestor.validators.duplicate_validator import DuplicateValidator
from tracebloc_ingestor.validators.xml_validator import PascalVOCXMLValidator
from tracebloc_ingestor.validators.time_series_validator import TimeSeriesValidator
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

        # Add time series validator with schema to identify date column
        if options.get("schema"):
            validators.append(
                TimeSeriesValidator(
                    schema=options["schema"],
                    date_column=options.get("date_column"),
                )
            )
        else:
            # If no schema, use default date column name
            validators.append(
                TimeSeriesValidator(date_column=options.get("date_column", "date"))
            )

        # Add data validator if schema is provided
        if options.get("schema"):
            validators.append(DataValidator(schema=options["schema"]))
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
    else:
        return []
