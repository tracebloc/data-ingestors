from typing import Dict, Any, List
from tracebloc_ingestor.validators.file_validator import FileTypeValidator
from tracebloc_ingestor.validators.base import BaseValidator
from tracebloc_ingestor.validators.image_validator import ImageResolutionValidator
from tracebloc_ingestor.validators.data_validator import DataValidator
from tracebloc_ingestor.validators.table_name_validator import TableNameValidator
from tracebloc_ingestor.validators.duplicate_validator import DuplicateValidator
from tracebloc_ingestor.utils.constants import TaskCategory



def map_validators(task_category: TaskCategory, options: Dict[str, Any]) -> List[BaseValidator]:

    if task_category == TaskCategory.IMAGE_CLASSIFICATION:
        return [
          FileTypeValidator(allowed_extension=options["extension"], path="images"),
          ImageResolutionValidator(expected_resolution=options["target_size"]),
          TableNameValidator(),
          DuplicateValidator()
         ]
    elif task_category == TaskCategory.OBJECT_DETECTION:
        return [
          FileTypeValidator(allowed_extension=options["extension"], path="images"),
          FileTypeValidator(allowed_extension=".xml", path="annotations"),
          ImageResolutionValidator(expected_resolution=options["target_size"]),
          TableNameValidator(),
          DuplicateValidator()
         ]
    elif task_category == TaskCategory.TABULAR_CLASSIFICATION:
        validators = []

        # Add data validator if schema is provided
        if options.get("schema"):
            validators.append(DataValidator(schema=options["schema"]))
        validators.append(TableNameValidator())
        validators.append(DuplicateValidator())

        return validators
    else:
        return []

