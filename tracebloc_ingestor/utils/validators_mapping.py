from typing import Dict, Any, List
from tracebloc_ingestor.validators.file_validator import FileTypeValidator
from tracebloc_ingestor.validators.base import BaseValidator
from tracebloc_ingestor.validators.image_validator import ImageResolutionValidator
from tracebloc_ingestor.validators.csv_validator import CSVStructureValidator
from tracebloc_ingestor.validators.schema_validator import SchemaValidator
from tracebloc_ingestor.utils.constants import TaskCategory



def map_validators(task_category: TaskCategory, options: Dict[str, Any]) -> List[BaseValidator]:

    if task_category == TaskCategory.IMAGE_CLASSIFICATION:
        return [
          FileTypeValidator(allowed_extension=options["extension"], path="images"),
          ImageResolutionValidator(expected_resolution=options["target_size"])
         ]
    elif task_category == TaskCategory.OBJECT_DETECTION:
        return [
          FileTypeValidator(allowed_extension=options["extension"], path="images"),
          FileTypeValidator(allowed_extension=".xml", path="annotations"),
          ImageResolutionValidator(expected_resolution=options["target_size"])
         ]
    elif task_category == TaskCategory.TABULAR_CLASSIFICATION:
        validators = [
            CSVStructureValidator(
                expected_encoding=options.get("encoding", "utf-8"),
                expected_delimiter=options.get("delimiter", ",")
            )
        ]
        
        # Add schema validator if schema is provided
        if options.get("schema"):
            validators.append(SchemaValidator(schema=options["schema"]))
        
        return validators
    else:
        return []

