from typing import Dict, Any, List
from tracebloc_ingestor.validators.file_validator import FileTypeValidator
from tracebloc_ingestor.validators.base import BaseValidator
from tracebloc_ingestor.validators.image_validator import ImageResolutionValidator
from tracebloc_ingestor.validators.data_validator import DataValidator
from tracebloc_ingestor.validators.table_name_validator import TableNameValidator
from tracebloc_ingestor.validators.duplicate_validator import DuplicateValidator
from tracebloc_ingestor.validators.xml_validator import PascalVOCXMLValidator
from tracebloc_ingestor.validators.text_validator import TextFileValidator
from tracebloc_ingestor.utils.constants import TaskCategory, FileExtension



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
          PascalVOCXMLValidator(),
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
    elif task_category == TaskCategory.TEXT_CLASSIFICATION:
        validators = []
        
        # Add text file validator
        allowed_extensions = options.get("allowed_extensions", [FileExtension.TXT, FileExtension.TEXT])
        max_file_size = options.get("max_file_size", 10 * 1024 * 1024)  # 10MB default
        encoding = options.get("encoding", "utf-8")
        
        validators.append(TextFileValidator(
            allowed_extensions=allowed_extensions,
            max_file_size=max_file_size,
            encoding=encoding,
            path="text_files"
        ))
        
        # Add data validator if schema is provided
        if options.get("schema"):
            validators.append(DataValidator(schema=options["schema"]))
        
        validators.append(TableNameValidator())
        validators.append(DuplicateValidator())
        
        return validators
    else:
        return []

