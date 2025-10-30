"""File Type Validator Module.

This module provides validation for file types and extensions to ensure uniformity
across the dataset before ingestion.
"""

from pathlib import Path
from typing import Any, List
import logging

from .base import BaseValidator, ValidationResult
from ..utils.constants import FileExtension, RED, RESET
from ..config import Config
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class FileTypeValidator(BaseValidator):
    """Validator for ensuring file type and extension uniformity.

    This validator checks that all files in a dataset have the same file extension
    and are of the expected file type. It supports validation of both individual
    files and entire directories.

    Attributes:
        allowed_extension: Set of allowed file extensions
    """

    def __init__(
        self,
        allowed_extension: str = ".jpeg",
        name: str = "File Type Validator",
        path: str = "images",
    ):
        """Initialize the file type validator.

        Args:
            allowed_extension: Set of allowed file extensions (e.g., {'.jpg', '.png'})
            name: Human-readable name of the validator
        """
        super().__init__(name)
        self.allowed_extension = allowed_extension
        self.strict_mode = True  # Whether to enforce strict file type checking . we can later make this configurable
        self.path = path

        # Check if extension is allowed (if strict mode is enabled)
        if not FileExtension.is_valid_extension(self.allowed_extension):
            raise ValueError(
                f"{RED}Invalid allowed extension: {self.allowed_extension}{RESET}"
            )

        # Normalize extensions to lowercase with leading dot
        if self.allowed_extension:
            self.allowed_extension = {
                (
                    self.allowed_extension.lower()
                    if self.allowed_extension.startswith(".")
                    else f".{self.allowed_extension.lower()}"
                )
            }

    def validate(self, path: Any, **kwargs) -> ValidationResult:
        """Validate file types and extensions.

        Args:
            data: File path, directory path, or list of file paths to validate
            **kwargs: Additional validation parameters
                - recursive: Whether to search directories recursively (default: True)
                - ignore_hidden: Whether to ignore hidden files (default: True)

        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            data = f"{path}/{self.path}"
            recursive = kwargs.get("recursive", True)
            ignore_hidden = kwargs.get("ignore_hidden", True)

            # Get list of files to validate
            files_to_validate = self._get_files_to_validate(
                data, recursive, ignore_hidden
            )

            if not files_to_validate:
                return self._create_result(
                    is_valid=False,
                    errors=["No files found to validate"],
                    metadata={"files_checked": 0},
                )

            # Validate file extensions
            return self._validate_file_extensions(files_to_validate)

        except Exception as e:
            logger.error(f"Error during file type validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )

    def _get_files_to_validate(
        self, data: Any, recursive: bool, ignore_hidden: bool
    ) -> List[Path]:
        """Get list of files to validate from the input data.

        Args:
            data: Input data (file path, directory, or list of paths)
            recursive: Whether to search directories recursively
            ignore_hidden: Whether to ignore hidden files

        Returns:
            List of file paths to validate
        """
        files_to_validate = []

        if isinstance(data, (str, Path)):
            path = Path(data)
            if path.is_file():
                files_to_validate.append(path)
            elif path.is_dir():
                pattern = "**/*" if recursive else "*"
                for file_path in path.glob(pattern):
                    if file_path.is_file():
                        if ignore_hidden and file_path.name.startswith("."):
                            continue
                        files_to_validate.append(file_path)
            else:
                raise ValueError(f"Path does not exist: {path}")

        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (str, Path)):
                    path = Path(item)
                    if path.is_file():
                        files_to_validate.append(path)
                    else:
                        logger.warning(f"File not found: {path}")
                else:
                    logger.warning(f"Invalid file path type: {type(item)}")
        else:
            raise ValueError(f"Unsupported data type for validation: {type(data)}")

        return files_to_validate

    def _validate_file_extensions(self, files: List[Path]) -> ValidationResult:
        """Validate file extensions for uniformity.

        Args:
            files: List of file paths to validate

        Returns:
            ValidationResult containing validation status and messages
        """
        if not files:
            return self._create_result(
                is_valid=False,
                errors=["No files to validate"],
                metadata={"files_checked": 0},
            )

        # Get all unique extensions
        extensions = set()
        invalid_files = []
        warnings = []

        # Create progress bar
        progress_bar = self._create_progress_bar(len(files), "Checking file extensions")

        try:
            for file_path in files:
                extension = file_path.suffix.lower()
                extensions.add(extension)

                # Check if extension is allowed (if strict mode is enabled)
                if self.strict_mode and self.allowed_extension:
                    if extension not in self.allowed_extension:
                        invalid_files.append(str(file_path))

                # Update progress bar
                if progress_bar:
                    progress_bar.update(1)
        finally:
            # Close progress bar
            if progress_bar:
                progress_bar.close()

        # Check for uniformity
        if len(extensions) > 1:
            return self._create_result(
                is_valid=False,
                errors=[
                    f"Multiple file extensions found: {sorted(extensions)}. All files must have the same extension.",
                    f"Allowed extensions: {sorted(self.allowed_extension)}",
                    f"Invalid files: {invalid_files}",
                ],
                metadata={
                    "files_checked": len(files),
                    "extensions_found": sorted(extensions),
                    "invalid_files": invalid_files,
                },
            )

        # Check for invalid extensions in strict mode
        if invalid_files:
            return self._create_result(
                is_valid=False,
                errors=[f"Files with invalid extensions found: {invalid_files}"],
                metadata={
                    "files_checked": len(files),
                    "extensions_found": sorted(extensions),
                    "invalid_files": invalid_files,
                    "allowed_extension": sorted(self.allowed_extension),
                },
            )

        # Success case
        extension = list(extensions)[0] if extensions else "unknown"
        return self._create_result(
            is_valid=True,
            warnings=warnings,
            metadata={
                "files_checked": len(files),
                "uniform_extension": extension,
                "allowed_extension": (
                    sorted(self.allowed_extension) if self.allowed_extension else None
                ),
            },
        )
