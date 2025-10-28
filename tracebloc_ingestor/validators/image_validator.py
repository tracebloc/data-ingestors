"""Image Resolution Validator Module.

This module provides validation for image resolution uniformity to ensure all images
in a dataset have the same dimensions before ingestion.
"""

from pathlib import Path
from typing import Any, List, Optional, Tuple
import logging

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.utils.logging import setup_logging

try:
    from PIL import Image

    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    Image = None

from .base import BaseValidator, ValidationResult


# Configure unified logging with config
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class ImageResolutionValidator(BaseValidator):
    """Validator for ensuring image resolution uniformity.

    This validator checks that all images in a dataset have the same resolution
    (width and height). It supports validation of both individual images and
    entire directories containing images.

    Attributes:
        expected_resolution: Expected image resolution as (width, height)
        supported_formats: Set of supported image formats
    """

    def __init__(
        self,
        expected_resolution: Optional[Tuple[int, int]] = None,
        name: str = "Image Resolution Validator",
    ):
        """Initialize the image resolution validator.

        Args:
            expected_resolution: Expected image resolution as (width, height)
            supported_formats: Set of supported image formats (e.g., {'.jpg', '.png'})
            name: Human-readable name of the validator
        """
        super().__init__(name)
        self.expected_resolution = expected_resolution
        self.tolerance = 0  # Whether to enforce strict file type checking . we can later make this configurable
        self.supported_formats = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif"}

        # Normalize formats to lowercase with leading dot
        self.supported_formats = {
            fmt.lower() if fmt.startswith(".") else f".{fmt.lower()}"
            for fmt in self.supported_formats
        }

        if not PIL_AVAILABLE:
            logger.warning(
                "PIL/Pillow not available. Image resolution validation will be limited."
            )

    def validate(self, path: Any, **kwargs) -> ValidationResult:
        """Validate image resolution uniformity.

        Args:
            data: Image file path, directory path, or list of image file paths to validate
            **kwargs: Additional validation parameters
                - recursive: Whether to search directories recursively (default: True)
                - ignore_hidden: Whether to ignore hidden files (default: True)
                - auto_detect_resolution: Whether to auto-detect expected resolution from first image (default: True)

        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            data = f"{path}/images"
            if not PIL_AVAILABLE:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        "PIL/Pillow not available. Cannot validate image resolutions."
                    ],
                    metadata={"pil_available": False},
                )

            recursive = kwargs.get("recursive", True)
            ignore_hidden = kwargs.get("ignore_hidden", True)
            auto_detect_resolution = kwargs.get("auto_detect_resolution", True)

            # Get list of image files to validate
            image_files = self._get_image_files(data, recursive, ignore_hidden)

            if not image_files:
                return self._create_result(
                    is_valid=False,
                    errors=["No image files found to validate"],
                    metadata={"files_checked": 0},
                )

            # Auto-detect resolution from first image if not specified
            if auto_detect_resolution and not self.expected_resolution:
                first_image_resolution = self._get_image_resolution(image_files[0])
                if first_image_resolution:
                    self.expected_resolution = first_image_resolution
                    logger.info(
                        f"Auto-detected expected resolution: {self.expected_resolution}"
                    )

            # Validate image resolutions
            return self._validate_image_resolutions(image_files)

        except Exception as e:
            logger.error(f"Error during image resolution validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )

    def _get_image_files(
        self, data: Any, recursive: bool, ignore_hidden: bool
    ) -> List[Path]:
        """Get list of image files to validate from the input data.

        Args:
            data: Input data (file path, directory, or list of paths)
            recursive: Whether to search directories recursively
            ignore_hidden: Whether to ignore hidden files

        Returns:
            List of image file paths to validate
        """
        image_files = []

        if isinstance(data, (str, Path)):
            path = Path(data)
            if path.is_file():
                if self._is_image_file(path):
                    image_files.append(path)
            elif path.is_dir():
                pattern = "**/*" if recursive else "*"
                for file_path in path.glob(pattern):
                    if file_path.is_file() and self._is_image_file(file_path):
                        if ignore_hidden and file_path.name.startswith("."):
                            continue
                        image_files.append(file_path)
            else:
                raise ValueError(f"Path does not exist: {path}")

        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (str, Path)):
                    path = Path(item)
                    if path.is_file() and self._is_image_file(path):
                        image_files.append(path)
                    elif path.is_file():
                        logger.warning(f"File is not a supported image format: {path}")
                else:
                    logger.warning(f"Invalid file path type: {type(item)}")
        else:
            raise ValueError(f"Unsupported data type for validation: {type(data)}")

        return image_files

    def _is_image_file(self, file_path: Path) -> bool:
        """Check if a file is a supported image format.

        Args:
            file_path: Path to the file to check

        Returns:
            True if the file is a supported image format, False otherwise
        """
        return file_path.suffix.lower() in self.supported_formats

    def _get_image_resolution(self, image_path: Path) -> Optional[Tuple[int, int]]:
        """Get the resolution of an image file.

        Args:
            image_path: Path to the image file

        Returns:
            Tuple of (width, height) if successful, None otherwise
        """
        try:
            with Image.open(image_path) as img:
                return img.size  # Returns (width, height)
        except Exception as e:
            logger.warning(f"Could not get resolution for {image_path}: {str(e)}")
            return None

    def _validate_image_resolutions(self, image_files: List[Path]) -> ValidationResult:
        """Validate image resolutions for uniformity.

        Args:
            image_files: List of image file paths to validate

        Returns:
            ValidationResult containing validation status and messages
        """
        if not image_files:
            return self._create_result(
                is_valid=False,
                errors=["No image files to validate"],
                metadata={"files_checked": 0},
            )

        if not self.expected_resolution:
            return self._create_result(
                is_valid=False,
                errors=["No expected resolution specified and auto-detection failed"],
                metadata={"files_checked": len(image_files)},
            )

        invalid_files = []
        resolution_errors = []
        warnings = []
        resolutions_found = set()

        # Create progress bar
        progress_bar = self._create_progress_bar(
            len(image_files), "Validating image resolutions"
        )

        try:
            for image_path in image_files:
                try:
                    resolution = self._get_image_resolution(image_path)
                    if resolution is None:
                        invalid_files.append(str(image_path))
                        continue

                    resolutions_found.add(resolution)
                    # Check if resolution matches expected (with tolerance)
                    if not self._resolution_matches(
                        resolution, self.expected_resolution
                    ):
                        resolution_errors.append(
                            f"{image_path}: {resolution} (expected: {self.expected_resolution})"
                        )

                except Exception as e:
                    invalid_files.append(f"{image_path}: {str(e)}")

                # Update progress bar
                if progress_bar:
                    progress_bar.update(1)
        finally:
            # Close progress bar
            if progress_bar:
                progress_bar.close()

        # Check for uniformity
        if len(resolutions_found) > 1:
            return self._create_result(
                is_valid=False,
                errors=[
                    f"Multiple image resolutions found: {sorted(resolutions_found)}. All images must have the same resolution.",
                    f"Expected resolution: {self.expected_resolution}",
                    f"Invalid files: {invalid_files}",
                    f"Resolution errors: {resolution_errors}",
                ],
                metadata={
                    "files_checked": len(image_files),
                    "resolutions_found": sorted(resolutions_found),
                    "expected_resolution": self.expected_resolution,
                    "invalid_files": invalid_files,
                    "resolution_errors": resolution_errors,
                },
            )

        # Check for resolution mismatches
        if resolution_errors:
            return self._create_result(
                is_valid=False,
                errors=[f"Images with incorrect resolution found: {resolution_errors}"],
                metadata={
                    "files_checked": len(image_files),
                    "resolutions_found": sorted(resolutions_found),
                    "expected_resolution": self.expected_resolution,
                    "invalid_files": invalid_files,
                    "resolution_errors": resolution_errors,
                },
            )

        # Check for files that couldn't be processed
        if invalid_files:
            return self._create_result(
                is_valid=False,
                errors=[f"Files that could not be processed: {invalid_files}"],
                metadata={
                    "files_checked": len(image_files),
                    "resolutions_found": sorted(resolutions_found),
                    "expected_resolution": self.expected_resolution,
                    "invalid_files": invalid_files,
                },
            )

        # Success case
        uniform_resolution = (
            list(resolutions_found)[0]
            if resolutions_found
            else self.expected_resolution
        )
        return self._create_result(
            is_valid=True,
            warnings=warnings,
            metadata={
                "files_checked": len(image_files),
                "uniform_resolution": uniform_resolution,
                "expected_resolution": self.expected_resolution,
                "tolerance": self.tolerance,
            },
        )

    def _resolution_matches(
        self, actual: Tuple[int, int], expected: Tuple[int, int]
    ) -> bool:
        """Check if actual resolution matches expected resolution within tolerance.

        Args:
            actual: Actual image resolution (width, height)
            expected: Expected image resolution (width, height)

        Returns:
            True if resolutions match within tolerance, False otherwise
        """
        if self.tolerance == 0:
            return actual == expected

        width_diff = abs(actual[0] - expected[0])
        height_diff = abs(actual[1] - expected[1])

        return width_diff <= self.tolerance and height_diff <= self.tolerance
