"""Image Resolution Validator Module.

This module provides validation for image resolution uniformity to ensure all images
in a dataset have the same dimensions before ingestion.
"""

from pathlib import Path
from typing import Any, List, Optional, Tuple
import logging

from tracebloc_ingestor.config import Config

try:
    from PIL import Image, UnidentifiedImageError

    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    Image = None
    UnidentifiedImageError = Exception

from .base import BaseValidator, ValidationResult

# Configure unified logging with config
config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

# Absolute lower bound on image dimensions, as (width, height) in pixels
# (#348, RFC-0002 §12.9 + Principle 6). Images with either side below this are
# rejected outright — independently of the per-model ``target_size`` uniformity
# check — because they carry too little spatial structure to train on: standard
# vision backbones downsample by 2 five times (total /32), so a side below 32px
# collapses to a sub-pixel feature map, and 32x32 is the canonical small-image
# benchmark size (CIFAR). A per-model override travels in ``file_options`` as
# ``min_size`` (schema-plumbed like ``target_size``), so the CLI can discover
# and preview the same floor (cli#183). Kept conservative on purpose: this is a
# floor, not the recommended resolution.
MIN_IMAGE_SIZE: Tuple[int, int] = (32, 32)

# Cap on how many offending files are named inline in a too-small error
# message (the full list is always kept in result metadata). Keeps the
# logged / API-bound error string bounded when an entire dataset is below
# the floor — the common case for this gate.
_MAX_LISTED_FILES = 20


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
        subdir: str = "images",
        min_size: Optional[Tuple[int, int]] = None,
    ):
        """Initialize the image resolution validator.

        Args:
            expected_resolution: Expected image resolution as (width, height)
            supported_formats: Set of supported image formats (e.g., {'.jpg', '.png'})
            name: Human-readable name of the validator
            subdir: The ``<SRC_PATH>`` subdirectory whose images this instance
                validates (default ``"images"``). Set to ``"masks"`` to validate
                semantic-segmentation masks — pixel-wise label maps that must be
                readable and share the images' resolution; the default instance
                only ever scans ``<SRC_PATH>/images``.
            min_size: Minimum acceptable image size as (width, height). Images
                with either side below this are rejected. Defaults to
                :data:`MIN_IMAGE_SIZE`; a per-model override is plumbed from
                ``file_options["min_size"]`` (#348).
        """
        super().__init__(name)
        self.expected_resolution = expected_resolution
        self.subdir = subdir
        # Absolute floor (width, height), normalized once here so every
        # downstream read can trust it is a 2-tuple. A falsy override falls
        # back to the module default so the gate is always active, never
        # silently disabled. A non-iterable or non-2-element override is a
        # config error, surfaced at construction rather than swallowed
        # mid-scan as a per-file image-read error.
        if min_size:
            try:
                min_w, min_h = min_size
            except (TypeError, ValueError):
                raise ValueError(
                    f"min_size must be a (width, height) pair; got {min_size!r}"
                )
            self.min_size = (min_w, min_h)
        else:
            self.min_size = MIN_IMAGE_SIZE
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
            path: Accepted for the ``BaseValidator.validate`` interface but
                IGNORED — the images directory is always resolved from the run's
                Config as ``<SRC_PATH>/images`` (see the first line of the body).
                Callers pass the source through ``validate_data``; it has no
                effect here. (Audit foot-gun note: do not "fix" this to honour
                ``path`` without checking every call site.)
            **kwargs: Additional validation parameters
                - recursive: Whether to search directories recursively (default: True)
                - ignore_hidden: Whether to ignore hidden files (default: True)
                - auto_detect_resolution: Whether to auto-detect expected resolution from first image (default: True)

        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            data = f"{(self._config or config).SRC_PATH}/{self.subdir}"
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

    @staticmethod
    def _diagnose_image_error(image_path: Path) -> str:
        """Return a human-readable reason an image could not be read, turning the
        generic "could not be processed" into an actionable cause: empty file,
        corrupt/unsupported format, or decompression bomb."""
        try:
            p = Path(image_path)
            if not p.exists():
                return "file not found"
            if p.stat().st_size == 0:
                return "empty file (0 bytes)"
        except OSError as exc:
            return f"unreadable ({exc.__class__.__name__})"
        try:
            with Image.open(image_path) as img:
                img.verify()  # integrity check without a full decode
            return "unreadable image"
        except UnidentifiedImageError:
            return "not a valid image (corrupt or unsupported format)"
        except Image.DecompressionBombError:
            return "exceeds the safe pixel limit (possible decompression bomb)"
        except Exception as exc:
            return f"{exc.__class__.__name__}: {exc}"

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
        too_small = []
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
                        invalid_files.append(
                            f"{image_path}: {self._diagnose_image_error(image_path)}"
                        )
                        continue

                    resolutions_found.add(resolution)
                    # Absolute minimum-size floor (#348): reject images with
                    # either side below ``min_size``, independently of the
                    # target_size uniformity check below — a uniform dataset of
                    # tiny (e.g. 8x8) images would otherwise pass.
                    if not self._meets_min_size(resolution):
                        too_small.append(f"{image_path}: {resolution}")
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

        # Enforce the minimum-size floor first: it's the most fundamental,
        # actionable failure (the images simply can't be trained on), so it
        # takes precedence over a uniformity / target_size mismatch.
        if too_small:
            # The trigger for this branch is typically a uniformly-tiny
            # dataset, i.e. EVERY file is offending, so cap the list echoed
            # into the (logged, API-bound) error string; the full set stays
            # in metadata. invalid_files is preserved here too — otherwise a
            # dataset that is both too-small and partly corrupt would hide the
            # corrupt files until a second run.
            sample = too_small[:_MAX_LISTED_FILES]
            more = len(too_small) - len(sample)
            listed = f"{sample}{f' (and {more} more)' if more else ''}"
            return self._create_result(
                is_valid=False,
                errors=[
                    f"Images below the minimum size {self.min_size} (width, height) "
                    f"were found — they are too small to train on. Provide larger images or, "
                    f"if your model accepts smaller inputs, lower the floor via "
                    f"file_options.min_size. Offending files: {listed}"
                ],
                metadata={
                    "files_checked": len(image_files),
                    "min_size": self.min_size,
                    "too_small": too_small,
                    "invalid_files": invalid_files,
                    "resolutions_found": sorted(resolutions_found),
                    "expected_resolution": self.expected_resolution,
                },
            )

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
                "min_size": self.min_size,
                "tolerance": self.tolerance,
            },
        )

    def _meets_min_size(self, resolution: Tuple[int, int]) -> bool:
        """Return True if ``resolution`` (width, height) is at least the floor
        on BOTH axes. An image exactly at the floor passes; either side below
        it fails. ``self.min_size`` is already normalized to a 2-tuple in
        ``__init__``, so a list-typed ``file_options.min_size`` compares by
        value here without re-casting."""
        min_w, min_h = self.min_size
        return resolution[0] >= min_w and resolution[1] >= min_h

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
        # Normalize both sides to tuple before comparison. PIL returns
        # ``image.size`` as a tuple; YAML/JSON parses ``target_size: [H, W]``
        # as a list. Python's equality is type-strict (``(256, 256) ==
        # [256, 256]`` is False), so without this normalization the
        # tolerance==0 path falsely flags every image as mismatched even
        # when dimensions are identical. Surfaced during real-cluster
        # ingestion (2026-05-19): a 6-image cats/dogs sample at 256×256
        # with explicit target_size=[256, 256] reported all 6 as
        # "(256, 256) (expected: [256, 256])". The tolerance>0 branch
        # accidentally avoided this by going through index access, which
        # doesn't care about sequence type.
        if self.tolerance == 0:
            return tuple(actual) == tuple(expected)

        width_diff = abs(actual[0] - expected[0])
        height_diff = abs(actual[1] - expected[1])

        return width_diff <= self.tolerance and height_diff <= self.tolerance
