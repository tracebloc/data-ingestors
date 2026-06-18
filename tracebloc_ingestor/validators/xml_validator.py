"""Pascal VOC XML Validator Module.

This module provides validation for Pascal VOC XML annotation files to ensure
they conform to the standard Pascal VOC format structure and contain valid data.
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, List, Dict, Optional
import logging

from .base import BaseValidator, ValidationResult
from ..config import Config

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class PascalVOCXMLValidator(BaseValidator):
    """Validator for Pascal VOC XML annotation files.

    This validator ensures that XML files conform to the Pascal VOC format
    specification, including proper structure, required elements, and valid data types.

    Attributes:
        strict_mode: Whether to enforce strict validation (default: True)
    """

    def __init__(
        self, name: str = "Pascal VOC XML Validator", strict_mode: bool = True
    ):
        """Initialize the Pascal VOC XML validator.

        Args:
            name: Human-readable name of the validator
            strict_mode: Whether to enforce strict validation
        """
        super().__init__(name)
        self.strict_mode = strict_mode

        # Required root elements for Pascal VOC format
        self.required_root_elements = {
            "folder",
            "filename",
            "source",
            "size",
            "segmented",
        }

        # Required source sub-elements
        self.required_source_elements = {"database", "annotation"}

        # Required size sub-elements
        self.required_size_elements = {"width", "height", "depth"}

        # Required object elements
        self.required_object_elements = {
            "name",
            "pose",
            "truncated",
            "difficult",
            "bndbox",
        }

        # Required bndbox elements
        self.required_bndbox_elements = {"xmin", "ymin", "xmax", "ymax"}

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate Pascal VOC XML files.

        Args:
            data: File path, directory path, or list of file paths to validate
            **kwargs: Additional validation parameters
                - recursive: Whether to search directories recursively (default: True)
                - ignore_hidden: Whether to ignore hidden files (default: True)

        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            recursive = kwargs.get("recursive", True)
            ignore_hidden = kwargs.get("ignore_hidden", True)

            # Get list of XML files to validate
            files_to_validate = self._get_xml_files(
                (self._config or config).SRC_PATH, recursive, ignore_hidden
            )

            if not files_to_validate:
                return self._create_result(
                    is_valid=False,
                    errors=["No XML files found to validate"],
                    metadata={"files_checked": 0},
                )

            # Validate each XML file
            return self._validate_xml_files(files_to_validate)

        except Exception as e:
            logger.error(f"Error during XML validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )

    def _get_xml_files(
        self, data: Any, recursive: bool, ignore_hidden: bool
    ) -> List[Path]:
        """Get list of XML files to validate from the input data.

        Args:
            data: Input data (file path, directory, or list of paths)
            recursive: Whether to search directories recursively
            ignore_hidden: Whether to ignore hidden files

        Returns:
            List of XML file paths to validate
        """
        files_to_validate = []

        if isinstance(data, (str, Path)):
            path = Path(data)
            if path.is_file() and path.suffix.lower() == ".xml":
                files_to_validate.append(path)
            elif path.is_dir():
                pattern = "**/*.xml" if recursive else "*.xml"
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
                    if path.is_file() and path.suffix.lower() == ".xml":
                        files_to_validate.append(path)
                    else:
                        logger.warning(f"XML file not found: {path}")
                else:
                    logger.warning(f"Invalid file path type: {type(item)}")
        else:
            raise ValueError(f"Unsupported data type for validation: {type(data)}")

        return files_to_validate

    def _validate_xml_files(self, files: List[Path]) -> ValidationResult:
        """Validate XML files for Pascal VOC format compliance.

        Args:
            files: List of XML file paths to validate

        Returns:
            ValidationResult containing validation status and messages
        """
        if not files:
            return self._create_result(
                is_valid=False,
                errors=["No XML files to validate"],
                metadata={"files_checked": 0},
            )

        all_valid = True
        all_errors = []
        all_warnings = []
        validation_metadata = {
            "files_checked": len(files),
            "valid_files": 0,
            "invalid_files": 0,
            "file_details": [],
        }

        # Create progress bar
        progress_bar = self._create_progress_bar(
            len(files), "Validating Pascal VOC XML files"
        )

        try:
            for file_path in files:
                file_result = self._validate_single_xml(file_path)

                if file_result.is_valid:
                    validation_metadata["valid_files"] += 1
                else:
                    validation_metadata["invalid_files"] += 1
                    all_valid = False

                # Collect errors and warnings
                all_errors.extend(
                    [f"{file_path.name}: {error}" for error in file_result.errors]
                )
                all_warnings.extend(
                    [f"{file_path.name}: {warning}" for warning in file_result.warnings]
                )

                # Store file details
                validation_metadata["file_details"].append(
                    {
                        "file": str(file_path),
                        "valid": file_result.is_valid,
                        "errors": file_result.errors,
                        "warnings": file_result.warnings,
                        "metadata": file_result.metadata,
                    }
                )

                # Update progress bar
                if progress_bar:
                    progress_bar.update(1)
        finally:
            # Close progress bar
            if progress_bar:
                progress_bar.close()

        return self._create_result(
            is_valid=all_valid,
            errors=all_errors if not all_valid else [],
            warnings=all_warnings,
            metadata=validation_metadata,
        )

    def _validate_single_xml(self, file_path: Path) -> ValidationResult:
        """Validate a single XML file for Pascal VOC format compliance.

        Args:
            file_path: Path to the XML file to validate

        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            # Parse XML file
            tree = ET.parse(file_path)
            root = tree.getroot()

            # Check if root element is 'annotation'
            if root.tag != "annotation":
                return self._create_result(
                    is_valid=False,
                    errors=[f"Root element must be 'annotation', found '{root.tag}'"],
                    metadata={"file": str(file_path), "root_tag": root.tag},
                )

            errors = []
            warnings = []
            metadata = {"file": str(file_path)}

            # Validate required root elements
            root_validation = self._validate_root_elements(root)
            errors.extend(root_validation["errors"])
            warnings.extend(root_validation["warnings"])
            metadata.update(root_validation["metadata"])

            # Validate source element
            source_validation = self._validate_source_element(root)
            errors.extend(source_validation["errors"])
            warnings.extend(source_validation["warnings"])
            metadata.update(source_validation["metadata"])

            # Validate size element
            size_validation = self._validate_size_element(root)
            errors.extend(size_validation["errors"])
            warnings.extend(size_validation["warnings"])
            metadata.update(size_validation["metadata"])

            # Declared image dimensions (only when they parsed cleanly as
            # positive ints) — used to bound bbox coordinates and to cross-check
            # against the actual image below.
            declared_w = size_validation["metadata"].get("width")
            declared_h = size_validation["metadata"].get("height")

            # Cross-check the declared <size> against the ACTUAL image: a wrong
            # declared size silently corrupts coordinate scaling at training
            # (and would let a bbox that's "within" a too-large declared size sit
            # outside the real image). Best-effort — skips when the image can't
            # be located/read (FilePairing/FileType own the missing-image case).
            errors.extend(
                self._validate_size_matches_image(
                    file_path,
                    declared_w,
                    declared_h,
                )
            )

            # Validate objects (bbox coordinates are bounded by the declared
            # image size when it's known).
            objects_validation = self._validate_objects(root, declared_w, declared_h)
            errors.extend(objects_validation["errors"])
            warnings.extend(objects_validation["warnings"])
            metadata.update(objects_validation["metadata"])

            is_valid = len(errors) == 0

            return self._create_result(
                is_valid=is_valid, errors=errors, warnings=warnings, metadata=metadata
            )

        except ET.ParseError as e:
            return self._create_result(
                is_valid=False,
                errors=[f"XML parsing error: {str(e)}"],
                metadata={"file": str(file_path), "error_type": "parse_error"},
            )
        except Exception as e:
            return self._create_result(
                is_valid=False,
                errors=[f"Unexpected error: {str(e)}"],
                metadata={"file": str(file_path), "error_type": "unexpected_error"},
            )

    def _validate_root_elements(self, root: ET.Element) -> Dict[str, Any]:
        """Validate required root elements.

        Args:
            root: Root XML element

        Returns:
            Dictionary containing validation results
        """
        errors = []
        warnings = []
        metadata = {}

        # Check for required elements
        present_elements = {child.tag for child in root}
        missing_elements = self.required_root_elements - present_elements

        if missing_elements:
            errors.append(f"Missing required root elements: {sorted(missing_elements)}")

        # Validate folder element
        folder_elem = root.find("folder")
        if folder_elem is not None:
            if folder_elem.text is None or folder_elem.text.strip() == "":
                errors.append("Folder element must have non-empty text content")
            else:
                metadata["folder"] = folder_elem.text.strip()
        elif "folder" in self.required_root_elements:
            errors.append("Missing required 'folder' element")

        # Validate filename element
        filename_elem = root.find("filename")
        if filename_elem is not None:
            if filename_elem.text is None or filename_elem.text.strip() == "":
                errors.append("Filename element must have non-empty text content")
            else:
                metadata["filename"] = filename_elem.text.strip()
        elif "filename" in self.required_root_elements:
            errors.append("Missing required 'filename' element")

        # Validate segmented element
        segmented_elem = root.find("segmented")
        if segmented_elem is not None:
            if segmented_elem.text not in ["0", "1"]:
                errors.append("Segmented element must be '0' or '1'")
            else:
                metadata["segmented"] = segmented_elem.text
        elif "segmented" in self.required_root_elements:
            errors.append("Missing required 'segmented' element")

        return {"errors": errors, "warnings": warnings, "metadata": metadata}

    def _validate_source_element(self, root: ET.Element) -> Dict[str, Any]:
        """Validate source element and its sub-elements.

        Args:
            root: Root XML element

        Returns:
            Dictionary containing validation results
        """
        errors = []
        warnings = []
        metadata = {}

        source_elem = root.find("source")
        if source_elem is None:
            errors.append("Missing required 'source' element")
            return {"errors": errors, "warnings": warnings, "metadata": metadata}

        # Check required source sub-elements
        present_source_elements = {child.tag for child in source_elem}
        missing_source_elements = (
            self.required_source_elements - present_source_elements
        )

        if missing_source_elements:
            errors.append(
                f"Missing required source elements: {sorted(missing_source_elements)}"
            )

        # Validate database element
        database_elem = source_elem.find("database")
        if database_elem is not None:
            if database_elem.text is None or database_elem.text.strip() == "":
                errors.append("Database element must have non-empty text content")
            else:
                metadata["database"] = database_elem.text.strip()

        # Validate annotation element
        annotation_elem = source_elem.find("annotation")
        if annotation_elem is not None:
            if annotation_elem.text is None or annotation_elem.text.strip() == "":
                errors.append("Annotation element must have non-empty text content")
            else:
                metadata["annotation"] = annotation_elem.text.strip()

        return {"errors": errors, "warnings": warnings, "metadata": metadata}

    def _validate_size_element(self, root: ET.Element) -> Dict[str, Any]:
        """Validate size element and its sub-elements.

        Args:
            root: Root XML element

        Returns:
            Dictionary containing validation results
        """
        errors = []
        warnings = []
        metadata = {}

        size_elem = root.find("size")
        if size_elem is None:
            errors.append("Missing required 'size' element")
            return {"errors": errors, "warnings": warnings, "metadata": metadata}

        # Check required size sub-elements
        present_size_elements = {child.tag for child in size_elem}
        missing_size_elements = self.required_size_elements - present_size_elements

        if missing_size_elements:
            errors.append(
                f"Missing required size elements: {sorted(missing_size_elements)}"
            )

        # Validate width, height, and depth elements
        for elem_name in ["width", "height", "depth"]:
            elem = size_elem.find(elem_name)
            if elem is not None:
                try:
                    value = int(elem.text) if elem.text else 0
                    if value <= 0:
                        errors.append(
                            f"{elem_name} must be a positive integer, found: {value}"
                        )
                    else:
                        metadata[elem_name] = value
                except (ValueError, TypeError):
                    errors.append(
                        f"{elem_name} must be a valid integer, found: {elem.text}"
                    )
            elif elem_name in self.required_size_elements:
                errors.append(f"Missing required '{elem_name}' element")

        return {"errors": errors, "warnings": warnings, "metadata": metadata}

    def _validate_objects(
        self,
        root: ET.Element,
        declared_w: Optional[int] = None,
        declared_h: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Validate object elements and their sub-elements.

        Args:
            root: Root XML element
            declared_w: Declared image width (from <size>), or None — used to
                bound each bbox's xmax.
            declared_h: Declared image height (from <size>), or None — used to
                bound each bbox's ymax.

        Returns:
            Dictionary containing validation results
        """
        errors = []
        warnings = []
        metadata = {"object_count": 0, "objects": []}

        objects = root.findall("object")
        metadata["object_count"] = len(objects)

        if len(objects) == 0:
            warnings.append("No objects found in annotation file")
            return {"errors": errors, "warnings": warnings, "metadata": metadata}

        for i, obj in enumerate(objects):
            obj_validation = self._validate_single_object(
                obj, i, declared_w, declared_h
            )
            errors.extend(obj_validation["errors"])
            warnings.extend(obj_validation["warnings"])
            metadata["objects"].append(obj_validation["metadata"])

        return {"errors": errors, "warnings": warnings, "metadata": metadata}

    def _validate_single_object(
        self,
        obj: ET.Element,
        index: int,
        declared_w: Optional[int] = None,
        declared_h: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Validate a single object element.

        Args:
            obj: Object XML element
            index: Index of the object in the list
            declared_w: Declared image width, or None.
            declared_h: Declared image height, or None.

        Returns:
            Dictionary containing validation results
        """
        errors = []
        warnings = []
        metadata = {"index": index}

        # Check required object elements
        present_obj_elements = {child.tag for child in obj}
        missing_obj_elements = self.required_object_elements - present_obj_elements

        if missing_obj_elements:
            errors.append(
                f"Object {index}: Missing required elements: {sorted(missing_obj_elements)}"
            )

        # Validate name element
        name_elem = obj.find("name")
        if name_elem is not None:
            if name_elem.text is None or name_elem.text.strip() == "":
                errors.append(
                    f"Object {index}: Name element must have non-empty text content"
                )
            else:
                metadata["name"] = name_elem.text.strip()
        elif "name" in self.required_object_elements:
            errors.append(f"Object {index}: Missing required 'name' element")

        # Validate pose element
        pose_elem = obj.find("pose")
        if pose_elem is not None:
            if pose_elem.text is None or pose_elem.text.strip() == "":
                errors.append(
                    f"Object {index}: Pose element must have non-empty text content"
                )
            else:
                metadata["pose"] = pose_elem.text.strip()
        elif "pose" in self.required_object_elements:
            errors.append(f"Object {index}: Missing required 'pose' element")

        # Validate truncated element
        truncated_elem = obj.find("truncated")
        if truncated_elem is not None:
            if truncated_elem.text not in ["0", "1"]:
                errors.append(
                    f"Object {index}: Truncated element must be '0' or '1', found: {truncated_elem.text}"
                )
            else:
                metadata["truncated"] = int(truncated_elem.text)
        elif "truncated" in self.required_object_elements:
            errors.append(f"Object {index}: Missing required 'truncated' element")

        # Validate difficult element. Pascal VOC officially uses 0/1, but
        # real-world detection datasets (e.g. VisDrone — our bundled OD sample)
        # use higher difficulty levels. Accept any non-negative integer; flag
        # non-standard values as a warning rather than failing ingestion.
        difficult_elem = obj.find("difficult")
        if difficult_elem is not None:
            try:
                difficult_val = (
                    int(difficult_elem.text)
                    if difficult_elem.text is not None
                    else None
                )
            except (ValueError, TypeError):
                difficult_val = None
            if difficult_val is None or difficult_val < 0:
                errors.append(
                    f"Object {index}: Difficult element must be a non-negative integer, "
                    f"found: {difficult_elem.text}"
                )
            else:
                metadata["difficult"] = difficult_val
                if difficult_val not in (0, 1):
                    warnings.append(
                        f"Object {index}: Difficult={difficult_val} is outside the standard "
                        f"Pascal VOC 0/1 (accepted; e.g. VisDrone uses 0/1/2)"
                    )
        elif "difficult" in self.required_object_elements:
            errors.append(f"Object {index}: Missing required 'difficult' element")

        # Validate bndbox element
        bndbox_validation = self._validate_bndbox_element(
            obj, index, declared_w, declared_h
        )
        errors.extend(bndbox_validation["errors"])
        warnings.extend(bndbox_validation["warnings"])
        metadata.update(bndbox_validation["metadata"])

        return {"errors": errors, "warnings": warnings, "metadata": metadata}

    def _validate_bndbox_element(
        self,
        obj: ET.Element,
        index: int,
        declared_w: Optional[int] = None,
        declared_h: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Validate bndbox element and its coordinates.

        Args:
            obj: Object XML element
            index: Index of the object in the list
            declared_w: Declared image width, or None — bounds xmax.
            declared_h: Declared image height, or None — bounds ymax.

        Returns:
            Dictionary containing validation results
        """
        errors = []
        warnings = []
        metadata = {}

        bndbox_elem = obj.find("bndbox")
        if bndbox_elem is None:
            errors.append(f"Object {index}: Missing required 'bndbox' element")
            return {"errors": errors, "warnings": warnings, "metadata": metadata}

        # Check required bndbox elements
        present_bndbox_elements = {child.tag for child in bndbox_elem}
        missing_bndbox_elements = (
            self.required_bndbox_elements - present_bndbox_elements
        )

        if missing_bndbox_elements:
            errors.append(
                f"Object {index}: Missing required bndbox elements: {sorted(missing_bndbox_elements)}"
            )

        # Validate coordinates
        coords = {}
        for coord_name in ["xmin", "ymin", "xmax", "ymax"]:
            coord_elem = bndbox_elem.find(coord_name)
            if coord_elem is not None:
                try:
                    value = int(coord_elem.text) if coord_elem.text else 0
                    if value < 0:
                        errors.append(
                            f"Object {index}: {coord_name} must be non-negative, found: {value}"
                        )
                    else:
                        coords[coord_name] = value
                except (ValueError, TypeError):
                    errors.append(
                        f"Object {index}: {coord_name} must be a valid integer, found: {coord_elem.text}"
                    )
            elif coord_name in self.required_bndbox_elements:
                errors.append(
                    f"Object {index}: Missing required '{coord_name}' element"
                )

        # Validate coordinate relationships
        if len(coords) == 4:
            if coords["xmin"] >= coords["xmax"]:
                errors.append(
                    f"Object {index}: xmin ({coords['xmin']}) must be less than xmax ({coords['xmax']})"
                )
            if coords["ymin"] >= coords["ymax"]:
                errors.append(
                    f"Object {index}: ymin ({coords['ymin']}) must be less than ymax ({coords['ymax']})"
                )

            # Calculate bounding box area
            width = coords["xmax"] - coords["xmin"]
            height = coords["ymax"] - coords["ymin"]
            area = width * height

            if area == 0:
                errors.append(f"Object {index}: Bounding box has zero area")
            elif area < 10:  # Very small bounding box
                warnings.append(
                    f"Object {index}: Very small bounding box (area: {area})"
                )

            # Bound the box by the declared image size: a bbox extending past the
            # image edge is malformed and crashes / corrupts training. (xmin/ymin
            # are already checked non-negative above.)
            if declared_w is not None and coords["xmax"] > declared_w:
                errors.append(
                    f"Object {index}: xmax ({coords['xmax']}) exceeds image width "
                    f"({declared_w}) — bounding box extends past the image edge."
                )
            if declared_h is not None and coords["ymax"] > declared_h:
                errors.append(
                    f"Object {index}: ymax ({coords['ymax']}) exceeds image height "
                    f"({declared_h}) — bounding box extends past the image edge."
                )

            metadata["bbox"] = coords
            metadata["area"] = area

        return {"errors": errors, "warnings": warnings, "metadata": metadata}

    def _validate_size_matches_image(
        self,
        file_path: Path,
        declared_w: Optional[int],
        declared_h: Optional[int],
    ) -> List[str]:
        """Cross-check the annotation's declared ``<size>`` against the ACTUAL
        image dimensions.

        The XML lives at ``<SRC>/annotations/<stem>.xml`` and its image at
        ``<SRC>/images/<stem>.<imgext>``; a declared size that disagrees with the
        real image silently corrupts coordinate scaling at training (and lets a
        bbox that's "within" a too-large declared size sit outside the real
        image).

        The image is located by the XML **stem** — NOT the annotation's
        ``<filename>`` element — because the ingestor pairs images to annotations
        by stem (``FilePairingValidator``). A stale / wrong ``<filename>`` could
        otherwise point the check at a different on-disk image that happens to
        match the declared size, while the actually-paired image still disagrees
        (bugbot, PR #314).

        Best-effort — returns no error (a benign skip) when the declared size is
        unknown, the image can't be located, or it can't be read: the
        FilePairing / FileType validators own the missing-image case, and Pillow
        may be unavailable. Returns a one-item error list on a real mismatch.
        """
        if declared_w is None or declared_h is None:
            return []
        try:
            from PIL import Image, UnidentifiedImageError
        except ImportError:  # pragma: no cover - Pillow is a runtime dep
            return []

        images_dir = file_path.parent.parent / "images"
        # Match the stem-based pairing the ingestor uses; the image extension is
        # whatever's on disk (datasets ship .jpg / .jpeg / .png).
        candidates = [
            images_dir / f"{file_path.stem}{ext}" for ext in (".jpg", ".jpeg", ".png")
        ]
        img_path = next((c for c in candidates if c.is_file()), None)
        if img_path is None:
            return []
        try:
            with Image.open(img_path) as im:
                actual_w, actual_h = im.size
        except (OSError, UnidentifiedImageError):  # corrupt/unreadable image
            return []

        if (actual_w, actual_h) != (declared_w, declared_h):
            return [
                f"declared <size> {declared_w}x{declared_h} does not match the "
                f"actual image '{img_path.name}' ({actual_w}x{actual_h}); fix the "
                f"annotation's <size> or the image."
            ]
        return []
