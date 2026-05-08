"""Keypoint Annotation Validator Module.

Validates keypoint annotation data before ingestion to prevent training
failures on the client side. Checks JSON structure, coordinate ranges,
bounding box feasibility, visibility values, and keypoint count consistency.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class KeypointAnnotationValidator(BaseValidator):
    """Validator for keypoint annotation data.

    Ensures annotation and visibility columns contain valid JSON with
    correct structure, coordinates within expected range, and bounding
    boxes with positive width and height.

    Attributes:
        target_size: Expected image dimensions (height, width)
        num_keypoints: Expected number of keypoints per record
        annotation_column: Name of the annotation column in CSV
        visibility_column: Name of the visibility column in CSV
    """

    def __init__(
        self,
        target_size: Tuple[int, int],
        num_keypoints: Optional[int] = None,
        annotation_column: str = "Annotation",
        visibility_column: str = "Visibility",
        name: str = "Keypoint Annotation",
    ):
        super().__init__(name)
        self.target_size = target_size
        self.num_keypoints = num_keypoints
        self.annotation_column = annotation_column
        self.visibility_column = visibility_column

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        try:
            df = self._load_data(data)
            if df is None or df.empty:
                return self._create_result(
                    is_valid=False,
                    errors=["No data found to validate"],
                )

            errors = []
            warnings = []

            # Check annotation column exists
            if self.annotation_column not in df.columns:
                return self._create_result(
                    is_valid=False,
                    errors=[f"Missing required column: {self.annotation_column}"],
                )

            has_visibility = self.visibility_column in df.columns

            for idx, row in df.iterrows():
                row_errors = self._validate_row(row, idx, has_visibility)
                errors.extend(row_errors)

            # Check keypoint name consistency across all records
            consistency_errors = self._validate_keypoint_consistency(df)
            errors.extend(consistency_errors)

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                metadata={
                    "rows_checked": len(df),
                    "target_size": self.target_size,
                    "num_keypoints": self.num_keypoints,
                },
            )

        except Exception as e:
            logger.error(f"Error during keypoint annotation validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Keypoint annotation validation error: {str(e)}"],
            )

    def _load_data(self, data: Any) -> Optional[pd.DataFrame]:
        try:
            if isinstance(data, pd.DataFrame):
                return data
            elif isinstance(data, (str, Path)):
                return pd.read_csv(
                    data, encoding="utf-8", on_bad_lines="warn"
                )
            return None
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            return None

    def _validate_row(
        self, row: pd.Series, idx: int, has_visibility: bool
    ) -> List[str]:
        errors = []
        row_label = f"Row {idx + 1}"

        # 1. Parse and validate Annotation JSON
        annotation = self._parse_json(row, self.annotation_column, row_label)
        if annotation is None:
            errors.append(f"{row_label}: Invalid JSON in {self.annotation_column}")
            return errors

        if not isinstance(annotation, dict):
            errors.append(
                f"{row_label}: {self.annotation_column} must be a JSON object, "
                f"got {type(annotation).__name__}"
            )
            return errors

        # 2. Validate keypoint count
        if self.num_keypoints is not None and len(annotation) != self.num_keypoints:
            errors.append(
                f"{row_label}: Expected {self.num_keypoints} keypoints, "
                f"got {len(annotation)}"
            )

        # 3. Validate each keypoint coordinate
        x_coords = []
        y_coords = []
        for kp_name, value in annotation.items():
            kp_errors, x, y = self._validate_keypoint_value(
                kp_name, value, row_label
            )
            errors.extend(kp_errors)
            if x is not None and y is not None:
                x_coords.append(x)
                y_coords.append(y)

        # 4. Validate bounding box feasibility
        if len(x_coords) >= 2 and len(y_coords) >= 2:
            x_range = max(x_coords) - min(x_coords)
            y_range = max(y_coords) - min(y_coords)
            if x_range <= 0 or y_range <= 0:
                errors.append(
                    f"{row_label}: Keypoints produce a degenerate bounding box "
                    f"(width={x_range:.4f}, height={y_range:.4f}). "
                    f"At least 2 keypoints must differ in both x and y coordinates."
                )

        # 5. Validate Visibility if present
        if has_visibility:
            vis_errors = self._validate_visibility(
                row, annotation, row_label
            )
            errors.extend(vis_errors)

        return errors

    def _validate_keypoint_value(
        self, kp_name: str, value: Any, row_label: str
    ) -> Tuple[List[str], Optional[float], Optional[float]]:
        errors = []

        if isinstance(value, list):
            if len(value) != 2:
                errors.append(
                    f"{row_label}: Keypoint '{kp_name}' must have exactly "
                    f"2 coordinates [x, y], got {len(value)}"
                )
                return errors, None, None
            x, y = value[0], value[1]
        elif isinstance(value, dict) and "x" in value and "y" in value:
            x, y = value["x"], value["y"]
        else:
            errors.append(
                f"{row_label}: Keypoint '{kp_name}' must be [x, y] list or "
                f"{{\"x\": x, \"y\": y}} dict"
            )
            return errors, None, None

        if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
            errors.append(
                f"{row_label}: Keypoint '{kp_name}' coordinates must be numeric"
            )
            return errors, None, None

        if x < 0 or y < 0:
            errors.append(
                f"{row_label}: Keypoint '{kp_name}' has negative coordinates "
                f"({x}, {y})"
            )

        return errors, float(x), float(y)

    def _validate_visibility(
        self, row: pd.Series, annotation: Dict, row_label: str
    ) -> List[str]:
        errors = []

        visibility = self._parse_json(row, self.visibility_column, row_label)
        if visibility is None:
            errors.append(f"{row_label}: Invalid JSON in {self.visibility_column}")
            return errors

        if not isinstance(visibility, dict):
            errors.append(
                f"{row_label}: {self.visibility_column} must be a JSON object"
            )
            return errors

        # Check keys match annotation keys
        annotation_keys = set(annotation.keys())
        visibility_keys = set(visibility.keys())
        missing = annotation_keys - visibility_keys
        if missing:
            errors.append(
                f"{row_label}: Visibility missing keys: {sorted(missing)}"
            )

        # Check values are 0 or 1
        for key, val in visibility.items():
            if val not in (0, 1):
                errors.append(
                    f"{row_label}: Visibility['{key}'] must be 0 or 1, got {val}"
                )

        return errors

    def _validate_keypoint_consistency(self, df: pd.DataFrame) -> List[str]:
        errors = []
        reference_keys = None

        for idx, row in df.iterrows():
            annotation = self._parse_json(row, self.annotation_column, f"Row {idx + 1}")
            if annotation is None or not isinstance(annotation, dict):
                continue

            keys = set(annotation.keys())
            if reference_keys is None:
                reference_keys = keys
            elif keys != reference_keys:
                errors.append(
                    f"Row {idx + 1}: Keypoint names differ from first record. "
                    f"Missing: {sorted(reference_keys - keys)}, "
                    f"Extra: {sorted(keys - reference_keys)}"
                )

        return errors

    def _parse_json(
        self, row: pd.Series, column: str, row_label: str
    ) -> Optional[Any]:
        try:
            value = row[column]
            if pd.isna(value):
                return None
            return json.loads(str(value))
        except (json.JSONDecodeError, TypeError):
            return None
