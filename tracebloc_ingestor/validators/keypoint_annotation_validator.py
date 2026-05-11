"""Keypoint Annotation Validator Module.

Validates keypoint annotation data before ingestion to prevent training
failures on the client side. Checks JSON structure, coordinate ranges,
bounding box feasibility, and keypoint name consistency across records.
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

    Ensures annotation column contains valid JSON with correct structure,
    numeric coordinates, non-negative values, and bounding boxes with
    positive width and height. Checks keypoint name consistency across
    all records.

    Attributes:
        annotation_column: Name of the annotation column in CSV
    """

    def __init__(
        self,
        annotation_column: str = "Annotation",
        name: str = "Keypoint Annotation",
    ):
        super().__init__(name)
        self.annotation_column = annotation_column

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

            for idx, row in df.iterrows():
                row_errors = self._validate_row(row, idx)
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

    def _validate_row(self, row: pd.Series, idx: int) -> List[str]:
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

        # 2. Validate each keypoint coordinate
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

        # 3. Validate bounding box feasibility
        if len(x_coords) >= 2 and len(y_coords) >= 2:
            x_range = max(x_coords) - min(x_coords)
            y_range = max(y_coords) - min(y_coords)
            if x_range <= 0 or y_range <= 0:
                errors.append(
                    f"{row_label}: Keypoints produce a degenerate bounding box "
                    f"(width={x_range:.4f}, height={y_range:.4f}). "
                    f"At least 2 keypoints must differ in both x and y coordinates."
                )

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
