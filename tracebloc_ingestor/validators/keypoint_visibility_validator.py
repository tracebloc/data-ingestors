"""Keypoint Visibility Validator Module.

Validates the Visibility column for keypoint detection data. Ensures
visibility values are valid integers (0 or 1) and that visibility keys
match the corresponding annotation keypoint names.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class KeypointVisibilityValidator(BaseValidator):
    """Validator for keypoint visibility data.

    Ensures the Visibility column contains valid JSON with integer values
    (0 or 1) and that keys match the corresponding Annotation column.

    Attributes:
        annotation_column: Name of the annotation column in CSV
        visibility_column: Name of the visibility column in CSV
    """

    def __init__(
        self,
        annotation_column: str = "Annotation",
        visibility_column: str = "Visibility",
        name: str = "Keypoint Visibility",
    ):
        super().__init__(name)
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

            if self.visibility_column not in df.columns:
                return self._create_result(
                    is_valid=False,
                    errors=[f"Missing required column: {self.visibility_column}"],
                )

            has_annotation = self.annotation_column in df.columns
            errors = []

            for idx, row in df.iterrows():
                row_errors = self._validate_row(row, idx, has_annotation)
                errors.extend(row_errors)

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                metadata={"rows_checked": len(df)},
            )

        except Exception as e:
            logger.error(f"Error during visibility validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Keypoint visibility validation error: {str(e)}"],
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
        self, row: pd.Series, idx: int, has_annotation: bool
    ) -> List[str]:
        errors = []
        row_label = f"Row {idx + 1}"

        visibility = self._parse_json(row, self.visibility_column, row_label)
        if visibility is None:
            errors.append(f"{row_label}: Invalid JSON in {self.visibility_column}")
            return errors

        if not isinstance(visibility, dict):
            errors.append(
                f"{row_label}: {self.visibility_column} must be a JSON object"
            )
            return errors

        # Check values are 0 or 1
        for key, val in visibility.items():
            if val not in (0, 1):
                errors.append(
                    f"{row_label}: Visibility['{key}'] must be 0 or 1, got {val}"
                )

        # Check keys match annotation keys if annotation column exists
        if has_annotation:
            annotation = self._parse_json(row, self.annotation_column, row_label)
            if annotation is not None and isinstance(annotation, dict):
                annotation_keys = set(annotation.keys())
                visibility_keys = set(visibility.keys())
                missing = annotation_keys - visibility_keys
                extra = visibility_keys - annotation_keys
                if missing:
                    errors.append(
                        f"{row_label}: Visibility missing keys: {sorted(missing)}"
                    )
                if extra:
                    errors.append(
                        f"{row_label}: Visibility has extra keys not in Annotation: {sorted(extra)}"
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
