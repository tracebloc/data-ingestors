"""File Pairing Validator Module.

For datasets that pair every image with a sidecar file — object detection
(image → annotation XML) and semantic segmentation (image → mask PNG) — this
validator checks at ingestion time that each image has its sidecar and each
sidecar has its image, matched by filename stem.

Without this, a missing counterpart only surfaces mid-training as a
``FileNotFoundError`` on the offending row, after the job has run for a while.
Catching it up front lets the dataset author fix the whole set in one pass.
"""

import logging
from pathlib import Path
from typing import Any, Set

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class FilePairingValidator(BaseValidator):
    """Ensure images and their sidecar files (annotations / masks) pair up 1:1.

    Directories live under ``config.SRC_PATH`` (e.g. ``<src>/images`` and
    ``<src>/annotations``). Matching is by filename stem, so ``cat.jpg`` pairs
    with ``cat.xml`` / ``cat.png``.
    """

    def __init__(
        self,
        image_path: str = "images",
        sidecar_path: str = "annotations",
        sidecar_label: str = "annotation",
        name: str = "File Pairing Validator",
    ):
        super().__init__(name)
        self.image_path = image_path
        self.sidecar_path = sidecar_path
        self.sidecar_label = sidecar_label

    @staticmethod
    def _stems(directory: Path) -> Set[str]:
        return {
            p.stem
            for p in directory.glob("*")
            if p.is_file() and not p.name.startswith(".")
        }

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        try:
            src = Path(config.SRC_PATH)
            image_dir = src / self.image_path
            sidecar_dir = src / self.sidecar_path

            # A missing directory is the FileTypeValidator's concern (it reports
            # "no files found"); skip here so we don't double-report.
            if not image_dir.is_dir() or not sidecar_dir.is_dir():
                return self._create_result(
                    is_valid=True,
                    metadata={"skipped": "image or sidecar directory not found"},
                )

            images = self._stems(image_dir)
            sidecars = self._stems(sidecar_dir)
            missing = sorted(images - sidecars)   # images with no sidecar
            orphans = sorted(sidecars - images)   # sidecars with no image

            errors = []
            if missing:
                shown = missing[:10]
                errors.append(
                    f"{len(missing)} image(s) have no matching {self.sidecar_label}: "
                    f"{shown}{' …' if len(missing) > 10 else ''}"
                )
            if orphans:
                shown = orphans[:10]
                errors.append(
                    f"{len(orphans)} {self.sidecar_label}(s) have no matching image: "
                    f"{shown}{' …' if len(orphans) > 10 else ''}"
                )

            return self._create_result(
                is_valid=len(errors) == 0,
                errors=errors,
                metadata={
                    "images": len(images),
                    "sidecars": len(sidecars),
                    "images_without_sidecar": len(missing),
                    "orphan_sidecars": len(orphans),
                },
            )
        except Exception as e:
            logger.error(f"File pairing validation error: {e}")
            return self._create_result(
                is_valid=False,
                errors=[f"Validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )
