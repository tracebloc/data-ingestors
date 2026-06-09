"""File Pairing Validator Module.

For datasets that pair every image with a sidecar file — object detection
(image → annotation XML) and semantic segmentation (image → mask PNG) — this
validator checks at ingestion time that each image has its sidecar and each
sidecar has its image.

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
    ``<src>/annotations``). Pairing is by filename stem with an optional
    ``sidecar_suffix`` appended on the sidecar side:

    - **object_detection**: ``sidecar_suffix=""`` (default) — plain stem
      match, so ``cat.jpg`` pairs with ``cat.xml``.
    - **semantic_segmentation**: ``sidecar_suffix="_mask"`` — the shipped
      template + documented convention is ``<filename>_mask.png``, so
      ``image_001.jpg`` pairs with ``image_001_mask.png`` (issue #196).

    Without the suffix support, the semantic_segmentation shipped sample
    failed pairing — every image was reported "no matching mask" and every
    mask "no matching image", because plain-stem comparison treated
    ``image_001`` and ``image_001_mask`` as unrelated.
    """

    def __init__(
        self,
        image_path: str = "images",
        sidecar_path: str = "annotations",
        sidecar_label: str = "annotation",
        sidecar_suffix: str = "",
        name: str = "File Pairing Validator",
    ):
        super().__init__(name)
        self.image_path = image_path
        self.sidecar_path = sidecar_path
        self.sidecar_label = sidecar_label
        self.sidecar_suffix = sidecar_suffix

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

            image_stems = self._stems(image_dir)
            sidecar_stems = self._stems(sidecar_dir)

            # When a suffix is configured (e.g. semantic_segmentation's
            # ``_mask``), strip it from sidecar stems before comparison so
            # ``image_001_mask`` matches the image ``image_001``. Sidecars
            # that don't carry the suffix are kept as-is so they still show
            # up as orphans (a useful signal, not a silent miss).
            if self.sidecar_suffix:
                # Build: paired set after suffix-strip, plus the orphans
                # (sidecars not following the convention).
                stripped = set()
                non_conforming = set()
                for s in sidecar_stems:
                    if s.endswith(self.sidecar_suffix):
                        stripped.add(s[: -len(self.sidecar_suffix)])
                    else:
                        non_conforming.add(s)
                sidecars_for_compare = stripped
                # Non-conforming sidecars are orphans (named neither to match
                # an image nor to follow the convention).
                extra_orphans = sorted(non_conforming)
            else:
                sidecars_for_compare = sidecar_stems
                extra_orphans = []

            missing = sorted(image_stems - sidecars_for_compare)
            orphans = sorted((sidecars_for_compare - image_stems)) + extra_orphans

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
                    "images": len(image_stems),
                    "sidecars": len(sidecar_stems),
                    "sidecar_suffix": self.sidecar_suffix,
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
