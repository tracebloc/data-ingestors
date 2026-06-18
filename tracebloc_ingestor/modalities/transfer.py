"""Per-category sidecar-transfer factories (structural refactor — backend#796, P3c).

One transfer function per file-bearing category, each
``(record, options) -> record | None`` (None = the record's sidecar file(s)
could not be staged, so the record is dropped). These are the bodies of the
old ``file_transfer.map_file_transfer`` if/elif arms, moved VERBATIM so the
copy behavior — including the atomic image+annotation / image+mask pre-checks —
is byte-for-byte identical. They are attached to each ModalitySpec
(``transfer``); ``map_file_transfer`` is now a thin lookup over the registry.

The copy primitives (image_transfer, _find_src, _find_mask_src, …) still live
in file_transfer.py; this module orchestrates them per category exactly as
before.

NOTE: semantic_segmentation reads ``mask_id`` off the record here. When the
dataset DECLARES ``mask_id`` in its schema (the template) it's a real column —
present on the cleaned record and stored, because the training client reads it
from MySQL to locate masks (backend#816). When it is NOT declared,
``map_file_transfer`` LENDS it from the raw source record for the duration of
the copy and strips it before return (not stored). Either way this factory
reads ``record.get("mask_id")`` unchanged.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from ..config import Config
from ..file_transfer import (
    _find_mask_src,
    _find_src,
    annotation_transfer,
    config,
    image_transfer,
    logger,
    mask_transfer,
    text_transfer,
)
from ..utils.constants import RED, RESET


def image_classification(
    record: Dict[str, Any], options: Dict[str, Any], cfg: Optional[Config] = None
) -> Optional[Dict[str, Any]]:
    return image_transfer(record, options, cfg=cfg)


def keypoint_detection(
    record: Dict[str, Any], options: Dict[str, Any], cfg: Optional[Config] = None
) -> Optional[Dict[str, Any]]:
    return image_transfer(record, options, cfg=cfg)


def object_detection(
    record: Dict[str, Any], options: Dict[str, Any], cfg: Optional[Config] = None
) -> Optional[Dict[str, Any]]:
    # Atomic: only copy image+annotation together. Pre-verify both sources so a
    # missing image (image_transfer returns the record, not None, on missing
    # source) doesn't let annotation_transfer leave an orphan annotation on
    # disk — and vice versa.
    cfg = cfg or config
    filename = record.get("filename")
    if not filename:
        logger.error(f"{RED}No filename found in record{RESET}")
        return None
    image_src_path, image_filename = _find_src(
        "images", filename, options.get("extension"), cfg=cfg
    )
    if image_src_path is None:
        logger.error(
            f"{RED}Source image not found: {os.path.join(cfg.SRC_PATH, 'images', image_filename)} — skipping record{RESET}"
        )
        return None
    annotation_src_path, annotation_filename = _find_src(
        "annotations", filename, ".xml", cfg=cfg
    )
    if annotation_src_path is None:
        logger.error(
            f"{RED}Source annotation not found: {os.path.join(cfg.SRC_PATH, 'annotations', annotation_filename)} — skipping record{RESET}"
        )
        return None
    record = image_transfer(record, options, image_src_path, image_filename, cfg=cfg)
    return annotation_transfer(
        record, options, ".xml", annotation_src_path, annotation_filename, cfg=cfg
    )


def text_classification(
    record: Dict[str, Any], options: Dict[str, Any], cfg: Optional[Config] = None
) -> Optional[Dict[str, Any]]:
    return text_transfer(record, options, cfg=cfg)


def token_classification(
    record: Dict[str, Any], options: Dict[str, Any], cfg: Optional[Config] = None
) -> Optional[Dict[str, Any]]:
    # Same on-disk layout as text classification: one .txt per sample in the
    # ``texts`` subdir. BIO tags travel in the labels CSV, not on disk.
    return text_transfer(record, options, cfg=cfg)


def masked_language_modeling(
    record: Dict[str, Any], options: Dict[str, Any], cfg: Optional[Config] = None
) -> Optional[Dict[str, Any]]:
    return text_transfer(record, options, src_subdir="sequences", cfg=cfg)


def semantic_segmentation(
    record: Dict[str, Any], options: Dict[str, Any], cfg: Optional[Config] = None
) -> Optional[Dict[str, Any]]:
    # Atomic: only copy image+mask together. Pre-verify both sources before
    # either copy, since image_transfer returns the record (not None) when the
    # source image is missing — without this pre-check a missing image would
    # still let mask_transfer leave an orphan mask on disk. Both sides resolve
    # their source via shared helpers (`_find_src` / `_find_mask_src`) so the
    # pre-check stays in lockstep with what the copy functions actually look for.
    cfg = cfg or config
    filename = record.get("filename")
    if not filename:
        logger.error(f"{RED}No filename found in record{RESET}")
        return None
    image_src_path, image_filename = _find_src(
        "images", filename, options.get("extension"), cfg=cfg
    )
    if image_src_path is None:
        logger.error(
            f"{RED}Source image not found: {os.path.join(cfg.SRC_PATH, 'images', image_filename)} — skipping record{RESET}"
        )
        return None

    mask_id = record.get("mask_id")
    if not mask_id:
        logger.error(f"{RED}No mask_id found in record{RESET}")
        return None
    mask_src_path, mask_ext, mask_name = _find_mask_src(mask_id, cfg=cfg)
    if mask_src_path is None:
        logger.error(
            f"{RED}Source mask not found: {mask_name} in {cfg.SRC_PATH}/masks/ — skipping record{RESET}"
        )
        return None
    record = image_transfer(record, options, image_src_path, image_filename, cfg=cfg)
    return mask_transfer(record, mask_src_path, mask_ext, mask_name, cfg=cfg)
