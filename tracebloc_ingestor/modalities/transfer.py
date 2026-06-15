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

NOTE: semantic_segmentation still reads ``mask_id`` off the record here, as
today. Moving that per-row sidecar pointer off the DB-bound record (the
cross-layer leak: process_record sets it, this reads it, _process_batch pops
it) is deferred to P5, where the record/sidecar split happens — it is out of
scope for this behavior-preserving slice.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from ..file_transfer import (
    _copy_tokenizer_if_present,
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
    record: Dict[str, Any], options: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    return image_transfer(record, options)


def keypoint_detection(
    record: Dict[str, Any], options: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    return image_transfer(record, options)


def object_detection(
    record: Dict[str, Any], options: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    # Atomic: only copy image+annotation together. Pre-verify both sources so a
    # missing image (image_transfer returns the record, not None, on missing
    # source) doesn't let annotation_transfer leave an orphan annotation on
    # disk — and vice versa.
    filename = record.get("filename")
    if not filename:
        logger.error(f"{RED}No filename found in record{RESET}")
        return None
    image_src_path, image_filename = _find_src(
        "images", filename, options.get("extension")
    )
    if image_src_path is None:
        logger.error(
            f"{RED}Source image not found: {os.path.join(config.SRC_PATH, 'images', image_filename)} — skipping record{RESET}"
        )
        return None
    annotation_src_path, annotation_filename = _find_src(
        "annotations", filename, ".xml"
    )
    if annotation_src_path is None:
        logger.error(
            f"{RED}Source annotation not found: {os.path.join(config.SRC_PATH, 'annotations', annotation_filename)} — skipping record{RESET}"
        )
        return None
    record = image_transfer(record, options, image_src_path, image_filename)
    return annotation_transfer(
        record, options, ".xml", annotation_src_path, annotation_filename
    )


def text_classification(
    record: Dict[str, Any], options: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    result = text_transfer(record, options)
    # Optional: ship a custom tokenizer.json so the client uses it instead of
    # the HF default; absent is fine (handled by the optional validator).
    _copy_tokenizer_if_present()
    return result


def token_classification(
    record: Dict[str, Any], options: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    # Same on-disk layout as text classification: one .txt per sample in the
    # ``texts`` subdir. BIO tags travel in the labels CSV, not on disk.
    result = text_transfer(record, options)
    # Optional custom tokenizer.json (same as text classification).
    _copy_tokenizer_if_present()
    return result


def masked_language_modeling(
    record: Dict[str, Any], options: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    result = text_transfer(record, options, src_subdir="sequences")
    # Copy the user's tokenizer.json so the MLM client uses it instead of
    # falling back to bert-base-uncased (a vocab_size mismatch with the model's
    # nn.Embedding would cause a CUDA device-side assert at training). For MLM
    # the tokenizer is mandatory — its presence and [MASK]/[PAD] tokens are
    # enforced by TokenizerValidator at validation.
    _copy_tokenizer_if_present()
    return result


def semantic_segmentation(
    record: Dict[str, Any], options: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    # Atomic: only copy image+mask together. Pre-verify both sources before
    # either copy, since image_transfer returns the record (not None) when the
    # source image is missing — without this pre-check a missing image would
    # still let mask_transfer leave an orphan mask on disk. Both sides resolve
    # their source via shared helpers (`_find_src` / `_find_mask_src`) so the
    # pre-check stays in lockstep with what the copy functions actually look for.
    filename = record.get("filename")
    if not filename:
        logger.error(f"{RED}No filename found in record{RESET}")
        return None
    image_src_path, image_filename = _find_src(
        "images", filename, options.get("extension")
    )
    if image_src_path is None:
        logger.error(
            f"{RED}Source image not found: {os.path.join(config.SRC_PATH, 'images', image_filename)} — skipping record{RESET}"
        )
        return None

    mask_id = record.get("mask_id")
    if not mask_id:
        logger.error(f"{RED}No mask_id found in record{RESET}")
        return None
    mask_src_path, mask_ext, mask_name = _find_mask_src(mask_id)
    if mask_src_path is None:
        logger.error(
            f"{RED}Source mask not found: {mask_name} in {config.SRC_PATH}/masks/ — skipping record{RESET}"
        )
        return None
    record = image_transfer(record, options, image_src_path, image_filename)
    return mask_transfer(record, mask_src_path, mask_ext, mask_name)
