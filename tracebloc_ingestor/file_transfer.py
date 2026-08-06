"""File Transfer Module.

This example demonstrates how to ingest image data from a CSV file into a database
and optionally send it to an API. It includes metadata extraction,
supporting both binary data and file-based image processing.
"""

import logging
import os
import re
import shutil
from typing import Any, Dict, Optional

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from tracebloc_ingestor import Config
from tracebloc_ingestor.utils.constants import (
    GREEN,
    RED,
    YELLOW,
    RESET,
    RETRY_MAX_ATTEMPTS,
    RETRY_WAIT_MAX,
    RETRY_WAIT_MIN,
    RETRY_WAIT_MULTIPLIER,
    FileExtension,
    TaskCategory,
)

# Initialize config and configure logging
config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

# `data delete` reclaims a dataset with an ephemeral stage-identity pod
# (uid/gid 65532, client-runtime#172). On a hostPath dataset the kubelet ignores
# that pod's fsGroup, so the ingest side must leave the destination tree
# group-owned by, and group-writable for, that group — otherwise the teardown
# can't remove the files and they leak (remainder of client#259). Create the
# destination dir setgid + group-writable so new entries inherit the group and
# the group can delete them, regardless of the writing uid. 0o2775 = rwxrwsr-x.
DEST_DIR_MODE = 0o2775


def _ensure_dest_dir(path: str) -> None:
    """Create ``path`` if absent, setgid + group-writable, so the ``data
    delete`` teardown group can reclaim the tree (#172).

    Only a directory we *create* is chmod'd — a pre-existing one is left as-is,
    so we neither override an operator's deliberate mode nor mask a genuine
    permission problem (an unwritable dest still surfaces downstream). The chmod
    is best-effort: a failure is logged, not fatal, so ingestion still proceeds.
    """
    existed = os.path.isdir(path)
    os.makedirs(path, exist_ok=True)
    if existed:
        return
    try:
        os.chmod(path, DEST_DIR_MODE)
    except OSError as error:
        logger.warning(
            "Could not set group-writable/setgid mode on %s (%s); the `data "
            "delete` teardown may not be able to reclaim it",
            path,
            error,
        )


# Define retry decorator for file operations
retry_decorator = retry(
    stop=stop_after_attempt(RETRY_MAX_ATTEMPTS),
    wait=wait_exponential(
        multiplier=RETRY_WAIT_MULTIPLIER, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX
    ),
    retry=retry_if_exception_type((OSError, IOError, shutil.Error)),
    before_sleep=before_sleep_log(logger, config.LOG_LEVEL),
    reraise=True,
)


@retry_decorator
def _copy_file_with_retry(src_path: str, dest_path: str) -> None:
    """Copy file with retry logic for handling transient errors."""
    logger.debug(f"Attempting to copy file from {src_path} to {dest_path}")

    # Remove destination file if it exists to avoid conflicts
    if os.path.exists(dest_path):
        logger.debug(f"Destination file exists, removing: {dest_path}")
        os.remove(dest_path)

    shutil.copy(src_path, dest_path)
    logger.debug(f"Successfully copied file from {src_path} to {dest_path}")


def _safe_join(root: str, *parts: str) -> str:
    """Join ``parts`` under ``root`` and guarantee the result stays inside it.

    The per-row ``filename`` / ``mask_id`` come straight from the user's
    manifest and are joined onto ``SRC_PATH`` / ``DEST_PATH`` to read and write
    sidecar files. ``os.path.join`` makes two unsafe moves on hostile input:

      - an absolute component drops everything before it
        (``os.path.join("/data/shared", "images", "/etc/passwd")`` ->
        ``"/etc/passwd"``), and
      - ``..`` segments walk up out of ``root``.

    On the shared cluster PVC that lets a crafted manifest read another
    tenant's files into the dataset, or clobber files anywhere the Job can
    write (#239). Normalise the join and reject anything that resolves outside
    ``root``. ``os.path.abspath`` (not ``realpath``) collapses ``.`` / ``..``
    and makes the path absolute WITHOUT resolving symlinks, so legitimately
    symlinked PVC mounts keep working while traversal / absolute injection is
    blocked.
    """
    root_abs = os.path.abspath(root)
    candidate = os.path.abspath(os.path.join(root_abs, *parts))
    if candidate != root_abs and not candidate.startswith(root_abs + os.sep):
        raise ValueError(
            f"{RED}Refusing path that escapes {root_abs!r}: the manifest value "
            f"{os.path.join(*parts)!r} resolved to {candidate!r}. A filename / "
            f"mask_id must stay within the dataset directory — absolute paths "
            f"and '..' traversal are rejected (path-traversal guard, #239).{RESET}"
        )
    return candidate


def _has_extension(filename: str) -> bool:
    """Check if filename has an extension, handling multiple dots correctly.

    ``FileExtension.get_all_extensions()`` returns values WITH the leading
    dot (``".jpeg"`` etc.), but ``str.split(".")`` on a filename like
    ``"cat1.jpeg"`` yields ``["cat1", "jpeg"]`` — the last part has no
    leading dot. Without normalization, the membership check always
    returned False, ``_find_src`` then appended the extension a second
    time, and the resulting ``cat1.jpeg.jpeg`` path never existed on
    disk. Surfaced during real-cluster ingestion (2026-05-19): all 576
    sample records were ``Source image not found: ...jpeg.jpeg`` while
    the ingestion summary still reported 100% success (which the
    summary fix #99/#100 now addresses separately).
    """
    if not filename:
        return False

    allowed_extensions = FileExtension.get_all_extensions()
    parts = filename.rsplit(".", 1)
    if len(parts) > 1:
        # Compare with leading dot + case-insensitive so ``Cat1.JPEG``
        # also resolves to a hit.
        ext = "." + parts[-1].lower()
        return ext in allowed_extensions
    return False


def _find_src(
    subdirectory: str,
    filename: str,
    extension: str,
    cfg: Optional[Config] = None,
    *,
    force_extension: bool = False,
):
    """Resolve a source file in `SRC_PATH/<subdirectory>/`.

    Returns (src_path, filename_with_ext) on success, or
    (None, filename_with_ext) if the file does not exist. Centralising
    the path/extension resolution keeps atomic pre-checks (e.g. the
    OBJECT_DETECTION and SEMANTIC_SEGMENTATION branches in
    `map_file_transfer`) consistent with what the corresponding
    `*_transfer` function actually copies.

    ``cfg`` is the run's resolved Config, threaded from ``map_file_transfer``
    (P4c); ``None`` falls back to the module-global ``config`` for direct
    callers / tests.

    ``force_extension`` selects how ``extension`` is applied:

    - ``False`` (default): the manifest ``filename`` names *this very file*, so
      keep its own extension when it already has one and only append
      ``extension`` when it doesn't — the image / text case (``a.jpg`` stays
      ``a.jpg``; bare ``a`` becomes ``a.jpg``).
    - ``True``: the manifest ``filename`` names a DIFFERENT file that merely
      shares the stem — a sidecar whose extension is fixed and differs from the
      image's. Object detection annotations are always ``<stem>.xml`` (the
      documented ``{image_name}.xml`` convention), so strip a recognised
      trailing extension off ``filename`` and append ``extension``: both the
      extension-bearing ``a.jpg`` and the bare stem ``a`` resolve to ``a.xml``.
      Without this, an extension-bearing manifest value kept its ``.jpg`` and
      the annotation lookup searched ``annotations/a.jpg`` — which never exists
      — so every record was silently skipped even though the File Pairing
      validator (which pairs by on-disk stem) had passed. The manifest format
      is documented as working "with or without extension"; this keeps the
      annotation side of the transfer honouring that contract.
    """
    cfg = cfg or config
    if force_extension:
        # Strip only a RECOGNISED trailing extension (the same _has_extension
        # gate the image lookup uses). For the jpg/jpeg/png images objdet ships
        # (FileTypeValidator enforces that set) this equals the image file's
        # Path.stem — what File Pairing / Pascal VOC XML pair on — so validation
        # and transfer resolve the same annotation. This is deliberately NOT
        # Path.stem: a value with no recognised extension — a bare stem, or an
        # internal dot like ``image.001`` — is kept whole, so we never strip a
        # non-extension segment (Path.stem would wrongly turn ``image.001`` into
        # ``image``). The two only diverge for a non-recognised final suffix
        # (e.g. ``a.tar.gz``), which objdet images can't have.
        stem = filename.rsplit(".", 1)[0] if _has_extension(filename) else filename
        filename_with_ext = f"{stem}{extension}"
    else:
        filename_with_ext = (
            filename if _has_extension(filename) else f"{filename}{extension}"
        )
    # _safe_join raises on a traversal/absolute filename (#239); a genuinely
    # missing file still returns None below (skip), so a malicious manifest
    # value is rejected loudly while an absent sidecar is tolerated.
    candidate = _safe_join(cfg.SRC_PATH, subdirectory, filename_with_ext)
    if os.path.exists(candidate):
        return candidate, filename_with_ext
    return None, filename_with_ext


def image_transfer(
    record: Dict[str, Any],
    options: Dict[str, Any],
    src_path: str = None,
    filename_with_ext: str = None,
    cfg: Optional[Config] = None,
) -> Optional[Dict[str, Any]]:
    """Copy an image from SRC_PATH/images/ to DEST_PATH/.

    Callers that have already resolved the source via `_find_src` (e.g.
    atomic multi-file branches that need to pre-check existence) can
    pass `src_path` and `filename_with_ext` to skip the lookup; in that
    mode no filesystem stat happens here.

    Returns ``None`` if the source file cannot be located or the record
    is missing a filename. The caller in ``BaseIngestor.ingest`` treats
    ``None`` as a file-transfer skip — see issue #99 (silent data-loss
    pattern: returning the record on a missing source let the DB/API
    write succeed and falsely report 100% success).
    """
    cfg = cfg or config
    # Create destination directory if it doesn't exist
    _ensure_dest_dir(cfg.DEST_PATH)

    try:
        # Get the filename from the record
        filename = record.get("filename")
        extension = options.get("extension")
        if not filename:
            logger.error(f"{RED}No filename found in record{RESET}")
            return None

        if src_path is None:
            src_path, filename_with_ext = _find_src(
                "images", filename, extension, cfg=cfg
            )
            if src_path is None:
                logger.error(
                    f"{RED}Source image not found: {os.path.join(cfg.SRC_PATH, 'images', filename_with_ext)}{RESET}"
                )
                return None

        # Save the resized image (write target guarded against escaping DEST — #239)
        image_dest_path = _safe_join(cfg.DEST_PATH, filename_with_ext)
        # Copy file with retry logic
        _copy_file_with_retry(src_path, image_dest_path)

        record["filename"] = os.path.splitext(filename_with_ext)[0]
        record["extension"] = extension

        logger.info(f"{GREEN}Successfully copied image: {filename}{RESET}")
        return record

    except Exception as e:
        raise ValueError(f"{RED}Error processing binary image: {str(e)}{RESET}")


"""
Row: id, data_id, filename, extension, label, intent, ingestor_id
filename: file_name.png (or any other extension) file_name.xml

"""


def annotation_transfer(
    record: Dict[str, Any],
    options: Dict[str, Any],
    extension: str,
    src_path: str = None,
    filename_with_ext: str = None,
    cfg: Optional[Config] = None,
) -> Optional[Dict[str, Any]]:
    """Copy an annotation file from SRC_PATH/annotations/ to DEST_PATH/.

    Callers that have already resolved the source via `_find_src` can
    pass `src_path` and `filename_with_ext` to skip the lookup; in that
    mode no filesystem stat happens here.

    Returns ``None`` on missing source (see issue #99).
    """
    cfg = cfg or config
    # Create destination directory if it doesn't exist
    _ensure_dest_dir(cfg.DEST_PATH)

    try:
        # Get the filename from the record
        filename = record.get("filename")
        if not filename:
            logger.error(f"{RED}No filename found in record{RESET}")
            return None

        if src_path is None:
            # force_extension: the annotation is <stem>.xml, derived from the
            # image filename's stem — never the image's own extension. See
            # _find_src for why an extension-bearing manifest value would
            # otherwise miss the annotation (object_detection skip bug).
            src_path, filename_with_ext = _find_src(
                "annotations", filename, extension, cfg=cfg, force_extension=True
            )
            if src_path is None:
                logger.error(
                    f"{RED}Source file not found: {os.path.join(cfg.SRC_PATH, 'annotations', filename_with_ext)}{RESET}"
                )
                return None

        # Save the file (write target guarded against escaping DEST — #239)
        file_dest_path = _safe_join(cfg.DEST_PATH, filename_with_ext)
        # Copy file with retry logic
        _copy_file_with_retry(src_path, file_dest_path)

        logger.info(f"{GREEN}Successfully copied file: {filename}{RESET}")
        return record

    except Exception as e:
        raise ValueError(f"{RED}Error processing binary file: {str(e)}{RESET}")


def text_transfer(
    record: Dict[str, Any],
    options: Dict[str, Any],
    src_subdir: str = "texts",
    cfg: Optional[Config] = None,
) -> Optional[Dict[str, Any]]:
    """Transfer text files for text-based tasks.

    Args:
        record: Dictionary containing filename and other record data
        options: Dictionary containing transfer options like extension
        src_subdir: Subdirectory under SRC_PATH where source files live
                    (``"texts"`` for text classification,
                     ``"sequences"`` for masked language modeling)

    Returns:
        Updated record dictionary, or ``None`` on missing source (see issue #99).
    """
    cfg = cfg or config
    # Create destination directory if it doesn't exist
    _ensure_dest_dir(cfg.DEST_PATH)

    try:
        # Get the filename from the record
        filename = record.get("filename")
        extension = options.get("extension")
        if not filename:
            logger.error(f"{RED}No filename found in record{RESET}")
            return None

        # Add extension to filename if it doesn't have one
        if not _has_extension(filename):
            filename_with_ext = f"{filename}{extension}"
        else:
            filename_with_ext = filename

        # Process the text file (both ends guarded against escaping the
        # SRC/DEST sandboxes via a crafted filename — #239)
        text_src_path = _safe_join(cfg.SRC_PATH, src_subdir, filename_with_ext)
        if not os.path.exists(text_src_path):
            logger.error(f"{RED}Source text file not found: {text_src_path}{RESET}")
            return None

        # Save the text file
        text_dest_path = _safe_join(cfg.DEST_PATH, filename_with_ext)
        # Copy file with retry logic
        _copy_file_with_retry(text_src_path, text_dest_path)

        record["filename"] = os.path.splitext(filename_with_ext)[0]
        record["extension"] = extension

        logger.info(f"{GREEN}Successfully copied text file: {filename}{RESET}")
        return record

    except Exception as e:
        raise ValueError(f"{RED}Error processing text file: {str(e)}{RESET}")


def _find_mask_src(mask_id: str, cfg: Optional[Config] = None):
    """Locate a mask file in SRC_PATH/masks/, trying common image extensions.

    Returns (src_path, extension, mask_name) on success, or (None, None, mask_name)
    if no matching file is found. ``cfg`` (P4c) is the run's resolved Config;
    ``None`` falls back to the module-global ``config``.
    """
    cfg = cfg or config
    mask_name = mask_id.split(".")[0] if "." in mask_id else mask_id
    for ext in [".png", ".jpg", ".jpeg"]:
        # _safe_join raises on a traversal/absolute mask_id (#239).
        candidate = _safe_join(cfg.SRC_PATH, "masks", f"{mask_name}{ext}")
        if os.path.exists(candidate):
            return candidate, ext, mask_name
    return None, None, mask_name


def mask_transfer(
    record: Dict[str, Any],
    mask_src_path: str,
    mask_ext: str,
    mask_name: str,
    cfg: Optional[Config] = None,
) -> Dict[str, Any]:
    """Copy a pre-resolved mask file from SRC_PATH/masks/ to DEST_PATH/.

    The caller is responsible for locating the mask via `_find_mask_src` and
    passing the resolved path; this keeps the filesystem lookup to a single
    call per record.
    """
    cfg = cfg or config
    _ensure_dest_dir(cfg.DEST_PATH)

    try:
        # Write target guarded against escaping DEST via a crafted mask_id (#239)
        mask_dest_path = _safe_join(cfg.DEST_PATH, f"{mask_name}{mask_ext}")
        _copy_file_with_retry(mask_src_path, mask_dest_path)

        logger.info(f"{GREEN}Successfully copied mask: {mask_name}{RESET}")
        return record

    except Exception as e:
        raise ValueError(f"{RED}Error processing mask file: {str(e)}{RESET}")


# Per-row sidecar pointers that live on the RAW source record (not table
# columns, so never on the cleaned DB-bound record). map_file_transfer lends
# them to the transfer for the copy, then strips them (#212, P5). Currently
# just ``mask_id`` (semantic_segmentation); add future runtime-only pointers
# here.
_SIDECAR_KEYS = ("mask_id",)


def map_file_transfer(
    task_category: TaskCategory,
    record: Dict[str, Any],
    options: Dict[str, Any],
    cfg: Optional[Config] = None,
    source_record: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Stage a record's per-row sidecar file(s), dispatched by category.

    Thin lookup over the ModalityRegistry: each file-bearing category's spec
    carries a ``transfer`` factory (the per-category bodies — atomic
    image+annotation / image+mask pre-checks and all — now live in
    ``modalities/transfer.py``; structural refactor backend#796, P3c). A
    non-file-bearing or unknown category has ``transfer is None`` and returns
    None (no sidecars to stage), exactly as the prior ``else`` branch did.

    The registry import is lazy to avoid an import cycle at module load
    (registry -> modalities.transfer -> this module).

    Args:
        task_category: the task category.
        record: the cleaned, DB-bound record (filename / data_id / …); the
            transfer mutates its filename/extension to the staged values.
        options: transfer options (extension, …).
        cfg: the run's resolved Config, threaded to the copy primitives so
            they read SRC_PATH / DEST_PATH from it instead of a module-global
            Config() that reads os.environ (P4c). None falls back to that
            global for direct callers / tests.
        source_record: the RAW source record (pre-cleaning). Used to LEND a
            per-row sidecar pointer (``mask_id``) to the transfer when it is NOT
            a declared schema column — so the copy can locate the mask file even
            though the cleaned ``record`` doesn't carry it; the lent value is
            stripped before return (not stored). When ``mask_id`` IS a declared
            column it's already on ``record`` (kept by RecordProcessor and
            stored for the client — backend#816), so no lend happens. None: the
            transfer reads any sidecar pointer off ``record`` itself.

    Returns:
        The (possibly mutated) record, or None if a required sidecar is missing.
    """
    from .modalities.registry import REGISTRY

    spec = REGISTRY.get(task_category)
    if spec is None or spec.transfer is None:
        return None

    # Lend the raw record's sidecar pointers to the transfer ONLY when they're
    # not already on the cleaned record (`key not in record` = not a declared
    # schema column), then strip exactly what we lent in `finally` so a
    # transfer-only pointer can't reach the DB insert — even if the transfer
    # raises or drops the record. A DECLARED mask_id column stays on `record`
    # and is stored (the client reads it to locate masks; backend#816).
    lent = []
    if source_record is not None:
        for key in _SIDECAR_KEYS:
            if key in source_record and key not in record:
                record[key] = source_record[key]
                lent.append(key)
    try:
        return spec.transfer(record, options, cfg)
    finally:
        for key in lent:
            record.pop(key, None)


# The isolated, tracebloc-created staging subtree the CLI stage pod writes to:
# SharedRoot/.tracebloc-staging/<table>/ (tracebloc/cli internal/push/teardown.go
# + data_ingest_cluster.go). reclaim_source deletes ONLY that per-table dir — a
# hidden, tracebloc-owned throwaway copy, never a customer's own data.
#
# CROSS-REPO CONTRACT: this name — and the per-table layout the gate below
# expects (SRC_PATH == SharedRoot/.tracebloc-staging/<TABLE_NAME>, which the CLI
# realises by staging images: under .../<table>/images/) — is shared with the
# CLI and nothing here pins it. If the CLI's staging layout changes, this must
# change in lockstep or reclaim silently no-ops and the #346 ~2x-disk leak
# returns on a green ingest. Mitigation until a shared constant / cross-repo
# integration check exists (PR #381 review): the gate WARNs when a source lands
# under this tree but not at the expected per-table path, so drift is loud.
STAGING_DIRNAME = ".tracebloc-staging"


def _is_within(child: str, parent: str) -> bool:
    """True if ``child`` is ``parent`` or a path inside it.

    Compares fully resolved (symlinks followed) absolute paths COMPONENT-WISE
    via ``os.path.commonpath`` — never a raw ``startswith`` string prefix. That
    dodges two traps a string compare falls into: a ``parent`` of ``/`` becoming
    a ``//`` prefix that matches nothing, and ``/data/shared-2`` looking
    "within" ``/data/shared``.
    """
    child = os.path.realpath(child)
    parent = os.path.realpath(parent)
    try:
        return os.path.commonpath([child, parent]) == parent
    except ValueError:
        # Mixed absolute/relative, or (on Windows) different drives — not within.
        return False


def reclaim_source(cfg: Optional[Config] = None) -> bool:
    """Delete the staged ``SRC_PATH`` tree after a VERIFIED, clean load (#346).

    Every ``*_transfer`` above **copies** a file-bearing dataset's staged
    sidecars from ``SRC_PATH/<subdir>/`` into the final table dir ``DEST_PATH``
    (see ``_copy_file_with_retry``) but historically never removed the staging
    copy — so each ingest left two copies of the data on the shared PVC (~2x
    disk for image / detection / segmentation datasets). This reclaims the
    staging tree, replacing the CLI's extra ``rm -rf`` pod (tracebloc/cli#167).

    The caller MUST invoke this only once the load is fully durable (DB rows
    committed + dataset registered with the backend) AND clean (no failed
    records). On a partial/failed load the source is deliberately kept so the
    operator can retry or inspect it — mirroring the compensating-delete branch
    in ``BaseIngestor._ingest_with_lock``, which leaves staged files in place.

    SAFETY — reclaim is OPT-IN, not blocklist. We delete a directory only when
    we can PROVE it is a throwaway copy tracebloc itself staged FOR THIS TABLE:
    the resolved ``SRC_PATH`` must equal EXACTLY
    ``STORAGE_PATH/.tracebloc-staging/<TABLE_NAME>`` — the per-table dir the CLI
    stage pod writes. Binding to ``cfg.TABLE_NAME`` (not merely "somewhere under
    ``.tracebloc-staging``") means a stray or symlinked ``SRC_PATH`` cannot reach
    a SIBLING dataset's staging. Every other layout is left untouched — no worse
    than the pre-#346 status quo — because it is not provably ours to delete:

      - unset / empty ``SRC_PATH`` or ``TABLE_NAME`` (tabular etc. stage nothing);
      - ``SRC_PATH`` is not an existing directory — nothing to reclaim;
      - a customer's OWN mounted dataset dir in the helm layout, where
        ``_resolve_config`` derives ``SRC_PATH`` as the parent of ``images:``
        (e.g. ``/data/shared/cats-dogs``, a SIBLING of the table dir) — deleting
        that would destroy the user's data;
      - another table's ``.tracebloc-staging/<other>`` dir, the shared-PVC root,
        ``/``, or anything that resolves outside this table's staging dir.

    Paths are resolved with ``os.path.realpath`` before every check AND before
    the delete, so a symlinked ``SRC_PATH`` (PVC mounts are symlinks) cannot
    slip a non-staging target past the gate, and ``rmtree`` acts on the real
    directory. A final backstop still refuses if the resolved source overlaps
    the table dir at all.

    Best-effort: ANY error — deriving the guards or the delete itself — is
    logged and swallowed (returns False). The load has already succeeded when
    this is called, so a leftover staging copy (the pre-#346 status quo) must
    never turn a green ingest red.

    Returns ``True`` iff the staging tree was removed.
    """
    cfg = cfg or config
    # Best-effort wrapper: reclaim runs AFTER the load is durable + registered
    # (see _ingest_with_lock), so NOTHING here — realpath, isdir, the guards,
    # rmtree, even a logging failure — may turn a green ingest red (Bugbot #381).
    # Any error is swallowed and the leftover staging copy (the pre-#346 status
    # quo) is left for manual cleanup.
    try:
        return _reclaim_source(cfg)
    except Exception as e:
        try:
            logger.warning(
                f"{YELLOW}Could not reclaim staged source (SRC_PATH="
                f"{cfg.SRC_PATH!r}): {e}. The load succeeded; the leftover "
                f"staging copy is the pre-#346 status quo and can be removed "
                f"manually.{RESET}"
            )
        except Exception:
            pass  # a broken logger must not fail a green ingest either
        return False


def _reclaim_source(cfg: Config) -> bool:
    """The guarded reclaim decision + delete (contract: see ``reclaim_source``).

    Split out so ``reclaim_source`` can wrap the WHOLE thing in one best-effort
    guard — a raise anywhere here (a bad ``realpath`` / ``isdir``, an overlap
    check, a logging failure, or ``rmtree``) must never propagate to the ingest.
    """
    src = cfg.SRC_PATH
    if not src:
        return False

    # Resolve symlinks up front: every guard below — and the delete itself —
    # must act on the REAL target, so a symlinked SRC_PATH can't bypass them.
    src_real = os.path.realpath(src)
    dest_real = os.path.realpath(cfg.DEST_PATH)
    storage_real = os.path.realpath(cfg.STORAGE_PATH)

    if not os.path.isdir(src_real):
        return False

    # Opt-in gate: reclaim ONLY THIS table's own staging dir — exactly
    # STORAGE_PATH/.tracebloc-staging/<TABLE_NAME>, what the CLI stage pod writes.
    # Binding to cfg.TABLE_NAME (not merely "somewhere under .tracebloc-staging")
    # stops a stray SRC_PATH — another dataset's staging, or a <table> symlink
    # that realpaths into the tree — from rmtree'ing a sibling's data on the
    # shared PVC (Bugbot #381). `expected` is the literal per-table path (NOT
    # realpath'd), so a `<table>` that is itself a symlink elsewhere fails the
    # equality against the fully-resolved src and is left alone. A blank table
    # name, or one that escapes the staging subtree, skips.
    staging_root = os.path.normpath(os.path.join(storage_real, STAGING_DIRNAME))
    table = (cfg.TABLE_NAME or "").strip()
    expected = os.path.normpath(os.path.join(staging_root, table))
    if not (
        table
        and src_real == expected
        and expected != staging_root
        and _is_within(expected, staging_root)
    ):
        # Didn't resolve to THIS table's staging dir — skip. Two very different
        # reasons, logged at very different volumes:
        #   - src IS under .tracebloc-staging but not this table's dir: a
        #     tracebloc-staged copy we could NOT bind to this table — a CLI
        #     staging-layout drift (the contract is cross-repo + unpinned, see
        #     STAGING_DIRNAME) or a cross-table SRC_PATH. Silently no-oping here
        #     would let the #346 ~2x-disk leak return on a GREEN ingest, so WARN.
        #   - src is OUTSIDE .tracebloc-staging (a helm user-data dir, the
        #     storage root, a symlink resolving away): we deliberately never
        #     touch it — info is enough.
        if _is_within(src_real, staging_root):
            logger.warning(
                f"{YELLOW}Staged source {src_real} is under {staging_root} but is "
                f"not this table's expected dir {expected} — NOT reclaiming, so "
                f"the ~2x-disk staging copy (#346) may persist even though the "
                f"ingest succeeded. If this is the CLI-staged path, its staging "
                f"layout (SharedRoot/{STAGING_DIRNAME}/<TABLE_NAME>/) has drifted "
                f"from what reclaim expects; otherwise SRC_PATH is pointed at "
                f"another table.{RESET}"
            )
        else:
            logger.info(
                f"{YELLOW}Not reclaiming staged source {src_real}: it is not under "
                f"this table's {STAGING_DIRNAME}/{table or '<table>'} staging dir "
                f"(e.g. a user-mounted dataset dir in the helm layout, or the "
                f"storage root) — left in place (#346).{RESET}"
            )
        return False

    # Backstop: never delete the freshly-written table dir or anything that
    # overlaps it, even if the staging root were somehow mis-derived.
    if _is_within(src_real, dest_real) or _is_within(dest_real, src_real):
        logger.warning(
            f"{YELLOW}Not reclaiming staged source {src_real}: it overlaps the "
            f"table dir {dest_real}; refusing to risk the freshly-written table "
            f"(#346).{RESET}"
        )
        return False

    shutil.rmtree(src_real)
    logger.info(
        f"{GREEN}Reclaimed staged source {src_real} after a verified, clean "
        f"load — no duplicate copy left on the PVC (#346).{RESET}"
    )
    return True


# A per-ingestion physical handle: ``ds_`` + uuid4 hex (35 chars). Same grammar
# the ingestor derives (BaseIngestor.physical_table_name) and the client-runtime
# husk sweep matches — used here to PROVE a dest dir is a throwaway per-ingestion
# handle before removing it, never a user label dir.
_DS_HANDLE_RE = re.compile(r"^ds_[0-9a-f]{32}$")


def reclaim_dest_tree(cfg: Optional[Config] = None) -> bool:
    """Best-effort removal of THIS run's per-ingestion DEST_PATH tree after a
    FAILED file-bearing ingest (#437 follow-up — data-ingestors#439).

    Under PER_INGESTION_TABLES the file tree is keyed on the run's unique
    ``ds_<hex>`` handle (``config.set_dest_table``), so — unlike the flag-off
    label tree, which a same-label re-run overwrites — a failed run's assets are
    NEVER overwritten (the retry mints a fresh handle) and nothing else reclaims
    them: a lasting PVC leak for image-sized payloads. The graceful-failure path
    (``BaseIngestor._ingest_with_lock``'s compensating-delete branch) calls this
    to remove them. Hard kills (OOMKilled/SIGKILL) bypass that branch; those
    orphans are the edge-side husk sweep's job, exactly like the DB husk tables.

    SAFETY — opt-in, provable: delete DEST_PATH only when its basename is a
    ``ds_<hex>`` handle (``_DS_HANDLE_RE`` — the per-ingestion pattern, never a
    user label) AND it resolves to a DIRECT child of STORAGE_PATH. A flag-off
    label dir, a user's mounted dataset dir, or the storage root is always left
    untouched. Paths are ``realpath``-resolved before the check and the delete,
    so a symlinked PVC mount cannot slip a non-handle target past the gate.

    Best-effort: ANY error is logged + swallowed (returns False) so a cleanup
    failure can never mask the ingest's original error.

    Returns ``True`` iff a per-ingestion dest tree was removed.
    """
    cfg = cfg or config
    try:
        return _reclaim_dest_tree(cfg)
    except Exception as e:
        try:
            logger.warning(
                f"{YELLOW}Could not reclaim the failed run's dest tree "
                f"(DEST_PATH={cfg.DEST_PATH!r}): {e}. Left in place — a manual "
                f"cleanup or the husk sweep can remove it.{RESET}"
            )
        except Exception:
            pass  # a broken logger must not fail the failure path either
        return False


def _reclaim_dest_tree(cfg: Config) -> bool:
    """The guarded dest-tree reclaim decision + delete (contract: see
    ``reclaim_dest_tree``). Split out so the wrapper can best-effort the WHOLE
    thing — a raise anywhere here must never propagate to the ingest."""
    dest = cfg.DEST_PATH
    storage = cfg.STORAGE_PATH
    if not dest or not storage or not os.path.isdir(dest):
        return False
    dest_real = os.path.realpath(dest)
    storage_real = os.path.realpath(storage)
    # Must be a DIRECT ds_<hex> child of STORAGE_PATH — never a flag-off label
    # dir, a user dataset dir, the storage root, or anything outside it.
    if os.path.dirname(dest_real) != storage_real:
        logger.info(
            f"{YELLOW}Not reclaiming dest {dest_real}: not a direct child of "
            f"STORAGE_PATH {storage_real} — left in place.{RESET}"
        )
        return False
    if not _DS_HANDLE_RE.match(os.path.basename(dest_real)):
        # flag-off label tree (overwritten on re-run) or any non-handle dir
        return False
    shutil.rmtree(dest_real)
    logger.info(
        f"{GREEN}Reclaimed the failed per-ingestion run's file tree {dest_real} "
        f"— no orphaned payload left on the PVC (#437 follow-up).{RESET}"
    )
    return True
