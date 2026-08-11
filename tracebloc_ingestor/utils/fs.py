"""Filesystem helpers shared by the validators and the transfer path.

Home of the one rule about destination directories that both sides must follow:
a directory the ingestor creates has to stay reclaimable by the `data delete`
teardown, which runs as a DIFFERENT uid in a different pod.
"""

import logging
import os

logger = logging.getLogger(__name__)

# Mode for a destination directory the ingestor creates.
#
# 0o2777 = rwxrwsrwx: setgid, plus write for group AND other.
#
# Why world-write and not the tighter group-write (0o2775) this used to be:
# removing a file requires write+execute on its PARENT DIRECTORY, and the
# processes that create and delete these trees are different pods running as
# different, non-overlapping identities:
#
#   * the ingestion Job runs as uid 65534 (`nobody`) by default, or HOST_UID on
#     a network export (client-runtime submit_ingestion_run.py);
#   * the CLI's staging/teardown pod runs as uid 65532 with fsGroup 65532
#     (cli internal/push/pod.go);
#   * the chart chowns the shared mount itself to 1000:1000 and sets setgid, so
#     a directory created underneath INHERITS group 1000 -- not 65532.
#
# So group-write could never help: the group the teardown pod carries (65532) is
# not the group the directory inherits (1000). And the usual fix for that --
# fsGroup, which makes kubelet recursively chgrp the volume -- does nothing here,
# because kubelet IGNORES fsGroup on hostPath volumes
# (kubernetes/kubernetes#138411), the layout every installer-provisioned cluster
# uses. Nothing at ingest time can know or align those identities, and the parent
# mount is already world-writable by chart design, so world-write on the
# directory is both the only mode that works for every caller and no wider a
# grant than the tree it sits in.
DEST_DIR_MODE = 0o2777


def ensure_reclaimable_dir(path: str) -> None:
    """Create ``path`` if absent, with a mode the teardown can reclaim.

    Only a directory we *create* is chmod'd. A pre-existing one is left alone so
    we neither override an operator's deliberate mode nor mask a genuine
    permission problem -- an unwritable destination still surfaces downstream.
    (A directory left behind by an older ingestor keeps its old mode; repairing
    those is the installer's job, not a silent chmod of data we did not create.)

    The chmod is best-effort: a failure is logged, never fatal, so ingestion
    still proceeds on a filesystem that cannot represent the mode at all.
    """
    existed = os.path.isdir(path)
    os.makedirs(path, exist_ok=True)
    if existed:
        return
    try:
        os.chmod(path, DEST_DIR_MODE)
    except OSError as error:
        logger.warning(
            "Could not set the reclaimable mode on %s (%s); the `data delete` "
            "teardown may not be able to remove it",
            path,
            error,
        )
