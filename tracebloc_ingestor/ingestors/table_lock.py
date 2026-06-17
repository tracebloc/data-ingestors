"""Cross-ingest table lock (structural refactor — backend#796, P5b).

A file-based mutex that serialises ingests targeting the same table (#772 P2):
two runs against one table used to race ``create_table`` / interleave upserts.
Extracted verbatim from ``BaseIngestor`` — the lock lifecycle (compute path,
atomic O_EXCL acquire with stale-reclaim, release) is a cohesive, stateful
responsibility, so it lives as its own class that ``BaseIngestor`` composes.

The lock file lives at the top of ``STORAGE_PATH`` (the parent of every
per-table DEST_PATH) so it's durable across pod restarts on the cluster PVC.
``STORAGE_PATH`` is a class constant on :class:`Config` (not env/override
driven), so it's read from a plain ``Config()`` — every instance sees the same
value. Behaviour (and every error message) is unchanged from the former
``BaseIngestor._acquire_table_lock`` / ``_release_table_lock``.
"""

import logging
import os
from typing import Any, Dict, Optional

from ..utils.constants import RED, RESET, YELLOW

logger = logging.getLogger(__name__)


class TableLock:
    """Exclusive, file-based lock for one ``table_name`` ingest.

    Stateless across calls: ``acquire`` returns the lock path and ``release``
    takes it back, so the only durable state is the on-disk lock file itself.
    """

    # Stale-lock cutoff (seconds). A crashed ingest leaves the lock file
    # behind; if the lock is older than this we log + remove + reacquire so a
    # customer isn't blocked indefinitely waiting for the writer whose pod has
    # long since been garbage-collected. 12h covers any reasonable ingest
    # (including multi-GB proteomics).
    STALE_SECONDS = 12 * 3600

    def __init__(self, table_name: str, ingestor_id: str):
        self.table_name = table_name
        self.ingestor_id = ingestor_id

    def path(self) -> Optional[str]:
        """Where the lock file lives — at the top of STORAGE_PATH (the parent
        of every per-table DEST_PATH), so it's durable across pod restarts on
        the cluster PVC. Returns None when STORAGE_PATH is unset or not a
        directory (test configs / local runs without a staging dir) — caller
        treats that as "no lock available, skip".
        """
        # Late import: keep this module free of a Config singleton at import
        # time so unit tests can monkeypatch the env per test.
        from ..config import Config

        storage = Config().STORAGE_PATH
        if not storage or not os.path.isdir(storage):
            return None
        return os.path.join(storage, f".tracebloc-ingest-{self.table_name}.lock")

    def acquire(self) -> Optional[str]:
        """Acquire an exclusive lock for ``self.table_name`` (#772 P2).

        Two ingests targeting the same table used to race ``create_table`` /
        interleave upserts. Atomic ``O_EXCL`` create either succeeds (lock
        acquired) or fails with ``FileExistsError`` (another ingest is in
        flight). On conflict, read the existing lock's metadata and surface it
        in the error so ops can find the other run; if the lock is older than
        the stale-cutoff, remove and reacquire.

        Returns the lock path (or None if no STORAGE_PATH is configured) so
        ``release`` can remove the right file.
        """
        import json as _json
        import socket as _socket
        from datetime import datetime as _datetime

        lock_path = self.path()
        if lock_path is None:
            return None

        lock_info = {
            "ingestor_id": self.ingestor_id,
            "table_name": self.table_name,
            "pid": os.getpid(),
            "hostname": _socket.gethostname(),
            "started_at": _datetime.utcnow().isoformat() + "Z",
        }
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            # Read the existing lock so the error names the holder. Also
            # check staleness and self-recover if so.
            existing_info: Dict[str, Any] = {}
            try:
                with open(lock_path, "r") as f:
                    existing_info = _json.load(f)
            except Exception:
                pass
            age = None
            try:
                started = _datetime.fromisoformat(
                    existing_info.get("started_at", "").rstrip("Z")
                )
                age = (_datetime.utcnow() - started).total_seconds()
            except Exception:
                # Lock metadata is corrupt (truncated file, malformed JSON,
                # missing/un-parseable started_at). Fall back to the file's
                # mtime as the age signal (#221 bugbot: a corrupt lock used to
                # never auto-expire because age stayed None). Use time.time()
                # rather than _datetime.utcnow().timestamp() — the latter is
                # timezone-broken (naive datetime treated as local) so the
                # cutoff would shift by the local UTC offset on non-UTC systems.
                import time as _time

                try:
                    mtime = os.path.getmtime(lock_path)
                    age = _time.time() - mtime
                except OSError:
                    pass
            if age is not None and age > self.STALE_SECONDS:
                logger.warning(
                    f"{YELLOW}Stale lock at {lock_path} (age={age:.0f}s, "
                    f"holder={existing_info!r}) — removing and reacquiring. "
                    f"The holder's pod likely crashed before its finally "
                    f"could run.{RESET}"
                )
                try:
                    os.remove(lock_path)
                except FileNotFoundError:
                    pass
                return self.acquire()
            raise RuntimeError(
                f"{RED}Another ingest is already running for table "
                f"'{self.table_name}' (lock at {lock_path}). "
                f"Holder: {existing_info!r}. Wait for it to finish, or — "
                f"if its pod crashed — remove the lock file manually. "
                f"(The lock auto-clears after "
                f"{self.STALE_SECONDS}s.){RESET}"
            )
        try:
            with os.fdopen(fd, "w") as f:
                _json.dump(lock_info, f)
        except Exception:
            # Couldn't write the metadata — drop the lock so we don't block
            # ourselves on a malformed file.
            try:
                os.remove(lock_path)
            except FileNotFoundError:
                pass
            raise
        logger.info(f"Acquired table lock for '{self.table_name}' at {lock_path}")
        return lock_path

    def release(self, lock_path: Optional[str]) -> None:
        """Remove the lock file. No-op when ``acquire`` returned None (no
        STORAGE_PATH configured). Idempotent — a double-release (e.g. exception
        path + finally path both call it) silently swallows
        ``FileNotFoundError``.
        """
        if not lock_path:
            return
        try:
            os.remove(lock_path)
            logger.info(f"Released table lock for '{self.table_name}'")
        except FileNotFoundError:
            pass
        except Exception as exc:
            logger.warning(
                f"Failed to remove table lock {lock_path}: {exc}. "
                f"It will auto-clear after "
                f"{self.STALE_SECONDS}s."
            )
