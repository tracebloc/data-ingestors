"""Per-run accounting of the bytes an ingest stages into the dataset tree.

backend#3644: the ingest summary reports the dataset's physical ``size_bytes``
so the platform can show a real size instead of an em dash. Nothing on the
platform has ever measured it, so there is nothing to read it off afterwards —
and the destination tree cannot be walked at the end of the run to find out. In
the default (shared-table) mode ``DEST_PATH`` is ``STORAGE_PATH/<table>``, a
tree that PERSISTS ACROSS RUNS of the same table, so a walk would attribute an
earlier ingest's files to this dataset. The size is therefore accumulated AS
the files are staged, at the single copy funnel
(``file_transfer._copy_file_with_retry``) every category's transfer goes
through.

Two properties this module exists to hold, both of them correctness rather than
taste:

* **Keyed by destination path, never a running sum.** A manifest may name the
  same file twice — that is a WARNING, not an error (``duplicate_validator``) —
  and the second copy OVERWRITES the first. A running sum would count those
  bytes twice and publish a dataset larger than the one on disk; re-assigning
  the key records the one file that is actually there. The cost is one dict
  entry per staged file, single-digit MB at the ~100k-file datasets ingested
  today — the same bound ``database.get_label_counts`` already accepts.

* **An unmeasurable file makes the whole total unreportable.** If the stat of a
  just-copied file fails, the running total is short by an unknown amount, and
  a short integer is indistinguishable from a measured one once it reaches the
  backend. So the meter latches INCOMPLETE and reports ``None``, which the
  caller turns into an omitted field. ``NULL`` means "not reported"; any
  integer — including ``0`` — is a positive claim about the dataset's size.

The active meter travels in a :class:`~contextvars.ContextVar` rather than as
an argument, so none of the eleven per-category transfer factories or the four
copy primitives change signature to carry it. A ContextVar (not a module
global) also keeps two ingests running in one process — the e2e
characterization harness does exactly that — from accumulating into each
other's total.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Dict, Iterator, Optional

logger = logging.getLogger(__name__)

__all__ = ["StagedBytes", "measure_staged_bytes", "record_staged_file"]


class StagedBytes:
    """The bytes of the data files one ingest run staged into the dataset tree.

    Not thread-safe, and deliberately so: one run stages its files from a
    single loop. Concurrent runs each get their own meter from
    :func:`measure_staged_bytes`.
    """

    def __init__(self) -> None:
        self._sizes: Dict[str, int] = {}
        self._complete = True

    def record(self, dest_path: str) -> None:
        """Record the on-disk size of the file just written to ``dest_path``.

        Re-recording a path REPLACES its size rather than adding to it (see the
        module docstring: an overwriting duplicate is one file on disk). A
        failed stat latches the meter incomplete instead of raising — a copy
        that succeeded must not fail the ingest over the bookkeeping, and must
        not silently under-report either.
        """
        try:
            self._sizes[dest_path] = os.path.getsize(dest_path)
        except OSError as exc:
            self._complete = False
            logger.warning(
                f"Could not measure the staged file {dest_path!r} ({exc}); the "
                f"dataset's size_bytes will not be reported for this run "
                f"(backend#3644). The ingest itself is unaffected."
            )

    @property
    def total(self) -> Optional[int]:
        """Total staged bytes, or ``None`` when the measurement is incomplete.

        ``None`` is the signal to OMIT ``size_bytes`` from the ingest summary.
        """
        if not self._complete:
            return None
        return sum(self._sizes.values())

    @property
    def file_count(self) -> int:
        """Number of distinct destination paths measured (diagnostics/tests)."""
        return len(self._sizes)


_ACTIVE_METER: ContextVar[Optional[StagedBytes]] = ContextVar(
    "tracebloc_staged_bytes", default=None
)


@contextmanager
def measure_staged_bytes() -> Iterator[StagedBytes]:
    """Measure every file staged inside the block, yielding the live meter.

    Outside such a block :func:`record_staged_file` is a no-op, so a direct
    caller of the transfer primitives (a template script, a test) behaves
    exactly as it did before this module existed.
    """
    meter = StagedBytes()
    token = _ACTIVE_METER.set(meter)
    try:
        yield meter
    finally:
        _ACTIVE_METER.reset(token)


def record_staged_file(dest_path: str) -> None:
    """Report a staged file to the active meter, if any."""
    meter = _ACTIVE_METER.get()
    if meter is not None:
        meter.record(dest_path)
