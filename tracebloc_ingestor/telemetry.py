"""Contract telemetry for the ingest Job (RFC-BACKEND-1872 D2).

The ingest Job emitted nothing. Every other Python service in the platform
adopted the shared contract emitter; this one was never wired up, so an ingest
that failed produced free-text log lines and no countable, groupable event.

    configure_job()          # once, at the entry point
    begin_run(correlation_id)
    mark_durable()           # once the work is committed; see below
    ... exactly one of job_succeeded() / job_rejected() / job_failed()
                     / job_cancelled() ...

WHY ``mark_durable`` EXISTS, since a flag in the middle of that list looks like
one thing too many. Every ENDING is classified the moment it becomes true except
success, which the entrypoint's funnel reports only after the run has unwound.
That asymmetry had a cost: ``_terminal`` is first-writer-wins, so a SIGTERM
arriving during the post-success teardown -- reclaiming the staged source tree,
releasing the table lock -- claimed the terminal slot as ``cancelled`` for a
dataset that was already committed and registered, and the success went uncounted
(backend#2435). ``mark_durable`` closes that window by recording the FACT the
handler was missing, so ``job_cancelled`` can tell "the platform stopped the
work" from "the platform stopped a process that had already finished the work".

It records the fact rather than emitting ``succeeded`` early on purpose. The
terminal slot stays OPEN, so anything that does go wrong after the commit still
reports its own failure rather than being dropped as a duplicate of a success
already claimed -- a broken run that reports success is the expensive direction
of this bug, and it is the one not worth trading for.

WHERE THESE RECORDS ACTUALLY GO, stated plainly rather than implied. This
process runs as a Job inside the CUSTOMER's cluster. Its stdout is collected by
the node-level collector, and that collector takes control-plane containers
only: ingestion is a lower-collection class and stays out until content
redaction ships. So for now these records land in the customer's own cluster
logs and reach nothing of ours. That is a real limitation and not a defect of
this module: the records exist, are contract-shaped, and become visible the
moment collection is widened. Emitting them now is what makes that widening a
configuration change rather than a code change.

WHY THE EXCEPTION MESSAGE IS NEVER EMITTED. This package's standing policy
(``utils/redaction.py``) is that cell VALUES never appear in errors or logs;
column names, file names, counts, dtypes and row indices may. Every exception
raised on the ingestion path has the customer's own cells in scope -- a MySQL
type rejection quotes the offending value, a cast error embeds it -- so
``str(exc)`` is a content leak by default, which is why the package already logs
``type(exc).__name__`` and suppresses the message.

The contract requires a failure record to carry its ``exception.type`` and its
``exception.stacktrace`` once an exception was caught, and it explicitly exempts
``exception.message`` from that set. ``safe_stacktrace`` below is what makes
those two safe to send: it renders the FRAMES only. A frame is file, line,
function and the SOURCE LINE -- code, never data -- whereas the message is the
one part of a rendered traceback that interpolates a value. So the contract's
required half is satisfied exactly, and the exempt field is the one omitted.

TELEMETRY NEVER FAILS AN INGEST. Every call here is guarded. The package's own
rule for the correlation id says it best: observability must never fail an
ingest. A contract violation in a constant defined in this module is a
programming error caught by ``tests/test_telemetry.py``, which checks these
constants against the emitter's real registry -- not something a customer's
ingest run should discover.
"""

from __future__ import annotations

import logging
import os
import traceback
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

#: The registry rows for this repo, hard-coded here at the entry point. Never
#: derived from a process, module, container or host name -- deriving service
#: identity from the process is the defect the contract exists to close.
SERVICE = "data-ingestors"
COMPONENT = "ingest-job"

#: The environment variable this package already reads for its deployment
#: environment (see ``Config.EDGE_ENV``). Read RAW here rather than through
#: ``Config``, and the difference is deliberate: ``Config.EDGE_ENV`` defaults to
#: ``"prod"`` when the variable is unset, which is the right default for picking
#: an API endpoint and the wrong one for stamping a record. The contract's rule
#: is that an unrecognised environment disables the exporter and says so, never
#: that it is guessed -- records landing under a guessed value is the defect
#: being fixed, not a convenience. So an unset variable here means "no
#: environment, do not export", not "prod".
ENVIRONMENT_ENV = "CLIENT_ENV"

#: Event names: ``<domain>.<object>.<outcome>``, three segments, compile-time
#: constants. ``ingest`` is the registered domain for this service and the
#: outcome verbs are the registered ones. ONE object (``job``) on purpose: it
#: makes the pairing rule -- every operation that can fail reports a terminal
#: event on every path, so a failure RATE is computable -- a property of this
#: four-name set rather than a convention someone has to remember.
EVENT_JOB_STARTED = "ingest.job.started"
EVENT_JOB_SUCCEEDED = "ingest.job.succeeded"
EVENT_JOB_REJECTED = "ingest.job.rejected"
EVENT_JOB_FAILED = "ingest.job.failed"
#: A run the platform ended: evicted, drained, or past `activeDeadlineSeconds`.
#: `cancelled` is already in the emitter registry's closed OUTCOMES set, so this
#: validates without a registry change.
EVENT_JOB_CANCELLED = "ingest.job.cancelled"

#: The four ways a run ends. Exactly one is emitted per run (see ``_terminal``).
TERMINAL_EVENTS = frozenset(
    {
        EVENT_JOB_SUCCEEDED,
        EVENT_JOB_REJECTED,
        EVENT_JOB_FAILED,
        EVENT_JOB_CANCELLED,
    }
)

#: ``error.type`` -- a stable, low-cardinality classification, closed for this
#: domain. Not the exception class name: that is an implementation detail with
#: unbounded cardinality, and grouping failures by it is what stops a failure
#: mode having a count.
#:
#: The first five are the entrypoint's config-rejection paths, which end the run
#: before any data is read. The last three are run failures.
ERROR_CONFIG_MISSING = "config_missing"
ERROR_CONFIG_NOT_FOUND = "config_not_found"
ERROR_CONFIG_UNPARSEABLE = "config_unparseable"
ERROR_CONFIG_NOT_A_MAPPING = "config_not_a_mapping"
ERROR_CONFIG_INVALID = "config_invalid"
ERROR_INGESTION_FAILED = "ingestion_failed"
ERROR_RECORDS_FAILED = "records_failed"
#: An exception escaped the run entirely. This is not a theoretical bucket: a
#: missing DB credential or missing backend auth raises out of ``Database()`` /
#: ``APIClient()`` construction, and today that produces a raw traceback and no
#: event at all.
ERROR_UNHANDLED_EXCEPTION = "unhandled_exception"
#: A non-zero exit that reached the funnel without classifying itself. Reported
#: rather than hidden: a path that forgets to say why it failed is a finding,
#: and "cannot tell" must be visible instead of looking like a clean run.
ERROR_UNCLASSIFIED = "unclassified"

ERROR_TYPES = frozenset(
    {
        ERROR_CONFIG_MISSING,
        ERROR_CONFIG_NOT_FOUND,
        ERROR_CONFIG_UNPARSEABLE,
        ERROR_CONFIG_NOT_A_MAPPING,
        ERROR_CONFIG_INVALID,
        ERROR_INGESTION_FAILED,
        ERROR_RECORDS_FAILED,
        ERROR_UNHANDLED_EXCEPTION,
        ERROR_UNCLASSIFIED,
    }
)

# ---------------------------------------------------------------------------
# Per-run state
# ---------------------------------------------------------------------------

# The correlation id is record scope, not resource scope: it identifies the RUN,
# not the process, so it is attached per record rather than baked into the
# resource by configure(). Held here so no call site has to thread it through.
_correlation_id: Optional[str] = None
_terminal_emitted: bool = False
_durable: bool = False


def terminal_emitted() -> bool:
    """Has this run already reported how it ended?"""
    return _terminal_emitted


def durable() -> bool:
    """Has this run's work been committed? See ``mark_durable``."""
    return _durable


def reset() -> None:
    """Forget the current run. For tests, and for re-entry.

    Module state that survives between runs is how a test can assert "no event
    was emitted" and be right for the wrong reason — the terminal flag left
    standing by the previous run suppresses the emit the test was checking for,
    and a vacuous pass looks exactly like a real one.

    ``_durable`` is cleared here and in ``begin_run`` for the sharper version of
    the same hazard: a second run in a process that already completed one would
    inherit the first run's durability and report a cancellation during its own
    STARTUP as a success.
    """
    global _correlation_id, _terminal_emitted, _durable
    _correlation_id = None
    _terminal_emitted = False
    _durable = False


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def safe_stacktrace(exc: Optional[BaseException]) -> str:
    """The frames of ``exc``, and never its message.

    ``traceback.format_exception`` / ``format_exc`` append
    ``<Type>: <str(exc)>`` -- the one part of a rendered traceback that
    interpolates a runtime value, and on this package's paths that value is a
    customer cell. ``format_tb`` renders the frame list alone: file, line,
    function and the source line, all of which are code.

    Returns ``""`` when there is nothing to render, which is also the signal the
    caller uses to omit the whole exception attribute set rather than send half
    of it.
    """
    if exc is None:
        return ""
    tb = getattr(exc, "__traceback__", None)
    if tb is None:
        return ""
    return "".join(traceback.format_tb(tb)).strip()


# ---------------------------------------------------------------------------
# Emission
# ---------------------------------------------------------------------------


def configure_job() -> bool:
    """Set up this process's telemetry. Call once, at the entry point.

    Returns whether telemetry is available, so a test can fail closed on a
    missing dependency instead of quietly asserting nothing.
    """
    try:
        from tracebloc_telemetry import configure

        from . import __version__

        configure(
            service=SERVICE,
            component=COMPONENT,
            # The released artifact's identity for this service is the package
            # version -- the same literal setup.py parses, so the wheel, the
            # image built from it and this record cannot disagree.
            version=__version__,
            environment=os.environ.get(ENVIRONMENT_ENV),
        )
        return True
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(
            "telemetry: could not configure (%s); this run will report no "
            "events. Ingestion is unaffected.",
            type(exc).__name__,
        )
        return False


def begin_run(correlation_id: Optional[str] = None) -> bool:
    """Open the run: remember its correlation id and report that it started."""
    global _correlation_id, _terminal_emitted, _durable
    _correlation_id = correlation_id or None
    _terminal_emitted = False
    _durable = False
    return _emit(EVENT_JOB_STARTED, "INFO")


def mark_durable() -> None:
    """This run's work is committed: rows durable, dataset registered.

    From here on the run has SUCCEEDED, whatever happens to the process. What
    remains is teardown — reclaiming the staged source tree, releasing the table
    lock, one log line — and none of it can un-commit the load or un-register the
    dataset.

    That is worth recording because the process can still be killed during it,
    and a SIGTERM there is not a cancellation: the platform is stopping a process
    that has already finished its work. Without this flag the signal handler
    could not tell those apart and called every SIGTERM a cancellation, which
    reported ``cancelled`` for a live, registered dataset and undercounted
    successes (backend#2435).

    Idempotent and cheap, so it may be called from more than one place; the
    run's own proof point calls it as early as it can prove it, and the
    entrypoint calls it again as a backstop for a success path that never
    reached that point. Cleared by ``begin_run`` / ``reset``, never set back to
    False within a run: durability is not a thing that stops being true.
    """
    global _durable
    _durable = True


def job_cancelled() -> bool:
    """The platform ended this run: evicted, drained, or deadline-exceeded.

    Distinct from ``job_failed``: nothing about the ingest went wrong, and
    counting these as failures would make the failure rate a function of cluster
    pressure.

    NOT distinct from ``job_succeeded`` once the work is durable, which is the
    only reason this function decides anything. A signal that arrives after the
    rows are committed and the dataset is registered stopped a process, not a
    run; reporting it as a cancellation would subtract a success that actually
    happened and is visible in the customer's cluster. So the outcome reported
    here is a function of ``mark_durable``, not of which signal arrived.

    The severity follows the outcome (``INFO`` for the success, ``WARN`` for the
    cancellation) because it is read off ``job_succeeded`` rather than restated.
    """
    if _durable:
        # Logged only if the record was actually the one sent. A run can be
        # durable AND already classified — something broke during the teardown
        # and said so — in which case `_terminal` drops this and a line
        # claiming the run was reported as succeeded would be describing an
        # emit that never happened.
        reported = job_succeeded()
        if reported:
            logger.info(
                "telemetry: signalled after the load was durable; reporting "
                "this run as %s rather than %s — the dataset is committed and "
                "registered.",
                EVENT_JOB_SUCCEEDED,
                EVENT_JOB_CANCELLED,
            )
        return reported
    return _terminal(EVENT_JOB_CANCELLED, "WARN")


def job_succeeded() -> bool:
    """The run finished with every record ingested."""
    return _terminal(EVENT_JOB_SUCCEEDED, "INFO")


def job_rejected(error_type: str) -> bool:
    """The run never started: its configuration was refused."""
    return _terminal(EVENT_JOB_REJECTED, "ERROR", error_type=error_type)


def job_failed(
    error_type: str,
    exc: Optional[BaseException] = None,
    failed_records: Optional[int] = None,
) -> bool:
    """The run started and did not finish cleanly.

    ``failed_records`` is a COUNT. Counts, like column names and row indices,
    are dataset structure and may be reported; the records themselves may not.
    """
    attributes: Dict[str, Any] = {"error_type": error_type}

    # Both, or neither. The contract requires the type and the stacktrace
    # together once an exception was caught, so sending the type without a
    # renderable stacktrace would be a half-populated failure record -- the
    # shape that makes an error report undiagnosable.
    stack = safe_stacktrace(exc)
    if stack:
        attributes["exception__type"] = type(exc).__name__
        attributes["exception__stacktrace"] = stack

    if failed_records is not None:
        attributes["tracebloc__ingest__failed_records"] = int(failed_records)

    return _terminal(EVENT_JOB_FAILED, "ERROR", **attributes)


def _terminal(event_name: str, severity: str, **attributes: Any) -> bool:
    """Emit the run's terminal event, at most once.

    THIS IS WHY THE PAIRING RULE HOLDS STRUCTURALLY rather than by review. The
    entrypoint's funnel reports a terminal event on every path it can take,
    including the ones that already classified themselves; this drops the
    duplicate. So a run emits exactly one terminal event -- never two, and never
    zero, even down a path added later that forgot to say anything.

    SCOPED, because "structurally" was over-claimed here and it matters. The
    funnel catches ``BaseException``, which covers every way PYTHON leaves the
    frame. It does not cover a signal, whose default disposition terminates the
    interpreter without unwinding -- no exception, no ``finally``, no
    ``atexit``. @shujaatTracebloc measured that on this branch: SIGTERM at 3s
    produced ``ingest.job.started`` and nothing else. ``main`` now handles
    SIGTERM for exactly that reason.

    What remains uncovered, stated rather than implied: **SIGKILL cannot be
    caught**, so an OOM-killed run still emits ``started`` alone. In Kubernetes
    that is the OOMKilled path specifically -- eviction, node drain and
    ``activeDeadlineSeconds`` all send SIGTERM first and are now covered.

    The flag is set BEFORE the emit, deliberately: a run whose terminal record
    could not be delivered is still a run that ended, and letting the funnel's
    backstop fire behind it would report the same ending twice under two
    different classifications.
    """
    global _terminal_emitted
    if _terminal_emitted:
        return False
    _terminal_emitted = True
    return _emit(event_name, severity, **attributes)


def _emit(event_name: str, severity: str, **attributes: Any) -> bool:
    """Hand one record to the contract emitter. Never raises."""
    try:
        from tracebloc_telemetry import emit

        if _correlation_id:
            attributes["tracebloc__ingest__correlation_id"] = _correlation_id
        emit(event_name, severity=severity, **attributes)
        return True
    except Exception as exc:
        # The exception's CLASS only, following this package's standing pattern:
        # a contract violation names the attribute it refused, and an attribute
        # is the thing likeliest to be holding customer data at this point.
        logger.warning(
            "telemetry: could not emit %s (%s); ingestion is unaffected.",
            event_name,
            type(exc).__name__,
        )
        return False
