"""The ingest Job's contract telemetry.

Three things are worth checking here and they are checked three different ways,
because only one of them is a behaviour.

1. **Agreement with the contract.** Every vocabulary these tests compare against
   is READ from the emitter package's own registry and checked by the emitter's
   own checkers. Nothing here holds a second copy of the service table, the
   domain list or the outcome verbs: a hand-written copy agrees with itself
   while drifting from the thing it claims to mirror.

2. **Completeness of the classification.** The entrypoint is PARSED, so the set
   of ``error.type`` constants it actually uses is derived from the code rather
   than listed here. A constant that is declared and never used, a call site
   that passes a bare string, and a constant missing from ``ERROR_TYPES`` are
   all findings rather than things a reviewer has to notice.

3. **Exactly one terminal event per run.** Driven over the whole outcome space
   of ``_run`` — returns zero, returns non-zero, raises — because that is the
   entirety of what the funnel has to cope with.

Redaction has its own home in ``tests/test_error_redaction.py``, alongside the
rest of the "a cell value can never reach this" gate; the leak-shaped assertions
here are the emitter-side half and each one states its mutation anchor.
"""

from __future__ import annotations

import ast
import logging
import signal
import traceback
from pathlib import Path

import pytest

import tracebloc_telemetry
from tracebloc_ingestor import telemetry
from tracebloc_ingestor.cli import run as cli_run

SECRET = "patient-4711-Müller"

RUN_PY = Path(cli_run.__file__)

#: Every event this module can emit. Derived from the module rather than
#: retyped, so a fifth event cannot be added without these checks seeing it.
ALL_EVENTS = frozenset({telemetry.EVENT_JOB_STARTED}) | telemetry.TERMINAL_EVENTS


@pytest.fixture(autouse=True)
def fresh_telemetry(monkeypatch):
    """A configured, non-exporting emitter and a closed run, per test."""
    monkeypatch.delenv("TRACEBLOC_ENV", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    tracebloc_telemetry.reset()
    telemetry.reset()
    telemetry.configure_job()
    yield
    tracebloc_telemetry.reset()
    telemetry.reset()


@pytest.fixture
def emitted(caplog):
    """The contract records this test produced, in order."""
    caplog.set_level(logging.DEBUG, logger="tracebloc.telemetry")

    def _records():
        return [
            record.telemetry
            for record in caplog.records
            if hasattr(record, "telemetry")
        ]

    return _records


def _error_constants() -> dict:
    """``{attribute name: value}`` for every ``ERROR_*`` string in the module."""
    return {
        name: getattr(telemetry, name)
        for name in dir(telemetry)
        if name.startswith("ERROR_") and isinstance(getattr(telemetry, name), str)
    }


def _entrypoint_ast() -> ast.Module:
    return ast.parse(RUN_PY.read_text(encoding="utf-8"))


def _attribute_text(record: dict) -> str:
    """Every attribute VALUE of a record, as text, unescaped.

    Deliberately not ``json.dumps``: it escapes non-ASCII by default, so
    ``"Müller"`` becomes ``"M\\u00fcller"`` and a leak assertion written
    against the raw marker passes while the value is right there in the
    record. Measured, not theorised — the first mutation run of
    ``safe_stacktrace`` reddened only the unit test and sailed past three
    end-to-end ones for exactly this reason.
    """
    return "\n".join(f"{key}={value!r}" for key, value in record.items())


# ── the dependency itself ────────────────────────────────────────────────────


def test_the_contract_emitter_is_installed_and_configures():
    """Fails CLOSED on a missing dependency.

    Without this, an absent ``tracebloc-telemetry`` would make every guarded
    call below a silent no-op, and a suite that emits nothing and asserts
    nothing about it looks exactly like a suite that passes.
    """
    assert telemetry.configure_job() is True
    assert tracebloc_telemetry.current() is not None


def test_the_resource_reports_the_package_version():
    from tracebloc_ingestor import __version__

    resource = tracebloc_telemetry.current().resource
    assert resource["service.version"] == __version__
    assert resource["service.version"] != tracebloc_telemetry.UNKNOWN_VERSION


# ── agreement with the registry ──────────────────────────────────────────────


def test_service_and_component_are_a_registered_pair():
    from tracebloc_telemetry._registry import SERVICES

    assert telemetry.SERVICE in SERVICES
    assert telemetry.COMPONENT in SERVICES[telemetry.SERVICE]


def test_every_event_name_passes_the_emitters_own_grammar_check():
    from tracebloc_telemetry import ContractViolation
    from tracebloc_telemetry._contract import check_event_name

    for name in sorted(ALL_EVENTS):
        check_event_name(name)

    # ANCHOR: the checker above is armed. Without this, a check_event_name that
    # had become a no-op would make the loop pass on any string at all, and the
    # log would be indistinguishable from real coverage.
    with pytest.raises(ContractViolation):
        check_event_name("ingest.job.refreshed")


def test_every_event_uses_the_registered_domain_and_outcome():
    from tracebloc_telemetry._registry import DOMAINS, FAILURE_OUTCOMES, OUTCOMES

    for name in sorted(ALL_EVENTS):
        domain, _object, outcome = name.split(".")
        assert domain in DOMAINS
        assert outcome in OUTCOMES

    # The two terminals that report a problem must be failure outcomes, because
    # that is what obliges them to carry `error.type`; the success one must not.
    for name in (telemetry.EVENT_JOB_REJECTED, telemetry.EVENT_JOB_FAILED):
        assert name.rsplit(".", 1)[-1] in FAILURE_OUTCOMES
    assert telemetry.EVENT_JOB_SUCCEEDED.rsplit(".", 1)[-1] not in FAILURE_OUTCOMES


def test_every_terminal_event_pairs_with_the_started_event():
    """One object, so a failure RATE is computable."""
    assert telemetry.TERMINAL_EVENTS
    subject = telemetry.EVENT_JOB_STARTED.rsplit(".", 1)[0]
    for name in sorted(telemetry.TERMINAL_EVENTS):
        assert name.rsplit(".", 1)[0] == subject


# ── completeness of the classification, read off the entrypoint ──────────────


def test_error_types_is_exactly_the_set_of_declared_constants():
    declared = _error_constants()
    assert set(declared.values()) == telemetry.ERROR_TYPES


def test_the_entrypoint_uses_every_declared_error_constant_and_no_other():
    """Derived from the code, not restated.

    A constant added to the module and never used is a classification nothing
    can ever produce; a call site naming a constant that does not exist is an
    ``AttributeError`` in production. Both are read off the entrypoint's AST.
    """
    referenced = {
        node.attr
        for node in ast.walk(_entrypoint_ast())
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "telemetry"
        and node.attr.startswith("ERROR_")
    }
    assert referenced, "the entrypoint references no telemetry.ERROR_* constant"
    assert referenced == set(_error_constants())
    for name in sorted(referenced):
        assert getattr(telemetry, name) in telemetry.ERROR_TYPES


def test_no_call_site_passes_a_literal_error_type():
    """An event's classification is a compile-time constant, never a string.

    A literal at the call site is how a closed vocabulary stops being closed:
    it is spelled slightly differently once and the two spellings each get half
    the count.
    """
    offenders = []
    for node in ast.walk(_entrypoint_ast()):
        if not isinstance(node, ast.Call):
            continue
        args = []
        if isinstance(node.func, ast.Name) and node.func.id == "_fail":
            args = node.args[1:2]
        elif (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "telemetry"
            and node.func.attr in ("job_failed", "job_rejected")
        ):
            args = node.args[:1]
        for arg in args:
            if isinstance(arg, ast.Constant):
                offenders.append((node.lineno, arg.value))
    assert offenders == []


def test_every_rejection_path_classifies_itself():
    """``_fail`` is the single rejection funnel, and it takes a classification.

    Checked structurally so a sixth rejection path cannot be added silently:
    every call to ``_fail`` must pass two arguments.
    """
    calls = [
        node
        for node in ast.walk(_entrypoint_ast())
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_fail"
    ]
    assert calls, "the entrypoint has no rejection path — did _fail get renamed?"
    for node in calls:
        assert len(node.args) == 2, f"_fail at line {node.lineno} names no error type"


# ── exactly one terminal event, over the whole outcome space ─────────────────


@pytest.mark.parametrize("outcome", ["returns_zero", "returns_nonzero", "raises"])
def test_main_reports_exactly_one_terminal_event(outcome, emitted, monkeypatch):
    """``_run`` returns an int or raises. That is the whole space; drive it."""
    if outcome == "raises":

        def _body(argv=None):
            raise RuntimeError("boom")

    else:
        code = 0 if outcome == "returns_zero" else 7

        def _body(argv=None, _code=code):
            return _code

    monkeypatch.setattr(cli_run, "_run", _body)

    if outcome == "raises":
        with pytest.raises(RuntimeError):
            cli_run.main()
    else:
        cli_run.main()

    names = [record["event.name"] for record in emitted()]
    assert names.count(telemetry.EVENT_JOB_STARTED) == 1
    terminals = [name for name in names if name in telemetry.TERMINAL_EVENTS]
    assert len(terminals) == 1, names


def test_sigterm_reports_the_run_as_cancelled(emitted, monkeypatch):
    """A run the platform ends still says how it ended (backend#2417).

    SIGTERM's default disposition terminates the interpreter WITHOUT unwinding —
    no exception, no ``finally``, no ``atexit`` — so ``main``'s ``except
    BaseException`` funnel never sees it. @shujaatTracebloc measured that on this
    branch: SIGTERM at 3s produced ``ingest.job.started`` and nothing else, which
    is the silence this whole change exists to remove, on the ending where the
    record is most useful (eviction, drain, deadline).

    Driven through the REAL handler the entrypoint installs rather than a copy of
    it, so deleting `signal.signal(...)` from ``main`` reddens this.
    """
    installed = {}
    real_signal = signal.signal

    def _capture(signum, handler):
        if signum == signal.SIGTERM:
            installed["handler"] = handler
        return real_signal(signum, handler)

    monkeypatch.setattr(cli_run.signal, "signal", _capture)
    monkeypatch.setattr(cli_run, "_run", lambda argv=None: 0)
    cli_run.main()

    assert "handler" in installed, "main() installed no SIGTERM handler"

    # A fresh run, then the signal mid-flight.
    telemetry.reset()
    telemetry.configure_job()
    telemetry.begin_run("corr-sigterm")
    before = len(emitted())
    with pytest.raises(SystemExit) as exit_info:
        installed["handler"](signal.SIGTERM, None)

    assert exit_info.value.code == 128 + signal.SIGTERM
    names = [record["event.name"] for record in emitted()[before:]]
    assert telemetry.EVENT_JOB_CANCELLED in names, names
    terminals = [name for name in names if name in telemetry.TERMINAL_EVENTS]
    assert len(terminals) == 1, names


def test_a_cancelled_run_does_not_double_report(emitted, monkeypatch):
    """If the signal lands while a classified path is mid-flight, one wins.

    ``_terminal`` is what makes that true; this pins it for the signal path
    specifically, because the handler runs outside every control-flow guarantee
    the funnel relies on.
    """
    installed = {}
    real_signal = signal.signal
    monkeypatch.setattr(
        cli_run.signal,
        "signal",
        lambda num, h: (
            installed.__setitem__("handler", h) if num == signal.SIGTERM else None
        )
        or real_signal(num, h),
    )
    monkeypatch.setattr(cli_run, "_run", lambda argv=None: 0)
    cli_run.main()  # emits succeeded, closing the terminal slot

    before = len(emitted())
    with pytest.raises(SystemExit):
        installed["handler"](signal.SIGTERM, None)
    assert [
        r["event.name"] for r in emitted()[before:]
    ] == [], "the terminal slot was already taken; cancelling after it must be dropped"


# ── a signal after the work is done is not a cancellation (backend#2435) ─────


#: How to ESTABLISH each terminal outcome, keyed by the event that outcome
#: reports. Written against the run's own vocabulary and checked for closure
#: below, so a fifth terminal event cannot be added without either giving it a
#: way to be established here or being named as deliberately unestablishable.
_ESTABLISH = {
    telemetry.EVENT_JOB_SUCCEEDED: telemetry.mark_durable,
    telemetry.EVENT_JOB_REJECTED: lambda: telemetry.job_rejected(
        telemetry.ERROR_CONFIG_INVALID
    ),
    telemetry.EVENT_JOB_FAILED: lambda: telemetry.job_failed(
        telemetry.ERROR_INGESTION_FAILED
    ),
}


def test_every_establishable_outcome_is_covered():
    """Derived from ``TERMINAL_EVENTS``, not from the dict above.

    Mutation coverage cannot see a vocabulary gap: the parametrised test below
    would keep passing over three outcomes while a fourth went unexercised. So
    the input domain is compared against the producer's declared set, and the
    one member deliberately absent is named rather than silently missing.
    """
    assert set(_ESTABLISH) | {telemetry.EVENT_JOB_CANCELLED} == (
        telemetry.TERMINAL_EVENTS
    )
    # `cancelled` is the outcome the SIGTERM handler itself reports, so it is
    # not something a run establishes BEFORE the signal — it is what the
    # signal means when nothing else was established.
    assert telemetry.EVENT_JOB_CANCELLED not in _ESTABLISH


@pytest.mark.parametrize("established", sorted(_ESTABLISH))
def test_a_signal_never_overwrites_an_outcome_already_established(established, emitted):
    """``cancelled`` may not relabel an ending the run already reached.

    ``_terminal`` gave the classified failures this property for free — they
    take the slot as they happen. Success did not have it, because the funnel
    reports it only after the run unwinds, and that gap is backend#2435. Driven
    over the whole vocabulary so the property belongs to the SET rather than to
    the one member that was broken.
    """
    telemetry.begin_run("run-established")
    _ESTABLISH[established]()
    before = len(emitted())

    telemetry.job_cancelled()

    names = [record["event.name"] for record in emitted()]
    terminals = [name for name in names if name in telemetry.TERMINAL_EVENTS]
    assert terminals == [established], names
    # For the two that already emitted, the cancel is dropped and adds nothing;
    # for success, `mark_durable` emitted nothing and the cancel IS the emit.
    if established is telemetry.EVENT_JOB_SUCCEEDED:
        assert len(emitted()) == before + 1
    else:
        assert len(emitted()) == before


def test_durability_does_not_swallow_a_later_genuine_failure(emitted):
    """The expensive direction, pinned.

    ``mark_durable`` records a fact and leaves the terminal slot OPEN, rather
    than emitting ``succeeded`` early and closing it. The difference only shows
    up here: something that goes wrong after the commit still reports its own
    failure instead of being dropped as a duplicate of a success already
    claimed. Collapsing ``mark_durable`` into a ``job_succeeded`` call reddens
    this test, which is the point of it existing.
    """
    telemetry.begin_run("run-durable-then-broken")
    telemetry.mark_durable()

    # ANCHOR: the slot really is open, so the emit below is the failure being
    # recorded rather than a duplicate being dropped.
    assert telemetry.terminal_emitted() is False
    assert telemetry.job_failed(telemetry.ERROR_UNHANDLED_EXCEPTION) is True

    record = emitted()[-1]
    assert record["event.name"] == telemetry.EVENT_JOB_FAILED
    assert record["error.type"] == telemetry.ERROR_UNHANDLED_EXCEPTION


@pytest.mark.parametrize("close_run", ["begin_run", "reset"])
def test_durability_does_not_survive_into_the_next_run(close_run, emitted):
    """Per RUN, not per process — the hazard ``reset``'s docstring names.

    A process that completed one run and started another would otherwise carry
    the first run's durability into the second, and report a SIGTERM during the
    SECOND run's startup — before it has ingested anything — as a success.
    """
    telemetry.begin_run("run-1")
    telemetry.mark_durable()
    assert telemetry.durable() is True

    if close_run == "begin_run":
        telemetry.begin_run("run-2")
    else:
        telemetry.reset()
        # ANCHOR: ``reset`` clears durability ON ITS OWN, asserted here rather
        # than after the ``begin_run`` below — which clears it too, and so
        # masks this. Measured: without this line the [reset] case stayed
        # GREEN under a mutation that deleted ``reset``'s clear, i.e. it was
        # proving what ``begin_run`` does twice and what ``reset`` does not at
        # all.
        assert telemetry.durable() is False
        telemetry.configure_job()
        telemetry.begin_run("run-2")

    assert telemetry.durable() is False
    telemetry.job_cancelled()
    assert emitted()[-1]["event.name"] == telemetry.EVENT_JOB_CANCELLED


def test_the_ingestor_claims_durability_before_it_reclaims_the_source():
    """Read off ``base.py``, because that window is the long one.

    The entrypoint's own ``mark_durable`` covers the gap after ``ingest``
    returns; it cannot cover the reclaim of the staged source tree, which
    happens INSIDE ``ingest`` and is an ``rmtree`` over a whole staged dataset.
    So the ingestor claims durability at the guard it already uses to decide
    the load is durable and clean — and it has to claim it BEFORE the reclaim,
    or it covers nothing.

    Structural rather than driven: reaching that line in a test needs a real
    load, so an assertion about the SOURCE is what keeps a later edit from
    quietly moving or dropping the call. Order is asserted, not just presence.
    """
    from tracebloc_ingestor.ingestors import base as ingestor_base

    tree = ast.parse(Path(ingestor_base.__file__).read_text(encoding="utf-8"))

    def _called_name(node):
        func = node.func
        if isinstance(func, ast.Name):
            return func.id
        if isinstance(func, ast.Attribute):
            return func.attr
        return None

    guards = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        calls = [
            _called_name(child)
            for child in ast.walk(ast.Module(body=node.body, type_ignores=[]))
            if isinstance(child, ast.Call)
        ]
        if "reclaim_source" in calls:
            guards.append(calls)

    assert len(guards) == 1, f"expected one reclaim_source guard, found {len(guards)}"
    calls = guards[0]
    assert "mark_durable" in calls, (
        "the ingestor reclaims the staged source without first claiming "
        "durability; a signal during the rmtree reports a committed dataset "
        "as cancelled (backend#2435)"
    )
    assert calls.index("mark_durable") < calls.index(
        "reclaim_source"
    ), "durability is claimed after the reclaim, so the reclaim window is uncovered"


def test_a_clean_run_reports_success(emitted, monkeypatch):
    monkeypatch.setattr(cli_run, "_run", lambda argv=None: 0)
    cli_run.main()
    assert emitted()[-1]["event.name"] == telemetry.EVENT_JOB_SUCCEEDED


def test_an_unclassified_nonzero_exit_says_so(emitted, monkeypatch):
    """A run that cannot say why it failed says THAT, rather than nothing."""
    monkeypatch.setattr(cli_run, "_run", lambda argv=None: 7)
    cli_run.main()
    record = emitted()[-1]
    assert record["event.name"] == telemetry.EVENT_JOB_FAILED
    assert record["error.type"] == telemetry.ERROR_UNCLASSIFIED


def test_an_escaping_exception_is_reported_and_still_raised(emitted, monkeypatch):
    def _body(argv=None):
        raise RuntimeError("startup failed")

    monkeypatch.setattr(cli_run, "_run", _body)
    with pytest.raises(RuntimeError):
        cli_run.main()

    record = emitted()[-1]
    assert record["event.name"] == telemetry.EVENT_JOB_FAILED
    assert record["error.type"] == telemetry.ERROR_UNHANDLED_EXCEPTION
    assert record["exception.type"] == "RuntimeError"
    assert record["exception.stacktrace"]


def test_the_funnel_never_overwrites_a_classified_failure(emitted, monkeypatch):
    """The backstop drops its duplicate; it does not relabel the real reason."""

    def _body(argv=None):
        telemetry.job_failed(telemetry.ERROR_RECORDS_FAILED, failed_records=3)
        return 1

    monkeypatch.setattr(cli_run, "_run", _body)
    cli_run.main()

    terminals = [
        record
        for record in emitted()
        if record["event.name"] in telemetry.TERMINAL_EVENTS
    ]
    assert len(terminals) == 1
    assert terminals[0]["error.type"] == telemetry.ERROR_RECORDS_FAILED
    assert terminals[0]["tracebloc.ingest.failed_records"] == 3


def test_the_correlation_id_rides_every_record(emitted):
    telemetry.begin_run("run-abc-123")
    telemetry.job_succeeded()
    records = emitted()
    assert len(records) == 2
    for record in records:
        assert record["tracebloc.ingest.correlation_id"] == "run-abc-123"


def test_an_absent_correlation_id_is_omitted_not_blanked(emitted):
    telemetry.begin_run(None)
    telemetry.job_succeeded()
    for record in emitted():
        assert "tracebloc.ingest.correlation_id" not in record


# ── the exception message never leaves ───────────────────────────────────────


def _caught(message: str) -> BaseException:
    """An exception carrying ``message``, with a real traceback attached.

    ``message`` is passed as a VARIABLE so the raising frame's source line reads
    ``raise ValueError(message)``. A literal here would put the value into the
    source line and the redaction assertions would be testing the wrong thing.
    """
    try:
        raise ValueError(message)
    except ValueError as exc:
        return exc


def test_safe_stacktrace_renders_frames_and_never_the_message():
    exc = _caught(SECRET)
    stack = telemetry.safe_stacktrace(exc)

    assert stack
    assert "test_telemetry.py" in stack
    assert SECRET not in stack

    # ANCHOR: the value really is in this exception, so the assertion above is
    # not passing because there was nothing to leak. A switch back to
    # format_exception / format_exc reddens the test.
    rendered = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    assert SECRET in rendered


def test_safe_stacktrace_has_nothing_to_say_about_a_bare_exception():
    assert telemetry.safe_stacktrace(None) == ""
    assert telemetry.safe_stacktrace(ValueError(SECRET)) == ""


def test_a_failure_record_carries_the_type_and_frames_but_no_message(emitted):
    exc = _caught(SECRET)
    telemetry.begin_run("run-1")
    telemetry.job_failed(telemetry.ERROR_INGESTION_FAILED, exc)

    record = emitted()[-1]
    assert record["exception.type"] == "ValueError"
    assert record["exception.stacktrace"]
    assert "exception.message" not in record
    assert SECRET not in _attribute_text(record)


def test_an_exception_without_frames_sends_neither_half(emitted):
    """Both, or neither — a type with no stacktrace is a half-populated record."""
    telemetry.begin_run("run-1")
    telemetry.job_failed(telemetry.ERROR_INGESTION_FAILED, ValueError(SECRET))

    record = emitted()[-1]
    assert "exception.type" not in record
    assert "exception.stacktrace" not in record
    assert record["error.type"] == telemetry.ERROR_INGESTION_FAILED


# ── telemetry never fails an ingest ──────────────────────────────────────────


def test_a_new_run_reopens_the_terminal_slot():
    """The at-most-once rule is per RUN, not per process."""
    telemetry.begin_run("run-1")
    assert telemetry.job_succeeded() is True
    assert telemetry.terminal_emitted() is True
    assert telemetry.job_failed(telemetry.ERROR_UNCLASSIFIED) is False

    telemetry.begin_run("run-2")
    assert telemetry.terminal_emitted() is False
    assert telemetry.job_failed(telemetry.ERROR_UNCLASSIFIED) is True


def test_a_refusing_emitter_does_not_fail_the_run(monkeypatch, caplog):
    # ANCHOR: with a working emitter the very same call returns True, so the
    # False below is the refusal being absorbed — not a duplicate terminal
    # being dropped, which would make this test pass while proving nothing.
    assert telemetry.job_failed(telemetry.ERROR_INGESTION_FAILED) is True
    telemetry.reset()

    def _boom(*args, **kwargs):
        raise RuntimeError(SECRET)

    monkeypatch.setattr(tracebloc_telemetry, "emit", _boom)
    caplog.set_level(logging.WARNING)

    assert telemetry.job_failed(telemetry.ERROR_INGESTION_FAILED) is False

    text = caplog.text
    assert telemetry.EVENT_JOB_FAILED in text
    assert "RuntimeError" in text
    # The diagnostic names the exception's CLASS, never its message — the same
    # rule the records themselves obey.
    assert SECRET not in text


def test_emitting_into_an_unconfigured_process_is_reported_not_raised(caplog):
    tracebloc_telemetry.reset()
    caplog.set_level(logging.WARNING)

    assert telemetry.begin_run("run-1") is False
    assert telemetry.job_succeeded() is False
    assert "telemetry" in caplog.text


# ── the environment is read raw, and is not guessed ──────────────────────────


def test_the_exporting_environments_agree_with_config(monkeypatch):
    """Agreement with ``Config``, derived from the registry's own set."""
    from tracebloc_telemetry._registry import EXPORTING_ENVIRONMENTS

    from tracebloc_ingestor.config import Config

    for environment in sorted(EXPORTING_ENVIRONMENTS):
        monkeypatch.setenv(telemetry.ENVIRONMENT_ENV, environment)
        tracebloc_telemetry.reset()
        telemetry.configure_job()

        resource = tracebloc_telemetry.current().resource
        assert resource["deployment.environment"] == environment
        assert Config().EDGE_ENV == environment


def test_an_unset_environment_is_not_guessed_as_prod(monkeypatch):
    """The one place this module deliberately disagrees with ``Config``."""
    from tracebloc_ingestor.config import Config

    monkeypatch.delenv(telemetry.ENVIRONMENT_ENV, raising=False)
    monkeypatch.delenv("TRACEBLOC_ENV", raising=False)
    tracebloc_telemetry.reset()
    telemetry.configure_job()

    configured = tracebloc_telemetry.current()
    assert "deployment.environment" not in configured.resource
    assert configured.decision.export is False

    # ANCHOR: Config really does default to prod, which is why the variable is
    # read raw above. Were that not so, the assertions above would be proving
    # nothing.
    assert Config().EDGE_ENV == "prod"


def test_a_non_exporting_environment_stays_on_the_console(monkeypatch):
    from tracebloc_telemetry._registry import NON_EXPORTING_ENVIRONMENTS

    for environment in sorted(NON_EXPORTING_ENVIRONMENTS):
        monkeypatch.setenv(telemetry.ENVIRONMENT_ENV, environment)
        tracebloc_telemetry.reset()
        telemetry.configure_job()
        assert tracebloc_telemetry.current().decision.export is False
