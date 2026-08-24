"""Declarative ingest entrypoint — ``tracebloc_ingestor.cli.run:main``.

Run by the official ingestor image (Ticket #45) when a customer triggers a
``helm install tracebloc/ingestor -f ingest.yaml`` (Ticket client#86). The
flow:

    1. Read ``INGEST_CONFIG`` env (path to YAML mounted by the Helm subchart).
    2. Parse + validate against ``schema/ingest.v1.json``. Fail fast with
       a single multi-line error listing every violation by JSON-pointer
       path. No DB / network I/O happens before validation passes.
    3. Resolve convention defaults via ``conventions.resolve`` (pure).
    4. Build the run's ``Config`` from the resolved YAML — ``SRC_PATH`` /
       ``TABLE_NAME`` / ``LABEL_FILE`` become explicit per-instance overrides
       (``_resolve_config``), with no ``os.environ`` bridge (P4c).
    5. Construct ``Database`` and ``APIClient`` with that Config (injected
       onward to the ingestor, its validators and file transfer). ``APIClient``
       triggers ``Config.validate()`` which fails fast on missing auth (#43).
    6. Dispatch to ``CSVIngestor`` or ``JSONIngestor`` based on
       ``source_type``.
    7. Run ``ingestor.ingest(source_path, batch_size=...)``.

Deferred to v1.1 (after client#86 lands):

- **Custom processors.** The schema accepts ``spec.processors[]`` today,
  but the runtime path requires the Helm subchart's ConfigMap-mounting
  story to actually deliver script bodies into the pod. We log a warning
  and skip when a config supplies processors; the rest of the run
  continues unchanged. Deferring this keeps the v1 surface honest:
  customers shouldn't write `processors:` until the deployment path is
  real.
- **Line-numbered validation errors.** Today the entrypoint emits
  ``<json-pointer>: <message>`` per error, which lets customers grep
  their YAML. Real line numbers require a YAML loader that preserves
  position info (``ruamel.yaml`` or a custom ``SafeLoader``); deferred
  as a quality-of-life improvement.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml
from jsonschema import Draft7Validator, ValidationError

from .. import telemetry
from ..api.client import APIClient
from ..config import Config
from ..database import Database
from ..ingestors import CSVIngestor, JSONIngestor
from ..utils.correlation import resolve_correlation_id
from ..utils.logging import setup_logging
from .conventions import ResolvedConfig, resolve

logger = logging.getLogger(__name__)


# Schema is bundled inside the package at tracebloc_ingestor/schema/ingest.v1.json
# so it's discoverable post pip-install (not just from a repo checkout). This
# file lives at tracebloc_ingestor/cli/run.py, so the schema is one parent up.
_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema" / "ingest.v1.json"


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------


def main(argv: List[str] | None = None) -> int:
    """Entrypoint registered as the ``tracebloc-ingest`` console script.

    Returns the process exit code. A non-zero return is converted to
    ``sys.exit`` by the console-script wrapper; using a return value (rather
    than raising) keeps the function testable from inside pytest.

    THIS IS THE TELEMETRY FUNNEL, and that is the only reason the run itself
    lives in ``_run``. Every path out of this function — a clean exit, a
    classified failure, an exception nobody caught — reports exactly one
    terminal event, because the funnel reports one on every branch and
    ``telemetry._terminal`` drops the duplicate when the path already
    classified itself. A run that ends without saying how it ended is
    indistinguishable from a run that never started, and that silence is the
    thing worth removing.
    """
    # Group-writable by default (umask 002) so every directory and file the run
    # writes under DEST_PATH can be reclaimed by the `data delete` teardown
    # group (uid/gid 65532), not just the setgid DEST_PATH root itself — the
    # hostPath half of client-runtime#172 (the teardown pod's fsGroup is ignored
    # on hostPath). Files themselves need no write bit to be deleted; the
    # containing directories do, which this guarantees for the whole tree.
    os.umask(0o002)

    telemetry.configure_job()
    # Resolved here as well as in BaseIngestor, so the config-rejection paths
    # below — which end the run before an ingestor exists — can still name their
    # run. The only cost is that a MALFORMED id logs its warning twice, which is
    # cheaper than a second copy of the id's validation rule.
    telemetry.begin_run(resolve_correlation_id())

    try:
        code = _run(argv)
    except BaseException as exc:
        # Startup failures live here: a missing DB credential or missing backend
        # auth raises out of Database() / APIClient() construction, and until now
        # produced a raw traceback and no event whatsoever.
        telemetry.job_failed(telemetry.ERROR_UNHANDLED_EXCEPTION, exc)
        raise

    if code == 0:
        telemetry.job_succeeded()
    else:
        telemetry.job_failed(telemetry.ERROR_UNCLASSIFIED)
    return code


def _run(argv: List[str] | None = None) -> int:
    """The run itself. See ``main`` for why it is a separate function."""
    config_path = os.environ.get("INGEST_CONFIG")
    if not config_path:
        return _fail(
            "INGEST_CONFIG env var not set. The official image expects the "
            "Helm subchart (client#86) to mount the ingest.yaml and set "
            "INGEST_CONFIG to its path.",
            telemetry.ERROR_CONFIG_MISSING,
        )

    raw_path = Path(config_path)
    if not raw_path.is_file():
        return _fail(
            f"INGEST_CONFIG points to {config_path} which does not exist.",
            telemetry.ERROR_CONFIG_NOT_FOUND,
        )

    try:
        raw_config = yaml.safe_load(raw_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        return _fail(
            f"ingest.yaml is not valid YAML:\n  {e}",
            telemetry.ERROR_CONFIG_UNPARSEABLE,
        )

    if not isinstance(raw_config, dict):
        return _fail(
            "ingest.yaml must be a mapping at the top level "
            "(apiVersion / kind / category / ...).",
            telemetry.ERROR_CONFIG_NOT_A_MAPPING,
        )

    errors = list(_validate(raw_config))
    if errors:
        return _fail(
            "ingest.yaml validation failed:\n" + _format_errors(errors),
            telemetry.ERROR_CONFIG_INVALID,
        )

    resolved = resolve(raw_config)

    if resolved.processor_specs:
        logger.warning(
            "spec.processors is accepted by the schema but is not yet "
            "executed at runtime. Custom-processor support requires the "
            "Helm subchart from client#86 to land first; skipping %d "
            "processor(s) and continuing without them.",
            len(resolved.processor_specs),
        )

    if resolved.validators_override:
        logger.warning(
            "spec.validators is accepted by the schema but is not yet "
            "honoured at runtime; the default validator set from "
            "map_validators(category) will run instead. Ignoring %d "
            "override(s).",
            len(resolved.validators_override),
        )

    if resolved.sidecars:
        logger.warning(
            "spec.sidecars is accepted by the schema but is not yet "
            "honoured at runtime; the framework's per-category sidecar "
            "convention (images/, annotations/, masks/, texts/ under "
            "SRC_PATH) will be used instead. Ignoring %d sidecar entry(s).",
            len(resolved.sidecars),
        )

    config = _resolve_config(resolved)
    setup_logging(config)

    database = Database(config)
    api_client = APIClient(config)  # triggers config.validate() per #43

    ingestor = _build_ingestor(database, api_client, resolved)

    if ingestor.correlation_id:
        # backend#1028 item 3: the one always-visible line (print, not
        # logger — default LOG_LEVEL is WARNING) that ties this Job's log
        # to the CLI's correlation id and to the per-process ingestor_id
        # stamped on every MySQL row this run inserts.
        print(
            f"Correlation id: {ingestor.correlation_id} "
            f"(ingestor_id: {ingestor.ingestor_id})"
        )

    with ingestor:
        try:
            failed = ingestor.ingest(resolved.source_path, batch_size=config.BATCH_SIZE)
        except Exception as exc:
            # A hard failure during ingestion — validation, DB write, or backend
            # dataset registration. base.py has already logged the detail; turn
            # the exception into a clean, single-line non-zero exit rather than a
            # raw traceback, so the CLI's live log stream shows an actionable
            # reason and marks the Job failed.
            telemetry.job_failed(telemetry.ERROR_INGESTION_FAILED, exc)
            print(f"\nIngestion failed: {exc}", file=sys.stderr)
            return 1
        if failed:
            logger.warning(
                "%d record(s) failed during ingestion; see logs for details.",
                len(failed),
            )
            # The COUNT, never the records: counts are dataset structure and may
            # be reported, the rows themselves may not.
            telemetry.job_failed(
                telemetry.ERROR_RECORDS_FAILED, failed_records=len(failed)
            )
            return 1

    logger.info("Ingestion completed successfully.")
    return 0


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _load_schema() -> Dict[str, Any]:
    return json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))


def _validate(raw_config: Dict[str, Any]) -> Iterable[ValidationError]:
    """Yield every validation error against the v1 schema.

    Returns errors sorted by their absolute path so output is deterministic
    regardless of jsonschema's internal traversal order.
    """
    validator = Draft7Validator(_load_schema())
    return sorted(
        validator.iter_errors(raw_config), key=lambda e: list(e.absolute_path)
    )


def _describe_from_schema_path(
    schema: Dict[str, Any], schema_path: List[Any]
) -> Optional[str]:
    """Walk the schema from root toward the failing rule, returning the
    deepest ``description`` field encountered along the way.

    JSON Schema ``allOf`` rules in ``schema/ingest.v1.json`` carry rich
    ``description`` fields explaining each branch (e.g. "Self-supervised
    categories MUST NOT set `label`. The shipped CSV has no label column,
    and the framework registers no edge-label metadata for them…"), but
    ``ValidationError.message`` only surfaces the mechanic
    ("``{full yaml dump}`` should not be valid under ``{'required':
    ['label']}``"). That raw message is correct but practically
    unreadable: it dumps the entire submission as a Python dict and uses
    JSON-schema vocabulary that the customer didn't write.

    Walking from root captures the description nearest the failing rule,
    so the user sees the rule author's intent (and the suggested fix)
    instead of the mechanic. Returns ``None`` when no description applies
    — the caller falls back to ``e.message`` so primitive checks
    (``'label' is a required property``, ``'foo' is not one of [...]``)
    still surface their existing clear messages.
    """
    # Walk step-by-step, only capturing descriptions on nodes we DESCEND
    # INTO — never the root's. The root description in ingest.v1.json is a
    # generic blurb ("Declarative configuration for the tracebloc data
    # ingestor…") that's correct for the schema as a whole but would
    # blanket-attach to every primitive error, overwriting jsonschema's
    # clear `'X' is a required property` messages with the generic prose
    # (bugbot #254). Only INNER descriptions — the ones authored on
    # `allOf` branches, sub-property definitions, etc. — are rule-specific
    # enough to be worth surfacing.
    description = None
    node: Any = schema
    for step in schema_path:
        try:
            node = node[step]
        except (KeyError, IndexError, TypeError):
            return description
        if isinstance(node, dict) and isinstance(node.get("description"), str):
            description = node["description"]
    return description


def _format_errors(errors: List[ValidationError]) -> str:
    """Format errors as ``<json-pointer>: <description-or-message>``,
    one per line. When a failing rule has a ``description`` field in the
    schema, surface THAT (with the underlying mechanic on a follow-up
    line) — the descriptions in ``schema/ingest.v1.json`` are
    customer-facing prose with rationale and fix hints, and the mechanic
    is JSON-schema vocabulary the customer didn't write.

    Real line numbers (per the ticket) require a YAML loader that
    preserves position info. v1.1 follow-up — for now, the JSON-pointer
    path is enough to grep the customer's YAML.
    """
    schema = _load_schema()
    lines = []
    for e in errors:
        path = ".".join(str(p) for p in e.absolute_path) or "<root>"
        description = _describe_from_schema_path(schema, list(e.absolute_schema_path))
        if description:
            lines.append(f"  {path}: {description}")
            # Keep the mechanic on a follow-up line for power-user
            # debugging without burying it in the headline.
            lines.append(f"      (rule: {e.validator}={e.validator_value!r})")
        else:
            lines.append(f"  {path}: {e.message}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Config resolution — build the run's Config from the resolved ingest.yaml
# ---------------------------------------------------------------------------


def _resolve_config(resolved: ResolvedConfig) -> Config:
    """Build the run's :class:`Config` from the resolved ingest.yaml (P4c).

    Replaces the former env-var bridge (``_set_legacy_env_vars``). Instead of
    writing ``SRC_PATH`` / ``TABLE_NAME`` / ``LABEL_FILE`` into ``os.environ``
    for a scattering of module-global ``Config()`` instances to read, we build
    ONE Config with these as explicit per-instance overrides and inject it:
    ``Database`` -> ``BaseIngestor`` -> validators (``map_validators``, P4b)
    and file transfer (``map_file_transfer``, P4c). No process-global env
    mutation, so the path layer is driven by construction, not spooky action
    through ``os.environ``.

    ``SRC_PATH`` is derived from whichever sidecar directory is set, since
    ``file_transfer`` joins ``SRC_PATH/<subfolder>/<filename>`` for each
    category. The dominant convention is that all sidecar dirs share a parent
    (``/data/images/``, ``/data/annotations/``, …) — for non-standard layouts,
    customers use ``spec.sidecars[]`` (deferred). Omitting the ``SRC_PATH``
    override when no sidecar dir is set preserves the prior behavior exactly
    (the bridge only set the env var in that case), letting ``Config.SRC_PATH``
    fall back to its empty default for tabular / time-series categories.
    """
    overrides = {
        "TABLE_NAME": resolved.table_name,
        "LABEL_FILE": resolved.source_path,
    }
    src_path_source = (
        resolved.images
        or resolved.texts
        or resolved.masks
        or resolved.annotations
        or resolved.sequences
    )
    if src_path_source:
        overrides["SRC_PATH"] = os.path.dirname(src_path_source.rstrip("/"))

    return Config(**overrides)


# ---------------------------------------------------------------------------
# Ingestor dispatch
# ---------------------------------------------------------------------------


def _build_ingestor(
    database: Database,
    api_client: APIClient,
    resolved: ResolvedConfig,
):
    """Construct the right ingestor for the source type."""
    common_kwargs = dict(
        database=database,
        api_client=api_client,
        table_name=resolved.table_name,
        schema=resolved.schema,
        unique_id_column=resolved.unique_id_column,
        data_id_strategy=resolved.data_id_strategy,
        label_column=resolved.label_column,
        intent=resolved.intent,
        annotation_column=resolved.annotation_column,
        category=resolved.category,
        data_format=resolved.data_format,
        label_policy=resolved.label_policy,
    )

    if resolved.source_type == "csv":
        return CSVIngestor(
            **common_kwargs,
            csv_options=resolved.csv_options,
            file_options=resolved.file_options,
        )

    if resolved.source_type == "json":
        # The schema doesn't expose json_options yet; defaults are fine
        # for v1. file_options carries category-specific knobs that
        # BaseIngestor passes to map_validators (e.g. time_column for
        # time_to_event_prediction, target_size for image categories).
        return JSONIngestor(
            **common_kwargs,
            json_options={},
            file_options=resolved.file_options,
        )

    raise ValueError(
        f"Unknown source_type {resolved.source_type!r}; "
        "this is a bug — the schema's oneOf should have rejected the config."
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fail(message: str, error_type: str) -> int:
    """Print to stderr and return non-zero. Logger may not be configured yet
    when validation fails (it depends on Config() which depends on env
    being set), so a plain stderr write is the most reliable channel.

    The single funnel for "the configuration was refused and no data was read",
    so a rejection path cannot be added without also classifying itself.
    ``error_type`` is a constant from ``telemetry.ERROR_TYPES``; a literal here
    is a finding, and ``tests/test_telemetry.py`` reads this module to say so.
    """
    print(message, file=sys.stderr)
    telemetry.job_rejected(error_type)
    return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
