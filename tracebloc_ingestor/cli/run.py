"""Declarative ingest entrypoint — ``tracebloc_ingestor.cli.run:main``.

Run by the official ingestor image (Ticket #45) when a customer triggers a
``helm install tracebloc/ingestor -f ingest.yaml`` (Ticket client#86). The
flow:

    1. Read ``INGEST_CONFIG`` env (path to YAML mounted by the Helm subchart).
    2. Parse + validate against ``schema/ingest.v1.json``. Fail fast with
       a single multi-line error listing every violation by JSON-pointer
       path. No DB / network I/O happens before validation passes.
    3. Resolve convention defaults via ``conventions.resolve`` (pure).
    4. Bridge to the legacy env-var path-resolution layer in
       ``file_transfer.py`` by setting ``SRC_PATH`` / ``TABLE_NAME`` /
       ``LABEL_FILE`` from the resolved config. Direct refactor of
       ``file_transfer.py`` to take paths via parameters is a follow-up;
       env-var injection is the minimal bridge for v1.
    5. Construct ``Config``, ``Database``, ``APIClient``. ``APIClient``
       triggers ``Config.validate()`` which fails fast on missing auth
       (per #43).
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
from typing import Any, Dict, Iterable, List, NoReturn

import yaml
from jsonschema import Draft7Validator, ValidationError

from ..api.client import APIClient
from ..config import Config
from ..database import Database
from ..ingestors import CSVIngestor, JSONIngestor
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

def main(argv: List[str] | None = None) -> int:  # pragma: no cover - thin shell
    """Entrypoint registered as the ``tracebloc-ingest`` console script.

    Returns the process exit code. A non-zero return is converted to
    ``sys.exit`` by the console-script wrapper; using a return value (rather
    than raising) keeps the function testable from inside pytest.
    """
    config_path = os.environ.get("INGEST_CONFIG")
    if not config_path:
        return _fail(
            "INGEST_CONFIG env var not set. The official image expects the "
            "Helm subchart (client#86) to mount the ingest.yaml and set "
            "INGEST_CONFIG to its path."
        )

    raw_path = Path(config_path)
    if not raw_path.is_file():
        return _fail(f"INGEST_CONFIG points to {config_path} which does not exist.")

    try:
        raw_config = yaml.safe_load(raw_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        return _fail(f"ingest.yaml is not valid YAML:\n  {e}")

    if not isinstance(raw_config, dict):
        return _fail(
            "ingest.yaml must be a mapping at the top level "
            "(apiVersion / kind / category / ...)."
        )

    errors = list(_validate(raw_config))
    if errors:
        return _fail(
            "ingest.yaml validation failed:\n" + _format_errors(errors)
        )

    resolved = resolve(raw_config)
    _set_legacy_env_vars(resolved)

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

    config = Config()
    setup_logging(config)

    database = Database(config)
    api_client = APIClient(config)  # triggers config.validate() per #43

    ingestor = _build_ingestor(database, api_client, resolved)

    with ingestor:
        failed = ingestor.ingest(resolved.source_path, batch_size=config.BATCH_SIZE)
        if failed:
            logger.warning(
                "%d record(s) failed during ingestion; see logs for details.",
                len(failed),
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
    return sorted(validator.iter_errors(raw_config), key=lambda e: list(e.absolute_path))


def _format_errors(errors: List[ValidationError]) -> str:
    """Format errors as ``<json-pointer>: <message>``, one per line.

    Real line numbers (per the ticket) require a YAML loader that preserves
    position info. v1.1 follow-up — for now, the JSON-pointer path is
    enough to grep the customer's YAML.
    """
    lines = []
    for e in errors:
        path = ".".join(str(p) for p in e.absolute_path) or "<root>"
        lines.append(f"  {path}: {e.message}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Bridging — the legacy env-var path layer
# ---------------------------------------------------------------------------

def _set_legacy_env_vars(resolved: ResolvedConfig) -> None:
    """Bridge the resolved YAML config into the legacy env-var path layer
    in ``file_transfer.py``.

    The path-resolution layer reads ``SRC_PATH`` / ``TABLE_NAME`` /
    ``LABEL_FILE`` via :class:`Config`, whose env-driven fields are now
    lazy properties — they read ``os.environ`` on access. So this
    function just needs to set the env vars; module-level
    ``config = Config()`` snapshots scattered across validators and
    ingestors all pick up the new values at their next attribute read.

    ``SRC_PATH`` is derived from whichever sidecar directory is set, since
    ``file_transfer.py`` joins ``SRC_PATH/<subfolder>/<filename>`` for each
    category. The dominant convention is that all sidecar dirs share a
    parent (``/data/images/``, ``/data/annotations/``, etc.) — for non-
    standard layouts, customers use ``spec.sidecars[]`` (also deferred).
    """
    src_path = None
    src_path_source = (
        resolved.images or resolved.texts or resolved.masks
        or resolved.annotations or resolved.sequences
    )
    if src_path_source:
        src_path = os.path.dirname(src_path_source.rstrip("/"))

    os.environ["TABLE_NAME"] = resolved.table_name
    os.environ["LABEL_FILE"] = resolved.source_path
    if src_path:
        os.environ["SRC_PATH"] = src_path


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

def _fail(message: str) -> int:
    """Print to stderr and return non-zero. Logger may not be configured yet
    when validation fails (it depends on Config() which depends on env
    being set), so a plain stderr write is the most reliable channel."""
    print(message, file=sys.stderr)
    return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
