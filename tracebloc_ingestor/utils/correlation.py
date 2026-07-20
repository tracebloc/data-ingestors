"""End-to-end ingest correlation id (backend#1028 item 3).

One id threads a single ingest run across every layer. The CLI generates an
idempotency key per ``tracebloc data ingest`` invocation; jobs-manager
(client-runtime) derives the Job name from that key, labels every spawned
resource with it (``tracebloc.io/ingestion-run``), and stamps it into the
ingestor container as the ``TRACEBLOC_INGEST_CORRELATION_ID`` env var. This
module is where the ingestor picks the thread up: the resolved value is
logged next to the per-process ``ingestor_id`` and rides the registration
payload's ``meta_data`` to the backend — so CLI output, Job name/labels,
ingestor logs, MySQL rows (via the logged ingestor_id) and the backend
dataset row are all reachable by grepping one string.

The env var is optional everywhere. Absent (direct/manual runs, template
runs, older jobs-managers) → ``None`` and behaviour is exactly as before.
Present but malformed → a warning, then ``None`` — observability must never
fail an ingest.

Deliberately **alongside**, not instead of, ``ingestor_id``: a Job retry
(``restartPolicy: OnFailure``) reruns the container with the same env, and
row scoping (label counts, the #227 compensating delete) must stay
per-process, or a hard-killed first attempt's orphan rows would be counted
into — and registered with — the retry's dataset.
"""

import logging
import os
import re
from typing import Mapping, Optional

logger = logging.getLogger(__name__)

# The env var jobs-manager sets on the spawned ingestor Job
# (client-runtime's submit_ingestion_run.build_job_spec extra_env).
CORRELATION_ID_ENV = "TRACEBLOC_INGEST_CORRELATION_ID"

# jobs-manager caps idempotency keys at 64 chars (VARCHAR(64) primary key in
# its ingestion_runs table); the charset mirrors its Kubernetes-label-safe
# alphabet (client-runtime's ``_safe_label_value``) so the same string is
# valid in Job labels, log greps, and JSON. The CLI's generated keys are
# 32 lowercase hex chars and always match.
_CORRELATION_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def resolve_correlation_id(env: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """Read and validate ``TRACEBLOC_INGEST_CORRELATION_ID``.

    Returns the validated id, or ``None`` when the var is unset/blank (the
    normal case outside jobs-manager-spawned Jobs). A present-but-malformed
    value also returns ``None`` — after a WARNING naming the rejected value,
    so a mangled key surfaces in the Job log instead of silently seeding a
    bogus thread (default LOG_LEVEL is WARNING, so this line is visible).

    Args:
        env: Environment mapping to read from; defaults to ``os.environ``.
            Injectable for tests.
    """
    source = os.environ if env is None else env
    raw = (source.get(CORRELATION_ID_ENV) or "").strip()
    if not raw:
        return None
    if not _CORRELATION_ID_RE.match(raw):
        logger.warning(
            "Ignoring %s=%r: expected 1-64 chars of [A-Za-z0-9._-] "
            "(the CLI's idempotency-key format). Proceeding without a "
            "correlation id.",
            CORRELATION_ID_ENV,
            raw,
        )
        return None
    return raw
