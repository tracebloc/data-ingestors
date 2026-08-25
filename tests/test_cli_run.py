"""End-to-end-but-mocked tests for the ``tracebloc-ingest`` entrypoint.

These tests exercise the full ``main()`` flow without spinning up MySQL or
hitting the network. ``Database`` and ``APIClient`` are patched at the
``cli.run`` import boundary so they construct cleanly without env vars.

Coverage:

- INGEST_CONFIG missing / pointing at a non-existent file → exit 2 with a
  clear stderr message and no DB / network calls.
- Malformed YAML → same.
- Schema validation failure → exit 2, message lists every error by
  json-pointer path, no DB / network calls.
- Happy path (CSV) → constructs ``CSVIngestor`` with the resolved kwargs,
  calls ``ingest()`` exactly once with the right path, exits 0.
- Happy path (JSON) → constructs ``JSONIngestor`` instead.
- ``spec.processors[]`` triggers the deferred-feature warning and the
  rest of the run continues.
- Legacy env vars (``SRC_PATH``, ``TABLE_NAME``, ``LABEL_FILE``) are set
  from the resolved config before ``Config()`` is constructed, so the
  framework's existing path-resolution layer keeps working unchanged.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_ROOT / "examples" / "yaml"


@pytest.fixture
def clean_env(monkeypatch):
    """Strip all the env vars the entrypoint reads / writes so each test
    starts from a known state. Use ``monkeypatch.setenv`` from inside the
    test for anything it actually wants to set."""
    for var in (
        "INGEST_CONFIG",
        "SRC_PATH",
        "TABLE_NAME",
        "LABEL_FILE",
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def mock_runtime():
    """Patch the heavy runtime — Database, APIClient, and Config — at the
    cli.run import boundary. Returns the mock objects so tests can assert
    on calls."""
    with patch("tracebloc_ingestor.cli.run.Config") as mock_config_cls, patch(
        "tracebloc_ingestor.cli.run.Database"
    ) as mock_db_cls, patch(
        "tracebloc_ingestor.cli.run.APIClient"
    ) as mock_api_cls, patch(
        "tracebloc_ingestor.cli.run.CSVIngestor"
    ) as mock_csv_cls, patch(
        "tracebloc_ingestor.cli.run.JSONIngestor"
    ) as mock_json_cls, patch(
        "tracebloc_ingestor.cli.run.setup_logging"
    ) as mock_setup_logging:
        mock_config = MagicMock()
        mock_config.BATCH_SIZE = 4000
        mock_config_cls.return_value = mock_config

        # Both ingestor classes share the same context-manager + ingest()
        # surface; tests just inspect which one was constructed.
        for cls_mock in (mock_csv_cls, mock_json_cls):
            instance = MagicMock()
            instance.__enter__ = MagicMock(return_value=instance)
            instance.__exit__ = MagicMock(return_value=False)
            instance.ingest = MagicMock(return_value=[])  # no failed records
            cls_mock.return_value = instance

        yield {
            "Config": mock_config_cls,
            "Database": mock_db_cls,
            "APIClient": mock_api_cls,
            "CSVIngestor": mock_csv_cls,
            "JSONIngestor": mock_json_cls,
            "setup_logging": mock_setup_logging,
        }


# ---------------------------------------------------------------------------
# Failure modes — must fail fast, no DB / network calls.
# ---------------------------------------------------------------------------


def test_missing_ingest_config_fails_fast(clean_env, mock_runtime, capsys):
    from tracebloc_ingestor.cli.run import main

    rc = main()
    assert rc == 2
    assert "INGEST_CONFIG" in capsys.readouterr().err
    mock_runtime["Database"].assert_not_called()
    mock_runtime["APIClient"].assert_not_called()


def test_nonexistent_ingest_config_fails_fast(
    clean_env, mock_runtime, monkeypatch, capsys
):
    monkeypatch.setenv("INGEST_CONFIG", "/nope/does/not/exist.yaml")
    from tracebloc_ingestor.cli.run import main

    rc = main()
    assert rc == 2
    assert "does not exist" in capsys.readouterr().err
    mock_runtime["Database"].assert_not_called()


def test_malformed_yaml_fails_fast(
    clean_env, mock_runtime, monkeypatch, capsys, tmp_path
):
    bad = tmp_path / "bad.yaml"
    bad.write_text("apiVersion: tracebloc.io/v1\n  kind: ::: invalid", encoding="utf-8")
    monkeypatch.setenv("INGEST_CONFIG", str(bad))

    from tracebloc_ingestor.cli.run import main

    rc = main()
    assert rc == 2
    assert "not valid YAML" in capsys.readouterr().err
    mock_runtime["Database"].assert_not_called()


def test_schema_violation_fails_fast(
    clean_env, mock_runtime, monkeypatch, capsys, tmp_path
):
    """A config that passes YAML parsing but fails schema validation must
    exit before any DB/network call, listing the failures.

    Also pins that primitive checks ('X is a required property') keep
    jsonschema's clear default message — they must NOT be replaced by
    the schema's generic root description ("Declarative configuration
    for the tracebloc data ingestor..."), which would happen if the
    description-walker captured the root node's description (bugbot
    #254). Schemas only attach their description when an INNER node
    along the failing rule's path has one.
    """
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        # Missing `table`, `csv`, `images`, `label` — schema must reject.
        "apiVersion: tracebloc.io/v1\n"
        "kind: IngestConfig\n"
        "category: image_classification\n"
        "intent: train\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("INGEST_CONFIG", str(bad))

    from tracebloc_ingestor.cli.run import main

    rc = main()

    err = capsys.readouterr().err
    assert rc == 2
    assert "validation failed" in err
    # Should mention at least one of the missing fields.
    assert "table" in err or "csv" in err or "images" in err or "label" in err
    # The schema's GENERIC root description must NOT leak into primitive
    # errors — bugbot #254 caught the walker capturing the root.
    assert "Declarative configuration" not in err
    # Primitive 'required property' messages must reach the user
    # unmodified, NOT be replaced with a description + (rule:) line.
    assert "is a required property" in err
    mock_runtime["Database"].assert_not_called()
    mock_runtime["APIClient"].assert_not_called()


def test_schema_violation_surfaces_description_not_raw_mechanic(
    clean_env, mock_runtime, monkeypatch, capsys, tmp_path
):
    """Setting ``label:`` for masked_language_modeling violates the
    schema's allOf rule. The user-visible error must surface the rule
    AUTHOR'S description (which names the self-supervised category, the
    fix, and #213's history) — NOT just the raw JSON-schema mechanic
    "<{full yaml dump}> should not be valid under {'required':
    ['label']}", which is technically correct but unreadable: it dumps
    the entire submission as a Python dict and uses JSON-schema
    vocabulary the customer didn't write.

    Verified end-to-end against v0.3.10-rc1: a user who copy-pasted
    `label: label` from another category's yaml saw only the raw
    mechanic and had no way to know that MLM is self-supervised.
    """
    bad = tmp_path / "mlm_with_label.yaml"
    bad.write_text(
        "apiVersion: tracebloc.io/v1\n"
        "kind: IngestConfig\n"
        "category: masked_language_modeling\n"
        "table: ssh_test\n"
        "intent: test\n"
        "csv: /tmp/ignored.csv\n"
        "sequences: /tmp/ignored/\n"
        "label: label\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("INGEST_CONFIG", str(bad))

    from tracebloc_ingestor.cli.run import main

    rc = main()

    err = capsys.readouterr().err
    assert rc == 2
    # The schema's description is what the user needs to see — it names
    # the actual rationale and the fix.
    assert "Self-supervised" in err
    assert "MUST NOT set `label`" in err
    # Don't drop the mechanic entirely — it's still on a follow-up line
    # for power-user debugging.
    assert "rule:" in err
    # Should NOT dump the entire YAML body as a Python dict in the
    # headline (the old behavior).
    headline = err.split("\n", 1)[0]
    assert "{'apiVersion'" not in headline  # raw dict dump suppressed
    mock_runtime["Database"].assert_not_called()
    mock_runtime["APIClient"].assert_not_called()


def test_describe_from_schema_path_skips_root_description():
    """Unit-test for the description walker:

    The schema's ROOT description ("Declarative configuration for the
    tracebloc data ingestor...") is a generic blurb about the schema
    as a whole, not about any specific rule. It must NOT be returned
    for primitive errors whose path doesn't dig into an `allOf`/`oneOf`
    branch — otherwise EVERY validation error gets blanketed with the
    same generic text and jsonschema's clear messages get hidden
    (bugbot #254).

    A primitive error like 'required' has schema_path == ('required',),
    which walks into the schema's `required` list — a list with no
    `description`. The walker must return None for that case.

    The MLM+label case has schema_path == ('allOf', N, 'then', 'not'),
    which DOES walk into a dict with the rule-specific description.
    The walker must return THAT description.
    """
    from tracebloc_ingestor.cli.run import _describe_from_schema_path

    schema = {
        "description": "Generic root blurb that should NEVER be returned",
        "properties": {"x": {"type": "integer"}},
        "required": ["x"],
        "allOf": [
            {
                "description": "Rule-specific prose with rationale and fix.",
                "if": {"properties": {"category": {"const": "abc"}}},
                "then": {"not": {"required": ["label"]}},
            }
        ],
    }
    # Primitive 'required' error path → no inner description → None
    assert _describe_from_schema_path(schema, ["required"]) is None
    # Generic top-level type error path → no inner description → None
    assert _describe_from_schema_path(schema, ["properties", "x", "type"]) is None
    # AllOf branch path → inner description is returned
    assert (
        _describe_from_schema_path(schema, ["allOf", 0, "then", "not"])
        == "Rule-specific prose with rationale and fix."
    )


# ---------------------------------------------------------------------------
# Happy path — CSV
# ---------------------------------------------------------------------------


def test_csv_happy_path(clean_env, mock_runtime, monkeypatch):
    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml"))

    from tracebloc_ingestor.cli.run import main

    rc = main()

    assert rc == 0
    # CSVIngestor was constructed; JSONIngestor was not.
    assert mock_runtime["CSVIngestor"].call_count == 1
    assert mock_runtime["JSONIngestor"].call_count == 0

    # ingest() called exactly once with the source path from the YAML.
    csv_instance = mock_runtime["CSVIngestor"].return_value
    csv_instance.ingest.assert_called_once()
    args, kwargs = csv_instance.ingest.call_args
    assert args[0] == "/data/shared/chest-xrays/labels.csv"
    assert kwargs.get("batch_size") == 4000


def test_csv_kwargs_match_resolved_config(clean_env, mock_runtime, monkeypatch):
    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml"))
    from tracebloc_ingestor.cli.run import main

    main()

    _, kwargs = mock_runtime["CSVIngestor"].call_args
    assert kwargs["table_name"] == "chest_xrays_train"
    assert kwargs["intent"] == "train"
    assert kwargs["category"] == "image_classification"
    assert kwargs["data_format"] == "image"
    assert kwargs["label_column"] == "image_label"
    assert kwargs["unique_id_column"] is None  # UUID generation, the default
    # image_classification isn't a time-series category, so no time_column is
    # bridged into file_options (#441).
    assert "time_column" not in kwargs["file_options"]
    # File / CSV options carry the conventional defaults — now aligned with
    # the bundled image_classification onboarding sample (256×256 .jpeg, #198).
    assert kwargs["file_options"]["target_size"] == [256, 256]
    assert kwargs["file_options"]["extension"] == ".jpeg"
    assert kwargs["csv_options"]["chunk_size"] == 1000


# ---------------------------------------------------------------------------
# Happy path — JSON
# ---------------------------------------------------------------------------


def test_json_happy_path(clean_env, mock_runtime, monkeypatch, tmp_path):
    """No JSON example ships in examples/yaml/; build one inline."""
    cfg = tmp_path / "json_config.yaml"
    cfg.write_text(
        "apiVersion: tracebloc.io/v1\n"
        "kind: IngestConfig\n"
        "category: tabular_classification\n"
        "table: events_train\n"
        "intent: train\n"
        "json: /data/events.json\n"
        "schema:\n"
        "  event_type: VARCHAR(64)\n"
        "  outcome: VARCHAR(8)\n"
        "label: outcome\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("INGEST_CONFIG", str(cfg))

    from tracebloc_ingestor.cli.run import main

    rc = main()

    assert rc == 0
    assert mock_runtime["CSVIngestor"].call_count == 0
    assert mock_runtime["JSONIngestor"].call_count == 1
    json_instance = mock_runtime["JSONIngestor"].return_value
    json_instance.ingest.assert_called_once_with("/data/events.json", batch_size=4000)


def test_json_receives_file_options_for_time_to_event(
    clean_env, mock_runtime, monkeypatch, tmp_path
):
    """JSON source + time_to_event_prediction must propagate `time_column`
    via file_options so map_validators picks up the right TimeToEventValidator
    config. Regression guard for the dispatch-layer drop."""
    cfg = tmp_path / "tte.yaml"
    cfg.write_text(
        "apiVersion: tracebloc.io/v1\n"
        "kind: IngestConfig\n"
        "category: time_to_event_prediction\n"
        "table: tte_train\n"
        "intent: train\n"
        "json: /data/events.json\n"
        "time_column: event_time\n"
        "schema:\n"
        "  event_time: DATETIME\n"
        "  duration: FLOAT\n"
        "label:\n"
        "  column: duration\n"
        "  policy: bucket\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("INGEST_CONFIG", str(cfg))

    from tracebloc_ingestor.cli.run import main

    main()

    _, kwargs = mock_runtime["JSONIngestor"].call_args
    assert kwargs["file_options"]["time_column"] == "event_time"


def test_csv_time_series_forecasting_threads_time_column(
    clean_env, mock_runtime, monkeypatch, tmp_path
):
    """A top-level `time_column` is bridged into file_options for the
    time-series family so validate_data can preflight it and reject a value the
    fixed-timestamp TSF/TSC categories never honor (#441)."""
    cfg = tmp_path / "tsf.yaml"
    cfg.write_text(
        "apiVersion: tracebloc.io/v1\n"
        "kind: IngestConfig\n"
        "category: time_series_forecasting\n"
        "table: tsf_train\n"
        "intent: train\n"
        "csv: /data/tsf.csv\n"
        "time_column: nonexistent_col\n"
        "schema:\n"
        "  timestamp: TIMESTAMP\n"
        "  demand_mw: FLOAT\n"
        "label:\n"
        "  column: demand_mw\n"
        "  policy: bucket\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("INGEST_CONFIG", str(cfg))

    from tracebloc_ingestor.cli.run import main

    main()

    _, kwargs = mock_runtime["CSVIngestor"].call_args
    assert kwargs["file_options"]["time_column"] == "nonexistent_col"


# ---------------------------------------------------------------------------
# Deferred-feature warning for processors
# ---------------------------------------------------------------------------


def test_processors_trigger_warning_but_run_continues(
    clean_env, mock_runtime, monkeypatch, caplog
):
    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "custom_processor.yaml"))
    with caplog.at_level(logging.WARNING, logger="tracebloc_ingestor.cli.run"):
        from tracebloc_ingestor.cli.run import main

        rc = main()

    assert rc == 0
    # Run continued (CSVIngestor was constructed and ingest() was called).
    assert mock_runtime["CSVIngestor"].call_count == 1
    # But the warning fired.
    assert any(
        "spec.processors" in r.message and "client#86" in r.message
        for r in caplog.records
    )


def test_validators_override_triggers_warning(
    clean_env, mock_runtime, monkeypatch, caplog, tmp_path
):
    """spec.validators is schema-accepted but the runtime path isn't built
    yet — the entrypoint must warn instead of silently dropping it."""
    cfg = tmp_path / "ingest.yaml"
    cfg.write_text(
        "apiVersion: tracebloc.io/v1\n"
        "kind: IngestConfig\n"
        "category: image_classification\n"
        "table: t\n"
        "intent: train\n"
        "csv: /data/labels.csv\n"
        "images: /data/images/\n"
        "label: image_label\n"
        "spec:\n"
        "  validators: [FileTypeValidator, TableNameValidator]\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("INGEST_CONFIG", str(cfg))

    with caplog.at_level(logging.WARNING, logger="tracebloc_ingestor.cli.run"):
        from tracebloc_ingestor.cli.run import main

        rc = main()

    assert rc == 0
    assert mock_runtime["CSVIngestor"].call_count == 1
    assert any("spec.validators" in r.message for r in caplog.records)


def test_sidecars_triggers_warning(
    clean_env, mock_runtime, monkeypatch, caplog, tmp_path
):
    """spec.sidecars is schema-accepted but the runtime path isn't built
    yet — the entrypoint must warn instead of silently dropping it."""
    cfg = tmp_path / "ingest.yaml"
    cfg.write_text(
        "apiVersion: tracebloc.io/v1\n"
        "kind: IngestConfig\n"
        "category: image_classification\n"
        "table: t\n"
        "intent: train\n"
        "csv: /data/labels.csv\n"
        "images: /data/images/\n"
        "label: image_label\n"
        "spec:\n"
        "  sidecars:\n"
        "    - {column: filename, source: /other/images/}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("INGEST_CONFIG", str(cfg))

    with caplog.at_level(logging.WARNING, logger="tracebloc_ingestor.cli.run"):
        from tracebloc_ingestor.cli.run import main

        rc = main()

    assert rc == 0
    assert mock_runtime["CSVIngestor"].call_count == 1
    assert any("spec.sidecars" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Config by construction (P4c — the env-var bridge was removed)
# ---------------------------------------------------------------------------


def test_resolved_config_built_with_overrides(clean_env, mock_runtime, monkeypatch):
    """P4c: the entrypoint builds ONE Config with the resolved paths as explicit
    per-instance overrides and injects it into Database (and thus the ingestor
    / validators / file_transfer) — config by construction, not via env."""
    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml"))

    from tracebloc_ingestor.cli.run import main

    main()

    # Config() was constructed with the resolved values as overrides.
    _, kwargs = mock_runtime["Config"].call_args
    assert kwargs["TABLE_NAME"] == "chest_xrays_train"
    assert kwargs["LABEL_FILE"] == "/data/shared/chest-xrays/labels.csv"
    # SRC_PATH = parent of the `images:` dir, since file_transfer joins
    # SRC_PATH/images/<filename>.
    assert kwargs["SRC_PATH"] == "/data/shared/chest-xrays"
    # …and that exact Config instance is injected into Database (-> ingestor).
    mock_runtime["Database"].assert_called_once_with(
        mock_runtime["Config"].return_value
    )


def test_entrypoint_does_not_mutate_environ(clean_env, mock_runtime, monkeypatch):
    """The env-var bridge (``_set_legacy_env_vars``) is gone: ``main`` resolves
    paths into a Config, never into ``os.environ``. Pre-poisoned env stays
    untouched — no spooky action at a distance through the process
    environment (P4c). (Previously the bridge overwrote these.)"""
    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml"))
    # Pre-poison: the old bridge would have overwritten these from the YAML.
    monkeypatch.setenv("TABLE_NAME", "STALE_TABLE")
    monkeypatch.setenv("LABEL_FILE", "/stale/labels.csv")
    monkeypatch.setenv("SRC_PATH", "/stale/src")

    from tracebloc_ingestor.cli.run import main

    main()

    # main() did not write the resolved paths into os.environ.
    assert os.environ["TABLE_NAME"] == "STALE_TABLE"
    assert os.environ["LABEL_FILE"] == "/stale/labels.csv"
    assert os.environ["SRC_PATH"] == "/stale/src"


# ---------------------------------------------------------------------------
# Failed-records non-zero exit
# ---------------------------------------------------------------------------


def test_failed_records_yield_nonzero_exit(clean_env, mock_runtime, monkeypatch):
    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml"))
    csv_instance = mock_runtime["CSVIngestor"].return_value
    csv_instance.ingest.return_value = [{"image_id": "broken"}]  # one failure

    from tracebloc_ingestor.cli.run import main

    rc = main()

    assert rc == 1  # not 0, not 2 (which is reserved for fail-fast)


# ---------------------------------------------------------------------------
# Contract telemetry (RFC-BACKEND-1872 D2)
#
# Every terminal path of the entrypoint reports exactly one contract event, and
# says WHY in a closed, low-cardinality `error.type`. Driven through the real
# paths — not through a stubbed `_run` — because the thing worth pinning is that
# each concrete failure keeps its own classification all the way out.
# ---------------------------------------------------------------------------


@pytest.fixture
def telemetry_records(caplog):
    """The contract records a driven run produced, in order."""
    import tracebloc_telemetry

    from tracebloc_ingestor import telemetry as job_telemetry

    tracebloc_telemetry.reset()
    job_telemetry.reset()
    caplog.set_level(logging.DEBUG, logger="tracebloc.telemetry")
    yield lambda: [r.telemetry for r in caplog.records if hasattr(r, "telemetry")]
    tracebloc_telemetry.reset()
    job_telemetry.reset()


def _attribute_text(record):
    """Every attribute VALUE of a record, as text, unescaped.

    Not ``json.dumps``: it escapes non-ASCII, so a leak assertion written
    against a marker containing ``ü`` passes while the value sits in the
    record. Found by mutating ``safe_stacktrace`` and watching three
    end-to-end tests stay green.
    """
    return "\n".join(f"{key}={value!r}" for key, value in record.items())


def _sole_terminal(records):
    from tracebloc_ingestor import telemetry as job_telemetry

    terminals = [r for r in records if r["event.name"] in job_telemetry.TERMINAL_EVENTS]
    assert len(terminals) == 1, [r["event.name"] for r in records]
    return terminals[0]


def _path_config_missing(monkeypatch, tmp_path, mock_runtime):
    """INGEST_CONFIG is not set at all (clean_env already removed it)."""


def _path_config_not_found(monkeypatch, tmp_path, mock_runtime):
    monkeypatch.setenv("INGEST_CONFIG", str(tmp_path / "absent.yaml"))


def _path_config_unparseable(monkeypatch, tmp_path, mock_runtime):
    bad = tmp_path / "bad.yaml"
    bad.write_text("apiVersion: tracebloc.io/v1\n  kind: ::: invalid", encoding="utf-8")
    monkeypatch.setenv("INGEST_CONFIG", str(bad))


def _path_config_not_a_mapping(monkeypatch, tmp_path, mock_runtime):
    bad = tmp_path / "list.yaml"
    bad.write_text("- apiVersion: tracebloc.io/v1\n", encoding="utf-8")
    monkeypatch.setenv("INGEST_CONFIG", str(bad))


def _path_config_invalid(monkeypatch, tmp_path, mock_runtime):
    bad = tmp_path / "incomplete.yaml"
    bad.write_text(
        "apiVersion: tracebloc.io/v1\n"
        "kind: IngestConfig\n"
        "category: image_classification\n"
        "intent: train\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("INGEST_CONFIG", str(bad))


def _path_ingestion_failed(monkeypatch, tmp_path, mock_runtime):
    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml"))
    mock_runtime["CSVIngestor"].return_value.ingest.side_effect = RuntimeError(
        "the database went away"
    )


def _path_records_failed(monkeypatch, tmp_path, mock_runtime):
    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml"))
    mock_runtime["CSVIngestor"].return_value.ingest.return_value = [
        {"image_id": "broken"},
        {"image_id": "also-broken"},
    ]


def _path_succeeded(monkeypatch, tmp_path, mock_runtime):
    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml"))


#: (setup, exit code, expected error.type or None for the clean run). Written
#: independently of `telemetry.ERROR_TYPES`, so the closure check below compares
#: two sources rather than a list with itself.
_TERMINAL_PATHS = [
    (_path_config_missing, 2, "config_missing"),
    (_path_config_not_found, 2, "config_not_found"),
    (_path_config_unparseable, 2, "config_unparseable"),
    (_path_config_not_a_mapping, 2, "config_not_a_mapping"),
    (_path_config_invalid, 2, "config_invalid"),
    (_path_ingestion_failed, 1, "ingestion_failed"),
    (_path_records_failed, 1, "records_failed"),
    (_path_succeeded, 0, None),
]


@pytest.mark.parametrize(
    "setup, expected_code, expected_error_type",
    _TERMINAL_PATHS,
    ids=[case[0].__name__ for case in _TERMINAL_PATHS],
)
def test_every_terminal_path_reports_one_classified_event(
    setup,
    expected_code,
    expected_error_type,
    clean_env,
    mock_runtime,
    monkeypatch,
    tmp_path,
    telemetry_records,
    capsys,
):
    from tracebloc_ingestor import telemetry as job_telemetry
    from tracebloc_ingestor.cli.run import main

    setup(monkeypatch, tmp_path, mock_runtime)
    rc = main()
    capsys.readouterr()

    assert rc == expected_code
    records = telemetry_records()
    assert records[0]["event.name"] == job_telemetry.EVENT_JOB_STARTED

    terminal = _sole_terminal(records)
    if expected_error_type is None:
        assert terminal["event.name"] == job_telemetry.EVENT_JOB_SUCCEEDED
        assert "error.type" not in terminal
    else:
        assert terminal["error.type"] == expected_error_type
        assert terminal["error.type"] in job_telemetry.ERROR_TYPES


def test_the_driven_paths_cover_the_whole_error_vocabulary():
    """Two sources compared, not a list against itself.

    ``_TERMINAL_PATHS`` above is a hand-written list of the entrypoint's real
    endings; ``ERROR_TYPES`` is the producer's declared vocabulary. Anything in
    the vocabulary that no driven path reaches must be a classification the
    FUNNEL produces, and those two are named here explicitly — so a new
    classification that nothing can emit fails this test rather than sitting
    unqueryable in a dashboard nobody can populate.
    """
    from tracebloc_ingestor import telemetry as job_telemetry

    driven = {case[2] for case in _TERMINAL_PATHS if case[2] is not None}
    funnel_only = {
        job_telemetry.ERROR_UNHANDLED_EXCEPTION,
        job_telemetry.ERROR_UNCLASSIFIED,
    }
    assert driven | funnel_only == job_telemetry.ERROR_TYPES
    assert not driven & funnel_only


# ---------------------------------------------------------------------------
# A signal arriving after the work is done (backend#2435)
# ---------------------------------------------------------------------------


@pytest.fixture
def restore_sigterm():
    """Put SIGTERM's disposition back after a test that drives ``main()``.

    ``main`` installs a process-wide handler and nothing uninstalls it, so a
    test that then delivers a REAL signal would leave the handler standing for
    everything that runs afterwards in this process.
    """
    import signal

    previous = signal.getsignal(signal.SIGTERM)
    yield
    signal.signal(signal.SIGTERM, previous)


def _sigterm_now(*_args, **_kwargs):
    """Deliver a real SIGTERM to this process, the way the platform does.

    A real signal rather than a call to the handler: the point of these two
    tests is WHERE the interpreter happens to be when the handler runs, and
    calling it directly would choose that for it.
    """
    import os
    import signal

    os.kill(os.getpid(), signal.SIGTERM)


def test_a_signal_during_teardown_reports_the_success_it_already_earned(
    clean_env,
    mock_runtime,
    monkeypatch,
    tmp_path,
    telemetry_records,
    capsys,
    restore_sigterm,
):
    """The reported ending of an evicted-but-finished run (backend#2435).

    Every failure path classifies itself where it happens; success was reported
    only by the funnel, after the run had unwound. ``_terminal`` is
    first-writer-wins, so a SIGTERM in between claimed the terminal slot as
    ``cancelled`` — for a dataset that was committed and registered. The
    success went uncounted, and an operator reading the board saw an ingest
    that looked aborted and was not.

    The signal lands in the ingestor's context-manager exit, which is inside
    that window and is real: the entrypoint's ``mark_durable`` runs in the
    ``with`` body, before this. Driven through the entrypoint's own handler and
    a real ``kill``, so removing ``telemetry.mark_durable()`` from ``_run``
    reddens this test rather than being invisible to it.
    """
    import signal as signal_module

    from tracebloc_ingestor import telemetry as job_telemetry
    from tracebloc_ingestor.cli.run import main

    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml"))
    ingestor = mock_runtime["CSVIngestor"].return_value
    ingestor.ingest.return_value = []  # a clean load
    ingestor.__exit__ = MagicMock(side_effect=_sigterm_now)

    with pytest.raises(SystemExit) as exit_info:
        main()
    capsys.readouterr()

    # ANCHOR: the signal really was delivered and really did unwind the run, so
    # the assertion below is not passing because nothing happened.
    assert exit_info.value.code == 128 + signal_module.SIGTERM
    assert ingestor.__exit__.called

    terminal = _sole_terminal(telemetry_records())
    assert terminal["event.name"] == job_telemetry.EVENT_JOB_SUCCEEDED
    assert "error.type" not in terminal


def test_a_signal_before_the_work_is_done_still_reports_a_cancellation(
    clean_env,
    mock_runtime,
    monkeypatch,
    tmp_path,
    telemetry_records,
    capsys,
    restore_sigterm,
):
    """The other direction, and the reason the fix is a decision and not a
    deletion.

    Without this, the test above could be satisfied by never reporting a
    cancellation at all — which would trade an undercount of successes for an
    undercount of evictions and lose the ending the SIGTERM handler exists to
    record. Same harness, same real signal; the only difference is that it
    arrives while the load is still in flight, so nothing has been earned yet.
    """
    import signal as signal_module

    from tracebloc_ingestor import telemetry as job_telemetry
    from tracebloc_ingestor.cli.run import main

    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml"))
    ingestor = mock_runtime["CSVIngestor"].return_value
    ingestor.ingest = MagicMock(side_effect=_sigterm_now)

    with pytest.raises(SystemExit) as exit_info:
        main()
    capsys.readouterr()

    assert exit_info.value.code == 128 + signal_module.SIGTERM
    terminal = _sole_terminal(telemetry_records())
    assert terminal["event.name"] == job_telemetry.EVENT_JOB_CANCELLED


def test_a_failed_ingestion_reports_frames_but_never_the_exception_message(
    clean_env, mock_runtime, monkeypatch, tmp_path, telemetry_records, capsys
):
    """The end-to-end shape of the redaction rule, on the real failure path."""
    secret = "patient-4711-Müller"
    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml"))
    # side_effect, so the raising frame belongs to unittest.mock and this file's
    # source line cannot be what carries the value into the traceback.
    mock_runtime["CSVIngestor"].return_value.ingest.side_effect = ValueError(
        f"Incorrect value {secret!r} for column x"
    )

    from tracebloc_ingestor.cli.run import main

    assert main() == 1
    # the operator's own console still gets the detail; it never leaves the
    # cluster, and pinning it here keeps this test about the RECORD.
    assert secret in capsys.readouterr().err

    terminal = _sole_terminal(telemetry_records())
    assert terminal["error.type"] == "ingestion_failed"
    assert terminal["exception.type"] == "ValueError"
    assert terminal["exception.stacktrace"]
    assert "exception.message" not in terminal
    assert secret not in _attribute_text(terminal)


def test_the_failed_record_count_is_sent_but_never_the_records(
    clean_env, mock_runtime, monkeypatch, tmp_path, telemetry_records
):
    monkeypatch.setenv("INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml"))
    mock_runtime["CSVIngestor"].return_value.ingest.return_value = [
        {"image_id": "patient-4711"},
        {"image_id": "patient-4712"},
    ]

    from tracebloc_ingestor.cli.run import main

    assert main() == 1

    terminal = _sole_terminal(telemetry_records())
    assert terminal["tracebloc.ingest.failed_records"] == 2
    assert "patient-4711" not in _attribute_text(terminal)
