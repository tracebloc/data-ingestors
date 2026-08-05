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
