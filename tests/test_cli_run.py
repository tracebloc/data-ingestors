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
    with patch("tracebloc_ingestor.cli.run.Config") as mock_config_cls, \
         patch("tracebloc_ingestor.cli.run.Database") as mock_db_cls, \
         patch("tracebloc_ingestor.cli.run.APIClient") as mock_api_cls, \
         patch("tracebloc_ingestor.cli.run.CSVIngestor") as mock_csv_cls, \
         patch("tracebloc_ingestor.cli.run.JSONIngestor") as mock_json_cls, \
         patch("tracebloc_ingestor.cli.run.setup_logging") as mock_setup_logging:
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


def test_nonexistent_ingest_config_fails_fast(clean_env, mock_runtime, monkeypatch, capsys):
    monkeypatch.setenv("INGEST_CONFIG", "/nope/does/not/exist.yaml")
    from tracebloc_ingestor.cli.run import main
    rc = main()
    assert rc == 2
    assert "does not exist" in capsys.readouterr().err
    mock_runtime["Database"].assert_not_called()


def test_malformed_yaml_fails_fast(clean_env, mock_runtime, monkeypatch, capsys, tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("apiVersion: tracebloc.io/v1\n  kind: ::: invalid", encoding="utf-8")
    monkeypatch.setenv("INGEST_CONFIG", str(bad))

    from tracebloc_ingestor.cli.run import main
    rc = main()
    assert rc == 2
    assert "not valid YAML" in capsys.readouterr().err
    mock_runtime["Database"].assert_not_called()


def test_schema_violation_fails_fast(clean_env, mock_runtime, monkeypatch, capsys, tmp_path):
    """A config that passes YAML parsing but fails schema validation must
    exit before any DB/network call, listing the failures."""
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
    mock_runtime["Database"].assert_not_called()
    mock_runtime["APIClient"].assert_not_called()


# ---------------------------------------------------------------------------
# Happy path — CSV
# ---------------------------------------------------------------------------

def test_csv_happy_path(clean_env, mock_runtime, monkeypatch):
    monkeypatch.setenv(
        "INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml")
    )

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
    assert args[0] == "/data/labels.csv"
    assert kwargs.get("batch_size") == 4000


def test_csv_kwargs_match_resolved_config(clean_env, mock_runtime, monkeypatch):
    monkeypatch.setenv(
        "INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml")
    )
    from tracebloc_ingestor.cli.run import main
    main()

    _, kwargs = mock_runtime["CSVIngestor"].call_args
    assert kwargs["table_name"] == "chest_xrays_train"
    assert kwargs["intent"] == "train"
    assert kwargs["category"] == "image_classification"
    assert kwargs["data_format"] == "image"
    assert kwargs["label_column"] == "image_label"
    assert kwargs["unique_id_column"] is None  # UUID generation, the default
    # File / CSV options carry the conventional defaults.
    assert kwargs["file_options"]["target_size"] == [512, 512]
    assert kwargs["file_options"]["extension"] == ".jpg"
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


# ---------------------------------------------------------------------------
# Deferred-feature warning for processors
# ---------------------------------------------------------------------------

def test_processors_trigger_warning_but_run_continues(
    clean_env, mock_runtime, monkeypatch, caplog
):
    monkeypatch.setenv(
        "INGEST_CONFIG", str(EXAMPLES_DIR / "custom_processor.yaml")
    )
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
# Legacy env-var bridge
# ---------------------------------------------------------------------------

def test_legacy_env_vars_set_before_config_construction(
    clean_env, mock_runtime, monkeypatch
):
    """The entrypoint must set SRC_PATH / TABLE_NAME / LABEL_FILE so any
    downstream code that reads env lazily picks them up."""
    monkeypatch.setenv(
        "INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml")
    )

    # Capture env state at the moment Config() is invoked.
    captured_env = {}
    def _capture_env(*args, **kwargs):
        captured_env["TABLE_NAME"] = os.environ.get("TABLE_NAME")
        captured_env["LABEL_FILE"] = os.environ.get("LABEL_FILE")
        captured_env["SRC_PATH"] = os.environ.get("SRC_PATH")
        return mock_runtime["Config"].return_value

    mock_runtime["Config"].side_effect = _capture_env

    from tracebloc_ingestor.cli.run import main
    main()

    assert captured_env["TABLE_NAME"] == "chest_xrays_train"
    assert captured_env["LABEL_FILE"] == "/data/labels.csv"
    # SRC_PATH = parent of `images:` dir, since file_transfer.py joins
    # SRC_PATH/images/<filename>.
    assert captured_env["SRC_PATH"] == "/data"


def test_file_transfer_config_patched_in_place(clean_env, mock_runtime, monkeypatch):
    """``file_transfer`` holds a module-level ``config = Config()`` captured
    at import time, before the entrypoint runs. Because ``Config`` is a
    dataclass whose ``os.getenv`` defaults are evaluated at class-definition
    time, neither setting env vars nor re-instantiating ``Config()`` would
    reach that captured instance. The bridge must mutate it in place."""
    monkeypatch.setenv(
        "INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml")
    )

    from tracebloc_ingestor import file_transfer

    # Poison the captured config with values the entrypoint should overwrite.
    monkeypatch.setattr(file_transfer.config, "TABLE_NAME", "STALE_TABLE")
    monkeypatch.setattr(file_transfer.config, "LABEL_FILE", "/stale/labels.csv")
    monkeypatch.setattr(file_transfer.config, "SRC_PATH", "/stale/src")
    monkeypatch.setattr(file_transfer.config, "DEST_PATH", "/stale/dest")
    storage_path = file_transfer.config.STORAGE_PATH

    from tracebloc_ingestor.cli.run import main
    main()

    assert file_transfer.config.TABLE_NAME == "chest_xrays_train"
    assert file_transfer.config.LABEL_FILE == "/data/labels.csv"
    assert file_transfer.config.SRC_PATH == "/data"
    # DEST_PATH is STORAGE_PATH/<table> — file_transfer joins onto it.
    assert file_transfer.config.DEST_PATH == os.path.join(storage_path, "chest_xrays_train")


# ---------------------------------------------------------------------------
# Failed-records non-zero exit
# ---------------------------------------------------------------------------

def test_failed_records_yield_nonzero_exit(clean_env, mock_runtime, monkeypatch):
    monkeypatch.setenv(
        "INGEST_CONFIG", str(EXAMPLES_DIR / "image_classification.yaml")
    )
    csv_instance = mock_runtime["CSVIngestor"].return_value
    csv_instance.ingest.return_value = [{"image_id": "broken"}]  # one failure

    from tracebloc_ingestor.cli.run import main
    rc = main()

    assert rc == 1  # not 0, not 2 (which is reserved for fail-fast)
