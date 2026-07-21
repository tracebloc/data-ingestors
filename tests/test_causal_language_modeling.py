"""Causal language modeling (CLM) — modality wiring, mirroring the MLM /
token_classification coverage.

CLM is the self-supervised, raw-text decoder modality added alongside MLM. It
shares MLM's self-supervised semantics (only a ``filename`` column, no label)
but stages from ``texts/`` (raw text — plain text or a ``prompt\\tcompletion``
SFT pair), not the pre-tokenized ``sequences/`` MLM uses. These tests pin the
three behaviors the modality must get right:

1. **CLI run** — a CLM ``ingest.yaml`` routes through ``cli.run.main`` to a
   CSVIngestor with the resolved kwargs; a CLM config that (wrongly) sets
   ``label:`` is rejected at the entrypoint (self-supervised, #213).
2. **Validation boundaries** — header-only / all-files-missing manifests
   fail fast before table creation; binary content is rejected; a clean
   pretraining+SFT dataset validates.
3. **Failure accounting** — a rejected batch POST surfaces every record as a
   failure so the run exits non-zero.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Generator
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.ingestors import base as base_mod
from tracebloc_ingestor.ingestors.base import BaseIngestor
from tracebloc_ingestor.utils.constants import TaskCategory

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_ROOT / "examples" / "yaml"


# ---------------------------------------------------------------------------
# Local test doubles (mirror tests/test_ingestor_base.py so this file is
# self-contained).
# ---------------------------------------------------------------------------


class FakeIngestor(BaseIngestor):
    """Concrete BaseIngestor whose read_data yields preset records."""

    def __init__(self, records, **kwargs):
        self._records = records
        super().__init__(**kwargs)

    def read_data(self, source: Any) -> Generator[Dict[str, Any], None, None]:
        yield from self._records


def make_ingestor(records=None, **overrides):
    db = MagicMock(name="Database")
    # #350: content_hash is the default id strategy; the mock DB must return a
    # real salt string (a MagicMock salt poisons the hash and drops every row).
    db.get_or_create_table_salt.return_value = "0" * 64
    db.create_table.return_value = MagicMock(name="table")
    db.insert_batch.return_value = ([1, 2], [])
    db.get_table_schema.return_value = {"a": "INT"}
    db.get_label_counts.return_value = {"cat": 2}
    db.get_samples.return_value = []
    api = MagicMock(name="APIClient")
    api.send_ingest_summary.return_value = {"dataset_id": 1, "dataset_key": "key"}

    kwargs = dict(
        database=db,
        api_client=api,
        table_name="tbl",
        schema={"a": "INT"},
        intent="train",
        category=TaskCategory.CAUSAL_LANGUAGE_MODELING,
        label_column=None,
    )
    kwargs.update(overrides)
    return FakeIngestor(records or [], **kwargs)


def _clm_ingestor_on(tmp_path, clean_env, records):
    """A CLM FakeIngestor wired to a real Config rooted at ``tmp_path/src`` with
    a ``texts/`` subdir, so the path-reading validators run against a real FS."""
    src = tmp_path / "src"
    (src / "texts").mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))
    clean_env.setenv("TABLE_NAME", "clm_train")
    clean_env.setenv("DEST_PATH", str(tmp_path / "dest" / "clm_train"))
    # Self-supervised CLM carries no feature schema (the manifest is just
    # filename/extension), so no DataValidator — match that real shape.
    ing = make_ingestor(records=records, schema={})
    ing.database.config = Config()
    return ing, src / "texts"


# ---------------------------------------------------------------------------
# 1. CLI run
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_runtime():
    with patch("tracebloc_ingestor.cli.run.Config") as cfg_cls, patch(
        "tracebloc_ingestor.cli.run.Database"
    ) as db_cls, patch("tracebloc_ingestor.cli.run.APIClient") as api_cls, patch(
        "tracebloc_ingestor.cli.run.CSVIngestor"
    ) as csv_cls, patch(
        "tracebloc_ingestor.cli.run.JSONIngestor"
    ) as json_cls, patch(
        "tracebloc_ingestor.cli.run.setup_logging"
    ):
        cfg = MagicMock()
        cfg.BATCH_SIZE = 4000
        cfg_cls.return_value = cfg
        for cls_mock in (csv_cls, json_cls):
            inst = MagicMock()
            inst.__enter__ = MagicMock(return_value=inst)
            inst.__exit__ = MagicMock(return_value=False)
            inst.ingest = MagicMock(return_value=[])
            cls_mock.return_value = inst
        yield {"Config": cfg_cls, "Database": db_cls, "APIClient": api_cls,
               "CSVIngestor": csv_cls, "JSONIngestor": json_cls}


def test_cli_run_routes_clm_yaml_to_csv_ingestor(clean_env, mock_runtime, monkeypatch):
    """The shipped CLM example yaml runs end-to-end-but-mocked: a CSVIngestor is
    built with the resolved CLM kwargs (TEXT, self-supervised, no label) and
    ingest() is called once; exit 0."""
    monkeypatch.setenv(
        "INGEST_CONFIG", str(EXAMPLES_DIR / "causal_language_modeling.yaml")
    )
    from tracebloc_ingestor.cli.run import main

    rc = main()

    assert rc == 0
    assert mock_runtime["CSVIngestor"].call_count == 1
    assert mock_runtime["JSONIngestor"].call_count == 0
    _, kwargs = mock_runtime["CSVIngestor"].call_args
    assert kwargs["category"] == TaskCategory.CAUSAL_LANGUAGE_MODELING
    assert kwargs["data_format"] == "text"
    assert kwargs["label_column"] == ""  # self-supervised
    assert kwargs["file_options"]["extension"] == ".txt"
    mock_runtime["CSVIngestor"].return_value.ingest.assert_called_once()


def test_cli_run_rejects_clm_with_label(clean_env, mock_runtime, monkeypatch, tmp_path):
    """Self-supervised: a CLM config that sets `label:` must be rejected at the
    entrypoint (exit 2) before any DB/network call (#213)."""
    bad = tmp_path / "clm_with_label.yaml"
    bad.write_text(
        "apiVersion: tracebloc.io/v1\n"
        "kind: IngestConfig\n"
        "category: causal_language_modeling\n"
        "table: clm_test\n"
        "intent: test\n"
        "csv: /tmp/ignored.csv\n"
        "texts: /tmp/ignored/\n"
        "label: label\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("INGEST_CONFIG", str(bad))
    from tracebloc_ingestor.cli.run import main

    rc = main()

    assert rc == 2
    mock_runtime["Database"].assert_not_called()
    mock_runtime["APIClient"].assert_not_called()


# ---------------------------------------------------------------------------
# 2. Validation boundaries (real filesystem)
# ---------------------------------------------------------------------------


def test_clm_header_only_csv_fails_before_table_creation(clean_env, tmp_path):
    """A header-only CLM manifest is rejected during validation, before the
    destination table is created — no orphan empty table."""
    ing, _ = _clm_ingestor_on(clean_env=clean_env, tmp_path=tmp_path, records=[])
    csv = tmp_path / "manifest.csv"
    pd.DataFrame(columns=["filename"]).to_csv(csv, index=False)

    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(ValueError, match="No data rows found"):
            ing.ingest(str(csv), batch_size=10)
    ing.database.create_table.assert_not_called()


def test_clm_all_files_missing_fails_before_table_creation(clean_env, tmp_path):
    """A populated manifest whose every referenced .txt is missing is rejected
    before table creation."""
    ing, _ = _clm_ingestor_on(clean_env=clean_env, tmp_path=tmp_path, records=[])
    csv = tmp_path / "manifest.csv"
    pd.DataFrame({"filename": ["doc1", "doc2"]}).to_csv(csv, index=False)

    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(ValueError, match="No referenced data files"):
            ing.ingest(str(csv), batch_size=10)
    ing.database.create_table.assert_not_called()


def test_clm_clean_dataset_validates(clean_env, tmp_path):
    """A clean dataset — plain text (pretraining) plus a prompt<TAB>completion
    SFT line — passes validation. The tab is ordinary UTF-8 text."""
    ing, texts = _clm_ingestor_on(clean_env=clean_env, tmp_path=tmp_path, records=[])
    (texts / "pre1.txt").write_text("plain pretraining text\n", encoding="utf-8")
    (texts / "sft1.txt").write_text("Translate: Hi\tBonjour\n", encoding="utf-8")
    csv = tmp_path / "manifest.csv"
    pd.DataFrame({"filename": ["pre1", "sft1"]}).to_csv(csv, index=False)

    assert ing.validate_data(str(csv)) is True


def test_clm_binary_content_rejected(clean_env, tmp_path):
    """A .txt whose bytes are binary (a NUL byte) is rejected by the shared
    TextContentValidator — the same content hygiene the other NLP modalities
    get, here pointed at texts/."""
    ing, texts = _clm_ingestor_on(clean_env=clean_env, tmp_path=tmp_path, records=[])
    (texts / "ok.txt").write_text("fine text\n", encoding="utf-8")
    (texts / "bad.txt").write_bytes(b"\x00\x01\x02binary")
    csv = tmp_path / "manifest.csv"
    pd.DataFrame({"filename": ["ok", "bad"]}).to_csv(csv, index=False)

    with pytest.raises(ValueError, match="NUL byte"):
        ing.validate_data(str(csv))


# ---------------------------------------------------------------------------
# 3. Failure accounting
# ---------------------------------------------------------------------------


def test_clm_api_send_failure_propagates(clean_env):
    """send_ingest_summary failing raises out of ingest() for CLM, so the run
    exits non-zero (file-bearing modality still exercises the file-transfer branch)."""
    records = [{"a": "1", "filename": "f1"}, {"a": "2", "filename": "f2"}]
    ing = make_ingestor(records=records)
    ing.api_client.send_ingest_summary.side_effect = RuntimeError("backend rejected")

    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(RuntimeError, match="backend rejected"):
            ing.ingest("src", batch_size=10)
