"""Sentence-pair classification — modality wiring, mirroring the
text_classification / embeddings coverage.

sentence_pair_classification is the SUPERVISED text-classification modality whose
``.txt`` holds a STRUCTURED tab-separated ``text_a\\ttext_b`` pair. It shares
text_classification's supervised semantics (a ``label`` column in the CSV, so it
carries LabelColumn + LabelDiversity validators) and its raw-text staging — each
sample is one ``.txt`` staged from ``texts/``. What makes it distinct is the
STRUCTURED on-disk shape: the dedicated ``SentencePairValidator`` rejects any
``.txt`` that isn't exactly 2 non-empty tab-separated fields. These tests pin the
behaviors the modality must get right:

1. **CLI run** — a sentence_pair ``ingest.yaml`` routes through ``cli.run.main``
   to a CSVIngestor with the resolved kwargs (TEXT, label_column="label"); a
   config that (wrongly) OMITS ``label`` is rejected at the entrypoint
   (supervised — the opposite of the self-supervised modalities).
2. **Validation boundaries** — header-only / all-files-missing manifests fail
   fast before table creation; binary content is rejected; a malformed sample
   (no tab, or a 3-field triplet) is rejected by the structural validator; clean
   pairs validate.
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
# Local test doubles (mirror tests/test_embeddings.py so this file is
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
    # base.py streams the outbound label counts (#488): an unstubbed MagicMock
    # iterates empty, which the peek reads as "no labels inserted".
    db.iter_label_counts.return_value = [("cat", 2)]
    api = MagicMock(name="APIClient")
    api.send_batch.return_value = True
    api.send_generate_edge_label_meta.return_value = True
    api.send_global_meta_meta.return_value = True
    api.prepare_dataset.return_value = True
    api.create_dataset.return_value = {"id": 1}

    kwargs = dict(
        database=db,
        api_client=api,
        table_name="tbl",
        schema={"a": "INT"},
        intent="train",
        category=TaskCategory.SENTENCE_PAIR_CLASSIFICATION,
        label_column="label",
    )
    kwargs.update(overrides)
    return FakeIngestor(records or [], **kwargs)


def _spc_ingestor_on(tmp_path, clean_env, records):
    """A sentence_pair FakeIngestor wired to a real Config rooted at
    ``tmp_path/src`` with a ``texts/`` subdir, so the path-reading validators
    run against a real FS. Supervised — carries label_column="label"."""
    src = tmp_path / "src"
    (src / "texts").mkdir(parents=True)
    clean_env.setenv("SRC_PATH", str(src))
    clean_env.setenv("TABLE_NAME", "spc_train")
    clean_env.setenv("DEST_PATH", str(tmp_path / "dest" / "spc_train"))
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
        yield {
            "Config": cfg_cls,
            "Database": db_cls,
            "APIClient": api_cls,
            "CSVIngestor": csv_cls,
            "JSONIngestor": json_cls,
        }


def test_cli_run_routes_sentence_pair_yaml_to_csv_ingestor(
    clean_env, mock_runtime, monkeypatch
):
    """The shipped sentence_pair example yaml runs end-to-end-but-mocked: a
    CSVIngestor is built with the resolved sentence_pair kwargs (TEXT, supervised
    with label_column="label") and ingest() is called once; exit 0."""
    monkeypatch.setenv(
        "INGEST_CONFIG", str(EXAMPLES_DIR / "sentence_pair_classification.yaml")
    )
    from tracebloc_ingestor.cli.run import main

    rc = main()

    assert rc == 0
    assert mock_runtime["CSVIngestor"].call_count == 1
    assert mock_runtime["JSONIngestor"].call_count == 0
    _, kwargs = mock_runtime["CSVIngestor"].call_args
    assert kwargs["category"] == TaskCategory.SENTENCE_PAIR_CLASSIFICATION
    assert kwargs["data_format"] == "text"
    assert kwargs["label_column"] == "label"  # supervised
    assert kwargs["file_options"]["extension"] == ".txt"
    mock_runtime["CSVIngestor"].return_value.ingest.assert_called_once()


def test_cli_run_rejects_sentence_pair_without_label(
    clean_env, mock_runtime, monkeypatch, tmp_path
):
    """Supervised: a sentence_pair config that OMITS `label:` must be rejected at
    the entrypoint (exit 2) before any DB/network call — the opposite of the
    self-supervised modalities, which reject a config that SETS label."""
    bad = tmp_path / "spc_no_label.yaml"
    bad.write_text(
        "apiVersion: tracebloc.io/v1\n"
        "kind: IngestConfig\n"
        "category: sentence_pair_classification\n"
        "table: spc_test\n"
        "intent: test\n"
        "csv: /tmp/ignored.csv\n"
        "texts: /tmp/ignored/\n",
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


def test_spc_header_only_csv_fails_before_table_creation(clean_env, tmp_path):
    """A header-only sentence_pair manifest is rejected during validation, before
    the destination table is created — no orphan empty table."""
    ing, _ = _spc_ingestor_on(clean_env=clean_env, tmp_path=tmp_path, records=[])
    csv = tmp_path / "manifest.csv"
    pd.DataFrame(columns=["filename", "label"]).to_csv(csv, index=False)

    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(ValueError, match="No data rows found"):
            ing.ingest(str(csv), batch_size=10)
    ing.database.create_table.assert_not_called()


def test_spc_all_files_missing_fails_before_table_creation(clean_env, tmp_path):
    """A populated manifest whose every referenced .txt is missing is rejected
    before table creation."""
    ing, _ = _spc_ingestor_on(clean_env=clean_env, tmp_path=tmp_path, records=[])
    csv = tmp_path / "manifest.csv"
    pd.DataFrame({"filename": ["p1", "p2"], "label": ["entailment", "neutral"]}).to_csv(
        csv, index=False
    )

    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(ValueError, match="No referenced data files"):
            ing.ingest(str(csv), batch_size=10)
    ing.database.create_table.assert_not_called()


def test_spc_clean_pairs_validate(clean_env, tmp_path):
    """A clean dataset of tab-separated text_a<TAB>text_b pairs with >=2 distinct
    labels passes validation."""
    ing, texts = _spc_ingestor_on(clean_env=clean_env, tmp_path=tmp_path, records=[])
    (texts / "pair1.txt").write_text(
        "A man plays guitar.\tSomeone makes music.\n", encoding="utf-8"
    )
    (texts / "pair2.txt").write_text(
        "A man plays guitar.\tThe man is silent.\n", encoding="utf-8"
    )
    csv = tmp_path / "manifest.csv"
    pd.DataFrame(
        {"filename": ["pair1", "pair2"], "label": ["entailment", "contradiction"]}
    ).to_csv(csv, index=False)

    assert ing.validate_data(str(csv)) is True


def test_spc_malformed_no_tab_rejected(clean_env, tmp_path):
    """A .txt with no tab (a single sentence — valid for text_classification, NOT
    for sentence pairs) is rejected by SentencePairValidator."""
    ing, texts = _spc_ingestor_on(clean_env=clean_env, tmp_path=tmp_path, records=[])
    (texts / "ok.txt").write_text("text_a\ttext_b\n", encoding="utf-8")
    (texts / "bad.txt").write_text("just one sentence, no tab here\n", encoding="utf-8")
    csv = tmp_path / "manifest.csv"
    pd.DataFrame(
        {"filename": ["ok", "bad"], "label": ["entailment", "neutral"]}
    ).to_csv(csv, index=False)

    with pytest.raises(ValueError, match="tab-separated fields"):
        ing.validate_data(str(csv))


def test_spc_triplet_rejected(clean_env, tmp_path):
    """A 3-field triplet is valid for embeddings but malformed for a sentence
    pair — exactly 2 tab-separated fields are required."""
    ing, texts = _spc_ingestor_on(clean_env=clean_env, tmp_path=tmp_path, records=[])
    (texts / "ok.txt").write_text("text_a\ttext_b\n", encoding="utf-8")
    (texts / "bad.txt").write_text("a\tb\tc\n", encoding="utf-8")
    csv = tmp_path / "manifest.csv"
    pd.DataFrame(
        {"filename": ["ok", "bad"], "label": ["entailment", "neutral"]}
    ).to_csv(csv, index=False)

    with pytest.raises(ValueError, match="tab-separated fields"):
        ing.validate_data(str(csv))


def test_spc_binary_content_rejected(clean_env, tmp_path):
    """A .txt whose bytes are binary (a NUL byte) is rejected by the shared
    TextContentValidator — the same content hygiene the other NLP modalities
    get, here pointed at texts/."""
    ing, texts = _spc_ingestor_on(clean_env=clean_env, tmp_path=tmp_path, records=[])
    (texts / "ok.txt").write_text("text_a\ttext_b\n", encoding="utf-8")
    (texts / "bad.txt").write_bytes(b"\x00\x01\x02binary")
    csv = tmp_path / "manifest.csv"
    pd.DataFrame(
        {"filename": ["ok", "bad"], "label": ["entailment", "neutral"]}
    ).to_csv(csv, index=False)

    with pytest.raises(ValueError, match="NUL byte"):
        ing.validate_data(str(csv))


# ---------------------------------------------------------------------------
# 3. Failure accounting
# ---------------------------------------------------------------------------


def test_spc_ingest_summary_failure_raises_out_of_ingest(clean_env):
    """The per-batch API publish is gone (#325): a single send_ingest_summary
    call is made after commit. When it fails, the error raises out of ingest()
    so the run exits non-zero (the file-transfer branch runs since sentence_pair
    is file-bearing)."""
    records = [
        {"a": "1", "filename": "f1", "label": "entailment"},
        {"a": "2", "filename": "f2", "label": "neutral"},
    ]
    ing = make_ingestor(records=records)
    ing.api_client.send_ingest_summary.side_effect = RuntimeError("backend rejected")

    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ), patch.object(
        base_mod, "compute_text_profile", return_value=None
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(RuntimeError, match="backend rejected"):
            ing.ingest("src", batch_size=10)
