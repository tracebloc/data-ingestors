"""End-to-end ingest correlation id (backend#1028 item 3).

The CLI's idempotency key reaches the ingestor as the
``TRACEBLOC_INGEST_CORRELATION_ID`` env var (stamped by jobs-manager on the
spawned Job). Covered here:

- ``resolve_correlation_id``: honoured when valid, ``None`` when absent,
  warned + ``None`` when malformed (observability must never fail an ingest);
- ``BaseIngestor.__init__``: keeps the id ALONGSIDE the per-process
  ``ingestor_id`` (never replacing it — row scoping across Job retries) and
  rides it on ``file_options``;
- full ingest: the id reaches the backend registration payload's
  ``meta_data``; without the env the payload is byte-identical to before.
"""

from __future__ import annotations

import logging
import uuid
from unittest.mock import MagicMock, patch

import pandas as pd

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.ingestors import base as base_mod
from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
from tracebloc_ingestor.utils.constants import DataFormat, TaskCategory
from tracebloc_ingestor.utils.correlation import (
    CORRELATION_ID_ENV,
    resolve_correlation_id,
)

# The CLI's generated shape: 16 crypto/rand bytes, hex-encoded (32 chars).
CLI_STYLE_KEY = "9f2c4a1de6b7085f3a9c0d12e4f56a7b"


# ---------------------------------------------------------------------------
# resolve_correlation_id — env honoured / validated / fallback
# ---------------------------------------------------------------------------


def test_resolver_returns_none_when_unset(monkeypatch):
    monkeypatch.delenv(CORRELATION_ID_ENV, raising=False)
    assert resolve_correlation_id() is None


def test_resolver_returns_none_for_blank_values(monkeypatch):
    for blank in ("", "   ", "\n"):
        monkeypatch.setenv(CORRELATION_ID_ENV, blank)
        assert resolve_correlation_id() is None


def test_resolver_honours_cli_style_key(monkeypatch):
    monkeypatch.setenv(CORRELATION_ID_ENV, CLI_STYLE_KEY)
    assert resolve_correlation_id() == CLI_STYLE_KEY


def test_resolver_accepts_customer_chosen_keys():
    # --idempotency-key overrides: dots, dashes, underscores, 64-char max —
    # the same alphabet jobs-manager's Kubernetes labels keep verbatim.
    for key in ("nightly-claims-2026.07", "run_1", "a" * 64):
        assert resolve_correlation_id({CORRELATION_ID_ENV: key}) == key


def test_resolver_strips_surrounding_whitespace():
    assert (
        resolve_correlation_id({CORRELATION_ID_ENV: f"  {CLI_STYLE_KEY}\n"})
        == CLI_STYLE_KEY
    )


def test_resolver_rejects_malformed_values_with_warning(caplog):
    # Over-long, illegal chars, log-injection attempts: warned and ignored —
    # a bad value must degrade to "no correlation id", never fail the run.
    for bad in ("a" * 65, "spaced key", "key:colon", "kéy", "a\nb"):
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            assert resolve_correlation_id({CORRELATION_ID_ENV: bad}) is None
        assert any(
            CORRELATION_ID_ENV in rec.getMessage() for rec in caplog.records
        ), f"expected a warning naming the env var for {bad!r}"


# ---------------------------------------------------------------------------
# BaseIngestor wiring — alongside ingestor_id, onto file_options
# ---------------------------------------------------------------------------


def _make_ingestor(**overrides):
    database = MagicMock(name="Database")
    # #350: content_hash default ⇒ mock DB must return a real salt string.
    database.get_or_create_table_salt.return_value = "0" * 64
    api_client = MagicMock(name="APIClient")
    kwargs = dict(
        database=database,
        api_client=api_client,
        table_name="corr_toy",
        schema={"heart_rate": "FLOAT", "label": "INT"},
        label_column="label",
        category=TaskCategory.TABULAR_CLASSIFICATION,
        file_options={},
    )
    kwargs.update(overrides)
    return CSVIngestor(**kwargs)


def test_ingestor_picks_up_env_alongside_fresh_ingestor_id(monkeypatch):
    monkeypatch.setenv(CORRELATION_ID_ENV, CLI_STYLE_KEY)
    ing = _make_ingestor()

    assert ing.correlation_id == CLI_STYLE_KEY
    assert ing.file_options["correlation_id"] == CLI_STYLE_KEY
    # ingestor_id stays a fresh per-process UUID — row scoping (label
    # counts, the #227 compensating delete) must not be shared across a
    # Job retry that reruns with the same env.
    assert ing.ingestor_id != CLI_STYLE_KEY
    assert str(uuid.UUID(ing.ingestor_id)) == ing.ingestor_id


def test_ingestor_without_env_behaves_as_before(monkeypatch):
    monkeypatch.delenv(CORRELATION_ID_ENV, raising=False)
    ing = _make_ingestor()

    assert ing.correlation_id is None
    assert "correlation_id" not in ing.file_options


def test_ingestor_ignores_malformed_env(monkeypatch):
    monkeypatch.setenv(CORRELATION_ID_ENV, "not a valid key!")
    ing = _make_ingestor()

    assert ing.correlation_id is None
    assert "correlation_id" not in ing.file_options


# ---------------------------------------------------------------------------
# Full ingest — the id reaches the registration payload's meta_data
# ---------------------------------------------------------------------------


def _toy_frame() -> pd.DataFrame:
    return pd.DataFrame([{"heart_rate": 70.0 + i, "label": i % 2} for i in range(10)])


def _make_full_ingestor():
    """Mirror the mocked-DB full-ingest harness of
    test_time_series_classification (tabular control variant)."""
    db = MagicMock(name="Database")
    db.config = Config(TABLE_NAME="corr_toy")
    # #350: content_hash default ⇒ mock DB must return a real salt string.
    db.get_or_create_table_salt.return_value = "0" * 64
    db.create_table.return_value = MagicMock(name="table")
    db.insert_batch.side_effect = lambda table, batch: (
        list(range(len(batch))),
        [],
    )
    db.get_table_schema.return_value = {"heart_rate": "FLOAT", "label": "INT"}
    db.get_label_counts.return_value = {"0": 5, "1": 5}
    db.iter_label_counts.return_value = [("0", 5), ("1", 5)]
    db.get_samples.return_value = []
    api = MagicMock(name="APIClient")
    api.config.TITLE = "corr toy"
    api.send_ingest_summary.return_value = {"dataset_id": 1}
    return CSVIngestor(
        database=db,
        api_client=api,
        table_name="corr_toy",
        schema={"heart_rate": "FLOAT", "label": "INT"},
        label_column="label",
        intent="train",
        category=TaskCategory.TABULAR_CLASSIFICATION,
        data_format=DataFormat.TABULAR,
    )


def _run_full_ingest(ing, csv_path):
    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        return ing.ingest(str(csv_path), batch_size=50)


def test_correlation_id_reaches_registration_payload(make_csv, monkeypatch):
    monkeypatch.setenv(CORRELATION_ID_ENV, CLI_STYLE_KEY)
    csv_path = make_csv(_toy_frame(), name="corr.csv")
    ing = _make_full_ingestor()

    failed = _run_full_ingest(ing, csv_path)

    assert failed == []
    summary_kwargs = ing.api_client.send_ingest_summary.call_args.kwargs
    assert summary_kwargs["meta_data"]["correlation_id"] == CLI_STYLE_KEY
    # The per-process id still scopes the run's rows and the summary call.
    assert summary_kwargs["ingestor_id"] == ing.ingestor_id


def test_payload_unchanged_without_env(make_csv, monkeypatch):
    monkeypatch.delenv(CORRELATION_ID_ENV, raising=False)
    csv_path = make_csv(_toy_frame(), name="corr.csv")
    ing = _make_full_ingestor()

    failed = _run_full_ingest(ing, csv_path)

    assert failed == []
    meta = ing.api_client.send_ingest_summary.call_args.kwargs["meta_data"]
    assert "correlation_id" not in meta
