"""RFC-0003 D16/D19 (tracebloc/backend#1205): per-ingestion immutable tables.

Flag OFF (the default) must be byte-for-byte today's behavior: the physical
table IS the user label, the summary payload has no ``physical_table`` key,
and existing tables are reflected (append). Flag ON: every run writes its own
``ds_<ingestor_id>`` table, the reflect/append path is a hard error, and the
ingest summary carries the physical handle so the backend can persist it
(tracebloc/backend#1206). The label keeps serving the user-facing surfaces
(summary URL segment, staging dirs, table lock, default title).
"""

import json
import uuid
from unittest.mock import MagicMock, patch

import pytest

from tracebloc_ingestor import database as db_mod
from tracebloc_ingestor.api.client import APIClient
from tracebloc_ingestor.config import Config
from tracebloc_ingestor.database import Database
from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor


# ── Config flag ──────────────────────────────────────────────────────────────


def test_flag_defaults_off(monkeypatch):
    monkeypatch.delenv("PER_INGESTION_TABLES", raising=False)
    assert Config(EDGE_ENV="local").PER_INGESTION_TABLES is False


@pytest.mark.parametrize(
    "raw,expected",
    [("1", True), ("true", True), ("YES", True), ("on", True),
     ("0", False), ("", False), ("no", False), ("off", False)],
)
def test_flag_env_parsing(monkeypatch, raw, expected):
    monkeypatch.setenv("PER_INGESTION_TABLES", raw)
    assert Config(EDGE_ENV="local").PER_INGESTION_TABLES is expected


def test_flag_override_beats_env(monkeypatch):
    monkeypatch.setenv("PER_INGESTION_TABLES", "1")
    assert Config(EDGE_ENV="local", PER_INGESTION_TABLES=False).PER_INGESTION_TABLES is False
    monkeypatch.delenv("PER_INGESTION_TABLES", raising=False)
    assert Config(EDGE_ENV="local", PER_INGESTION_TABLES=True).PER_INGESTION_TABLES is True


# ── Ingestor naming ─────────────────────────────────────────────────────────


def _make_ingestor(per_ingestion):
    database = MagicMock(name="Database")
    # Real bool, not a truthy Mock — BaseIngestor compares `is True`.
    database.config.PER_INGESTION_TABLES = per_ingestion
    api_client = MagicMock(name="APIClient")
    return CSVIngestor(
        database=database,
        api_client=api_client,
        table_name="my_dataset",
        schema={"feature": "FLOAT", "label": "VARCHAR(50)"},
        label_column="label",
    )


def test_flag_off_physical_name_is_the_label():
    ing = _make_ingestor(per_ingestion=False)
    assert ing.per_ingestion_tables is False
    assert ing.physical_table_name == "my_dataset"
    assert ing.table_name == "my_dataset"


def test_flag_on_physical_name_is_hex_of_ingestor_id():
    ing = _make_ingestor(per_ingestion=True)
    assert ing.per_ingestion_tables is True
    # ds_<uuid4().hex>: a pure function of ingestor_id, hyphen-free because
    # SQL table grammars (trainer _SQL_IDENTIFIER_RE, this package's
    # TableNameValidator) forbid hyphens — D3 amendment on backend#1204.
    assert ing.physical_table_name == f"ds_{uuid.UUID(ing.ingestor_id).hex}"
    assert "-" not in ing.physical_table_name
    # 'ds_' + 32 hex chars = 35, inside MySQL's 64-char identifier cap.
    assert len(ing.physical_table_name) == 35
    # The label survives untouched for the user-facing surfaces.
    assert ing.table_name == "my_dataset"


def test_flag_on_rejects_file_bearing_categories():
    """v1 boundary (Bugbot): the flag isolates the row store only — assets of
    file-bearing categories still land under the shared label tree, where a
    later same-label ingest would overwrite an earlier dataset's files. Fail
    at construction, before any validation or DDL; per-dataset file isolation
    ships with tracebloc/client-runtime#203 phase 2."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    database = MagicMock(name="Database")
    database.config.PER_INGESTION_TABLES = True
    with pytest.raises(ValueError, match="file-bearing"):
        CSVIngestor(
            database=database,
            api_client=MagicMock(),
            table_name="imgs",
            schema={"filename": "VARCHAR(255)", "label": "VARCHAR(50)"},
            label_column="label",
            category=TaskCategory.IMAGE_CLASSIFICATION,
        )


def test_flag_on_accepts_row_only_categories():
    """Tabular / time-series families — the RFC-0003 v1 rollout target —
    construct normally under the flag."""
    from tracebloc_ingestor.utils.constants import TaskCategory

    database = MagicMock(name="Database")
    database.config.PER_INGESTION_TABLES = True
    ing = CSVIngestor(
        database=database,
        api_client=MagicMock(),
        table_name="rows",
        schema={"feature": "FLOAT", "label": "VARCHAR(50)"},
        label_column="label",
        category=TaskCategory.TABULAR_CLASSIFICATION,
    )
    assert ing.physical_table_name.startswith("ds_")


def test_truthy_mock_config_does_not_flip_the_storage_model():
    """Bare MagicMock config (the fixture default across this suite) must
    read as flag OFF — a truthy Mock silently flipping every mocked test
    onto per-ingestion naming would invalidate the whole legacy suite."""
    database = MagicMock(name="Database")  # config.PER_INGESTION_TABLES is a Mock
    ing = CSVIngestor(
        database=database,
        api_client=MagicMock(),
        table_name="t",
        schema={"feature": "FLOAT", "label": "VARCHAR(50)"},
        label_column="label",
    )
    assert ing.per_ingestion_tables is False
    assert ing.physical_table_name == "t"


# ── create_table immutability guard ─────────────────────────────────────────


@pytest.fixture
def mock_engine_factory():
    with patch.object(db_mod, "create_engine") as ce:
        engine = MagicMock(name="engine")
        conn = MagicMock(name="connection")
        engine.connect.return_value.__enter__.return_value = conn
        ce.return_value = engine
        yield ce, engine, conn


@pytest.fixture
def db(mock_engine_factory):
    return Database(Config(EDGE_ENV="local"))


def test_create_table_must_not_exist_rejects_cached_table(db):
    db.tables["ds_deadbeef"] = MagicMock(name="table")
    with pytest.raises(ValueError, match="immutable"):
        db.create_table("ds_deadbeef", {"feature": "FLOAT"}, must_not_exist=True)


def test_create_table_must_not_exist_rejects_existing_db_table(db):
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["ds_deadbeef"]
    with patch.object(db_mod, "inspect", return_value=inspector):
        with pytest.raises(ValueError, match="immutable"):
            db.create_table("ds_deadbeef", {"feature": "FLOAT"}, must_not_exist=True)


def test_create_table_default_still_reflects_existing(db):
    """Legacy path unchanged: without the flag, an existing cached table is
    returned (the append behavior)."""
    sentinel = MagicMock(name="table")
    db.tables["legacy"] = sentinel
    assert db.create_table("legacy", {"feature": "FLOAT"}) is sentinel


# ── summary payload ──────────────────────────────────────────────────────────


def _resp(status_code, body=None):
    r = MagicMock()
    r.status_code = status_code
    r.text = json.dumps(body or {})
    r.json.return_value = body or {}
    r.raise_for_status = MagicMock()
    return r


def _client():
    cfg = Config(BACKEND_TOKEN="tok", EDGE_ENV="prod")
    with patch.object(APIClient, "authenticate", return_value="tok"):
        return APIClient(cfg)


def _send_and_capture(client, **extra):
    """POST a summary against a mocked transport; return the JSON payload."""
    with patch.object(
        client.session, "post",
        return_value=_resp(201, {"dataset_id": 1, "dataset_key": "k"}),
    ) as post:
        client.send_ingest_summary(
            table_name="my_dataset", ingestor_id="ing-1", labels={"cat": 2},
            dataset_title="T", data_format="tabular", data_intent="train",
            category="tabular_classification", schema={}, samples=[], **extra,
        )
        _, kwargs = post.call_args
    return json.loads(kwargs["data"])


def test_summary_payload_carries_physical_table_when_given():
    payload = _send_and_capture(_client(), physical_table="ds_ing-1")
    assert payload["physical_table"] == "ds_ing-1"


def test_summary_payload_omits_physical_table_by_default():
    """Legacy payload stays byte-identical: no key at all, not a null."""
    payload = _send_and_capture(_client())
    assert "physical_table" not in payload
