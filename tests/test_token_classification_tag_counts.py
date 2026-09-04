"""token_classification output_classes = DISTINCT TAGS, not tag SEQUENCES
(backend#1747).

The ``label`` column stores the whole per-row BIO tag sequence (one tag per
word — e.g. ``"O B-PER I-PER O"``), so the row-unit ``get_label_counts``
(``GROUP BY label``) counts distinct SEQUENCE STRINGS as classes — dozens of
them — and the trained model head, built on that class set, never links to the
9-ish real tags. The task is then unrunnable end to end.

The fix routes token_classification's ingest-summary count through
``Database.get_tag_counts``, which explodes each sequence into its tags, gated
on the ``label_is_tag_sequence`` ModalitySpec trait (never a category string).

Covered here:
- the registry trait (set on token_classification, and unique to it);
- the trait does NOT leak into the CLI layout contract (unchanged surface);
- the base.py count-path routing: token_classification queries the exploded
  helper and ships its distinct-tag counts as ``labels``; a non-tag category
  still uses the row-unit helper;
- ``get_tag_counts`` itself — the explode-and-weight arithmetic (the 77→9
  bug), NULL-label handling, and SQL shape.

The DB boundary is mocked like the rest of the unit suite.
"""

from __future__ import annotations

from typing import Any, Dict, Generator
from unittest.mock import MagicMock, patch

import pytest

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.database import Database
from tracebloc_ingestor.ingestors import base as base_mod
from tracebloc_ingestor.ingestors.base import BaseIngestor
from tracebloc_ingestor.modalities import REGISTRY, spec_for
from tracebloc_ingestor.modalities.layout import build_layout_contract
from tracebloc_ingestor.utils.constants import TaskCategory

TOKEN = TaskCategory.TOKEN_CLASSIFICATION


# ---------------------------------------------------------------------------
# Registry trait + layout contract
# ---------------------------------------------------------------------------


def test_token_classification_carries_tag_sequence_trait():
    spec = spec_for(TOKEN)
    assert spec.label_is_tag_sequence is True
    # Sanity: it's still the file-bearing NLP text task it always was, and it is
    # NOT a whole-sequence classifier (the per-token head has no single class).
    assert spec.is_file_bearing and spec.is_nlp
    assert not spec.is_classification and not spec.is_self_supervised


def test_tag_sequence_trait_is_unique_to_token_classification():
    # Today exactly one tag-sequence category. A second one is a deliberate
    # registry decision — update this test when it lands.
    tagged = {c for c, s in REGISTRY.items() if s.label_is_tag_sequence}
    assert tagged == {TOKEN}


def test_trait_does_not_leak_into_layout_contract():
    # label_is_tag_sequence drives the ingest-summary count only; it is not part
    # of the CLI's on-disk staging contract, so it never surfaces as a per-task
    # key in the layout JSON.
    doc = build_layout_contract()
    assert doc["version"] == "4"
    assert "label_is_tag_sequence" not in doc["tasks"][TOKEN]


# ---------------------------------------------------------------------------
# base.py count-path routing (DB boundary mocked)
# ---------------------------------------------------------------------------


class _FakeIngestor(BaseIngestor):
    """Concrete BaseIngestor whose read_data yields preset records."""

    def __init__(self, records, **kwargs):
        self._records = records
        super().__init__(**kwargs)

    def read_data(self, source: Any) -> Generator[Dict[str, Any], None, None]:
        yield from self._records


def _make_ingestor(records, **overrides):
    db = MagicMock(name="Database")
    # #350: content_hash is the default id strategy; the mock DB must return a
    # real salt string (a MagicMock salt poisons the hash and drops every row).
    db.get_or_create_table_salt.return_value = "0" * 64
    db.create_table.return_value = MagicMock(name="table")
    db.insert_batch.return_value = ([1, 2], [])  # ids, db_failures
    db.get_table_schema.return_value = {"label": "VARCHAR(255)"}
    db.get_samples.return_value = []
    api = MagicMock(name="APIClient")
    api.config.TITLE = None
    api.send_ingest_summary.return_value = {"dataset_id": 1, "dataset_key": "k"}

    kwargs = dict(
        database=db,
        api_client=api,
        table_name="tok_tbl",
        schema={"label": "VARCHAR(255)"},
        label_column="label",
        intent="train",
        category=TOKEN,
    )
    kwargs.update(overrides)
    return _FakeIngestor(records, **kwargs)


def _run_ingest(ing, batch_size=10):
    """Run ingest with Session / validation / file-transfer patched out so a
    file-bearing category runs without a real filesystem."""
    with patch.object(base_mod, "Session") as Sess, patch.object(
        ing, "validate_data", return_value=True
    ), patch.object(
        base_mod,
        "map_file_transfer",
        side_effect=lambda c, r, o, cfg=None, source_record=None: r,
    ):
        Sess.return_value.__enter__.return_value = MagicMock()
        return ing.ingest("src", batch_size=batch_size)


def test_ingest_counts_distinct_tags_not_sequences():
    """The whole bug: two rows carry DISTINCT BIO SEQUENCE strings but share the
    same three TAGS. output_classes must be those tags, so the count path uses
    the exploded helper and ships its result verbatim as ``labels`` — never the
    row-unit or sequence-unit helpers."""
    records = [
        {"filename": "f1", "label": "O B-PER I-PER"},
        {"filename": "f2", "label": "B-PER O O"},
    ]
    ing = _make_ingestor(records)
    ing.database.get_tag_counts.return_value = {"O": 4, "B-PER": 2, "I-PER": 1}

    failed = _run_ingest(ing)

    assert failed == []
    ing.database.get_tag_counts.assert_called_once_with("tok_tbl", ing.ingestor_id)
    ing.database.get_label_counts.assert_not_called()
    ing.database.get_label_sequence_counts.assert_not_called()

    summary_kwargs = ing.api_client.send_ingest_summary.call_args.kwargs
    # Distinct TAGS (3), not distinct sequence strings (2).
    assert summary_kwargs["labels"] == {"O": 4, "B-PER": 2, "I-PER": 1}
    assert summary_kwargs["category"] == TOKEN
    # backend#2770: the record count rides the payload EXPLICITLY and is the
    # ROW count (2 texts) — NOT sum(labels.values()) (7 tag occurrences). This
    # is exactly the fixture where the two diverge, which is the whole point:
    # before this the backend inferred the count from the label sum, so #527's
    # exploded tag counts silently inflated it by ~mean(sequence length)×.
    assert summary_kwargs["record_count"] == 2
    assert summary_kwargs["record_count"] != sum(summary_kwargs["labels"].values())


def test_non_tag_category_keeps_row_counts():
    # Control: text_classification (a real single-class text task) still uses the
    # row-unit helper — the explode is trait-gated, not global to text tasks.
    records = [
        {"filename": "f1", "label": "spam"},
        {"filename": "f2", "label": "ham"},
    ]
    ing = _make_ingestor(records, category=TaskCategory.TEXT_CLASSIFICATION)
    ing.database.get_label_counts.return_value = {"spam": 1, "ham": 1}

    _run_ingest(ing)

    ing.database.get_label_counts.assert_called_once()
    ing.database.get_tag_counts.assert_not_called()
    ing.database.get_label_sequence_counts.assert_not_called()


# ---------------------------------------------------------------------------
# Database.get_tag_counts (SQL shape + explode arithmetic; engine mocked)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    with patch("tracebloc_ingestor.database.create_engine") as ce:
        engine = MagicMock(name="engine")
        conn = MagicMock(name="conn")
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        ce.return_value = engine
        db = Database(Config(DB_USER="u", DB_PASSWORD="p", DB_NAME="d"))
        conn.reset_mock()  # drop __init__'s CREATE DATABASE call
        yield db, conn


def test_get_tag_counts_explodes_and_weights_by_row_count(mock_db):
    """Two distinct sequence strings (what GROUP BY label returns) explode into
    the handful of distinct tags, each tag weighted by its occurrences × the
    sequence's row count. This is the 77-sequences → 9-tags collapse."""
    db, conn = mock_db
    conn.execute.return_value.fetchall.return_value = [
        ("O B-PER I-PER", 3),  # O×1, B-PER×1, I-PER×1, over 3 rows
        ("O O B-PER", 2),  # O×2, B-PER×1, over 2 rows
    ]
    counts = db.get_tag_counts("t", "ing-1")
    assert counts == {"O": 1 * 3 + 2 * 2, "B-PER": 1 * 3 + 1 * 2, "I-PER": 1 * 3}
    # The heavy lifting still happens in SQL: GROUP BY bounds the row set to the
    # distinct sequences, and the id is bound, not interpolated.
    sql = str(conn.execute.call_args.args[0])
    assert "GROUP BY label" in sql
    assert conn.execute.call_args.args[1] == {"ingestor_id": "ing-1"}


def test_get_tag_counts_skips_null_label(mock_db):
    # A NULL label carries no tags (unlike get_label_counts, which folds NULL to
    # the "" key — an empty class is meaningless for a per-token tag set).
    db, conn = mock_db
    conn.execute.return_value.fetchall.return_value = [
        (None, 5),
        ("O B-LOC", 2),
    ]
    assert db.get_tag_counts("t", "ing-1") == {"O": 2, "B-LOC": 2}


def test_get_tag_counts_escapes_table_backticks(mock_db):
    db, conn = mock_db
    conn.execute.return_value.fetchall.return_value = []
    db.get_tag_counts("we`ird", "ing-1")
    sql = str(conn.execute.call_args.args[0])
    assert "FROM `we``ird`" in sql
