"""backend#816 — the semseg ``mask_id`` contract, pinned at the ingestor boundary.

The training client resolves each mask file from a MySQL ``mask_id`` column
(``segmentation_dataset_pytorch.py``: ``str(row["mask_id"])`` → filename, with
no naming-convention fallback), so a semseg ingest that stores a NULL/absent
``mask_id`` makes the client crash with ``FileNotFoundError`` — not a silent
skip. The develop regression (#212's blanket ``mask_id`` pop) shipped precisely
because *no test crossed the ingestor → stored-row boundary*. These tests close
that gap:

- a sidecar's ``link_column`` (the masks sidecar's ``mask_id``) is always a
  stored column, even when the dataset schema doesn't declare it (auto-add);
- the rule is ``link_column``-gated (object detection's annotations sidecar has
  none, so nothing is added there);
- an ingested semseg row carries the per-row ``mask_id`` VALUE, not NULL.
"""

from __future__ import annotations

from typing import Any, Dict, Generator
from unittest.mock import MagicMock

from tracebloc_ingestor.ingestors.base import BaseIngestor
from tracebloc_ingestor.ingestors.record_processor import RecordProcessor


class _FakeIngestor(BaseIngestor):
    """Concrete BaseIngestor with a no-op read_data — we only exercise __init__."""

    def read_data(self, source: Any) -> Generator[Dict[str, Any], None, None]:
        yield from ()


def _make(**overrides) -> _FakeIngestor:
    kwargs = dict(
        database=MagicMock(name="Database"),
        api_client=MagicMock(name="APIClient"),
        table_name="tbl",
        schema={"a": "INT"},
        intent="train",
        category=None,
    )
    kwargs.update(overrides)
    return _FakeIngestor(**kwargs)


def test_semseg_schema_less_auto_adds_mask_id():
    # A semseg dataset whose schema omits mask_id still gets a stored mask_id
    # column — the masks sidecar's required link_column.
    ing = _make(
        category="semantic_segmentation", schema={"image_label": "VARCHAR(255)"}
    )
    assert ing.schema.get("mask_id") == "VARCHAR(255)"
    # ...and it reaches the CREATE TABLE schema (not just self.schema): the
    # column must actually exist, or the row insert fails with "Unconsumed
    # column names: mask_id". This assertion is what catches the sourcing bug.
    assert ing._table_schema.get("mask_id") == "VARCHAR(255)"
    # the user's declared column is left exactly as given
    assert ing.schema["image_label"] == "VARCHAR(255)"


def test_semseg_declared_mask_id_preserved():
    # An explicitly declared mask_id is untouched (no clobber, no duplicate).
    ing = _make(
        category="semantic_segmentation",
        schema={"mask_id": "VARCHAR(255)", "image_label": "VARCHAR(255)"},
    )
    assert ing.schema["mask_id"] == "VARCHAR(255)"


def test_non_sidecar_category_gets_no_mask_id():
    # image_classification has no sidecar link_column → no auto-add.
    ing = _make(category="image_classification", schema={"a": "INT"})
    assert "mask_id" not in ing.schema


def test_object_detection_sidecar_without_link_column_not_added():
    # object_detection's annotations sidecar has NO link_column (paired by
    # filename), so the rule is link_column-gated, not "any sidecar".
    ing = _make(category="object_detection", schema={"a": "INT"})
    assert "mask_id" not in ing.schema


def test_semseg_ingested_row_carries_populated_mask_id():
    # End-to-end at the ingestor boundary: a schema-less semseg run's effective
    # schema → RecordProcessor → the stored row carries the per-row mask_id
    # VALUE. This is the exact ingestor→client contract backend#816 is about; it
    # fails under develop's old blanket mask_id pop (#212).
    ing = _make(
        category="semantic_segmentation", schema={"image_label": "VARCHAR(255)"}
    )
    # Both halves of the stored contract: the column is CREATED (create_table
    # reads _table_schema) and the per-row VALUE is kept (RecordProcessor reads
    # self.schema). The develop regression slipped through exactly this boundary.
    assert "mask_id" in ing._table_schema
    # RecordProcessor is built from self.schema in the real ingestor, so mirror
    # that here (self.schema carries the auto-added mask_id too).
    rp = RecordProcessor(
        schema=ing.schema,
        intent="train",
        label_column=None,
        annotation_column=None,
        unique_id_column=None,
        label_policy=None,
        ingestor_id="t",
    )
    row = rp.process(
        {
            "filename": "img1.png",
            "extension": "png",
            "mask_id": "mask1.png",
            "image_label": "cat",
        }
    )
    assert row is not None
    assert row.get("mask_id") == "mask1.png"
