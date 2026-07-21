"""#225: deterministic salted content-hash data_id (opt-in, landed dark).

A k8s Job retry re-runs the whole ingest in a fresh pod; with uuid ids every
row re-inserts. With content_hash, the retry reproduces the same ids and the
data_id UNIQUE upsert re-claims the prior attempt's rows instead of
duplicating them. The per-table salt keeps ids unlinkable across tables and
never leaves the cluster.
"""

import json

import pytest

from tracebloc_ingestor.cli.conventions import resolve
from tracebloc_ingestor.ingestors.record_processor import RecordProcessor
from tracebloc_ingestor.utils import label_policy as label_policy_module


SALT_A = "a" * 64
SALT_B = "b" * 64


def make_processor(salt=SALT_A, strategy="content_hash", unique_id_column=None):
    return RecordProcessor(
        schema={"feature": "FLOAT", "note": "TEXT"},
        intent="train",
        label_column="target",
        annotation_column=None,
        unique_id_column=unique_id_column,
        label_policy=label_policy_module.PASSTHROUGH,
        ingestor_id="run-1",
        data_id_strategy=strategy,
        table_salt=salt,
    )


def _record(**over):
    base = {"feature": 1.5, "note": "hello", "target": "cat"}
    base.update(over)
    return base


# ── determinism ──────────────────────────────────────────────────────────────


def test_same_record_same_salt_same_id_across_instances():
    """The whole point: a retried process (fresh instance, fresh ingestor_id)
    reproduces the identical data_id for identical source content."""
    a = make_processor().process(_record())
    retry = RecordProcessor(
        schema={"feature": "FLOAT", "note": "TEXT"},
        intent="train",
        label_column="target",
        annotation_column=None,
        unique_id_column=None,
        label_policy=label_policy_module.PASSTHROUGH,
        ingestor_id="run-2-after-retry",  # different run
        data_id_strategy="content_hash",
        table_salt=SALT_A,
    ).process(_record())
    assert a["data_id"] == retry["data_id"]
    assert len(a["data_id"]) == 64  # sha256 hexdigest


def test_different_content_different_id():
    a = make_processor().process(_record())
    b = make_processor().process(_record(feature=2.5))
    assert a["data_id"] != b["data_id"]


def test_different_salt_different_id():
    """Per-table salt: identical content in two tables is unlinkable."""
    a = make_processor(salt=SALT_A).process(_record())
    b = make_processor(salt=SALT_B).process(_record())
    assert a["data_id"] != b["data_id"]


def test_label_participates_in_the_hash():
    """Same features, different label ⇒ different id (a relabeled source row
    is a different record, not a silent overwrite)."""
    a = make_processor().process(_record(target="cat"))
    b = make_processor().process(_record(target="dog"))
    assert a["data_id"] != b["data_id"]


# ── precedence + guards ──────────────────────────────────────────────────────


def test_unique_id_column_wins_over_content_hash():
    """Belt-and-suspenders: an explicit source-id column takes precedence."""
    p = make_processor(unique_id_column="feature")
    out = p.process(_record(feature="row-77"))
    assert out["data_id"] == "row-77"


def test_uuid_default_unchanged():
    """Landed dark: without opting in, ids stay per-process-random."""
    p = make_processor(strategy="uuid", salt=None)
    a = p.process(_record())
    b = p.process(_record())
    assert a["data_id"] != b["data_id"]  # uuid4 per record


def test_content_hash_without_salt_fails_at_construction():
    with pytest.raises(ValueError, match="table_salt"):
        make_processor(salt=None)


# ── conventions resolution ───────────────────────────────────────────────────


def _cfg(strategy=None):
    cfg = {
        "apiVersion": "tracebloc.io/v1",
        "kind": "IngestConfig",
        "table": "t1",
        "category": "tabular_classification",
        "csv": "/data/shared/d.csv",
        "intent": "train",
        "label": "target",
        "schema": {"feature": "FLOAT"},
    }
    if strategy:
        cfg["data_id"] = {"strategy": strategy}
        if strategy == "column":
            cfg["data_id"]["column"] = "feature"
    return cfg


def test_resolve_default_is_content_hash():
    # #350: content_hash is now the default when no data_id block is present.
    r = resolve(_cfg())
    assert r.data_id_strategy == "content_hash"
    assert r.unique_id_column is None


def test_resolve_content_hash():
    r = resolve(_cfg("content_hash"))
    assert r.data_id_strategy == "content_hash"
    assert r.unique_id_column is None


def test_resolve_uuid_is_explicit_opt_out():
    # #350: after the default flipped, strategy=uuid must still opt back in.
    r = resolve(_cfg("uuid"))
    assert r.data_id_strategy == "uuid"
    assert r.unique_id_column is None


def test_resolve_column_still_sets_unique_id_column():
    r = resolve(_cfg("column"))
    assert r.unique_id_column == "feature"
    # column path maps unique_id_column and leaves data_id_strategy at its
    # default; unique_id_column wins over the strategy in RecordProcessor.
    assert r.data_id_strategy == "content_hash"


# ── schema acceptance ────────────────────────────────────────────────────────


def _validator():
    from jsonschema import Draft7Validator
    from tracebloc_ingestor.cli.run import _load_schema

    return Draft7Validator(_load_schema())


def test_schema_accepts_content_hash_strategy():
    errors = list(_validator().iter_errors(_cfg("content_hash")))
    assert errors == []


def test_schema_still_requires_column_for_column_strategy():
    cfg = _cfg("column")
    del cfg["data_id"]["column"]
    errors = list(_validator().iter_errors(cfg))
    assert errors, "strategy=column without column must be rejected"


def test_filename_participates_in_the_hash():
    """File-bearing categories: the schema-filtered record may be nothing but
    {label, data_intent}, so the source filename must drive the id — two
    different images with the same label are different records, and the same
    image re-ingested on retry reproduces its id."""
    p = make_processor()
    a = p.process({"filename": "img_001.jpeg", "target": "cat"})
    b = p.process({"filename": "img_002.jpeg", "target": "cat"})
    retry = make_processor().process({"filename": "img_001.jpeg", "target": "cat"})
    assert a["data_id"] != b["data_id"]
    assert a["data_id"] == retry["data_id"]


# ── construction through the real builder (the gap that hid a TypeError) ────


def _build(config):
    from unittest.mock import MagicMock

    from tracebloc_ingestor.cli.run import _build_ingestor

    db = MagicMock()
    db.get_or_create_table_salt.return_value = SALT_A
    return _build_ingestor(database=db, api_client=MagicMock(), resolved=resolve(config))


@pytest.mark.parametrize("source_key", ["csv", "json"])
@pytest.mark.parametrize("strategy", [None, "content_hash"])
def test_build_ingestor_threads_strategy_through_both_subclasses(source_key, strategy):
    """CSVIngestor and JSONIngestor must accept + forward data_id_strategy —
    a kwarg passed by _build_ingestor that either subclass drops is a
    TypeError on EVERY YAML-driven run, even with pure defaults."""
    cfg = _cfg(strategy)
    if source_key == "json":
        cfg.pop("csv")
        cfg["json"] = "/data/shared/d.json"
    ing = _build(cfg)
    expected = strategy or "content_hash"  # #350: default flipped to content_hash
    assert ing.data_id_strategy == expected
    # salt is deferred to ingest(); never fetched at construction
    ing.database.get_or_create_table_salt.assert_not_called()
    assert ing._table_salt is None
