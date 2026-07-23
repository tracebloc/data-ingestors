"""Tests for the metadata backfill runner — the GET → recompute → POST sweep
that wires build_dataset_metadata to the backend metadata-backfill endpoint.

build_dataset_metadata and the APIClient are the units-under-test's collaborators
and are exercised elsewhere; here they are mocked so the runner's own
orchestration/skip/guard logic is what's covered.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from tracebloc_ingestor import metadata_backfill_runner as runner
from tracebloc_ingestor.metadata_backfill_runner import (
    STATUS_ERROR,
    STATUS_NOT_FOUND,
    STATUS_OK,
    STATUS_SKIPPED_COMPETITION,
    STATUS_SKIPPED_CURRENT,
    backfill_dataset,
    backfill_datasets,
)

# A pre-cutover plain dataset record as the GET returns it — no new-shape
# attributes yet, not a competition.
_PLAIN_RECORD = {
    "id": 1,
    "table_name": "tb",
    "category": "tabular_classification",
    "data_format": "tabular",
    "intent": "train",
    "ingestor_id": "ing-1",
    "is_competition": False,
    "source_dataset_ids": [],
    "schema": {"x": "INT"},
    "meta_data": {},
}

_PAYLOAD = {
    "schema": {"x": {"dtype": "int"}},
    "meta_data": {"attributes": {"feature_stats": {"x": {"count": 5}}}},
}


def _api(record=None, send_result=None):
    api = MagicMock()
    api.get_dataset_metadata.return_value = record
    api.send_metadata_backfill.return_value = send_result or {
        "table_name": "tb",
        "created": True,
        "competitions_refolded": 2,
    }
    return api


# ---------------------------------------------------------------------------
# backfill_dataset — single dataset
# ---------------------------------------------------------------------------


def test_backfill_dataset_happy_path_gets_builds_and_posts():
    api = _api(record=dict(_PLAIN_RECORD))
    db = MagicMock()
    with patch.object(runner, "build_dataset_metadata", return_value=_PAYLOAD) as build:
        result = backfill_dataset(db, api, "ing-1")

    # GET by ingestor_id, build keyed on the backend's category/data_format,
    # then POST the recomputed payload back.
    api.get_dataset_metadata.assert_called_once_with("ing-1")
    build.assert_called_once()
    assert build.call_args.kwargs["category"] == "tabular_classification"
    assert build.call_args.kwargs["data_format"] == "tabular"
    api.send_metadata_backfill.assert_called_once_with(
        "tb", _PAYLOAD["schema"], _PAYLOAD["meta_data"]
    )
    assert result.status == STATUS_OK
    assert result.created is True
    assert result.competitions_refolded == 2


def test_backfill_dataset_not_found_returns_status_and_skips_build():
    api = _api(record=None)  # GET 404 → None
    db = MagicMock()
    with patch.object(runner, "build_dataset_metadata") as build:
        result = backfill_dataset(db, api, "missing")
    assert result.status == STATUS_NOT_FOUND
    build.assert_not_called()
    api.send_metadata_backfill.assert_not_called()


def test_backfill_dataset_skips_competition():
    record = dict(_PLAIN_RECORD, is_competition=True)
    api = _api(record=record)
    db = MagicMock()
    with patch.object(runner, "build_dataset_metadata") as build:
        result = backfill_dataset(db, api, "ing-1")
    assert result.status == STATUS_SKIPPED_COMPETITION
    build.assert_not_called()
    api.send_metadata_backfill.assert_not_called()


def test_backfill_dataset_skips_merged_with_source_ids():
    record = dict(_PLAIN_RECORD, source_dataset_ids=[7, 8])
    api = _api(record=record)
    with patch.object(runner, "build_dataset_metadata") as build:
        result = backfill_dataset(MagicMock(), api, "ing-1")
    assert result.status == STATUS_SKIPPED_COMPETITION
    build.assert_not_called()


def test_backfill_dataset_skips_when_already_new_shape():
    record = dict(
        _PLAIN_RECORD,
        meta_data={"attributes": {"feature_stats": {"x": {"count": 1}}}},
    )
    api = _api(record=record)
    with patch.object(runner, "build_dataset_metadata") as build:
        result = backfill_dataset(MagicMock(), api, "ing-1")
    assert result.status == STATUS_SKIPPED_CURRENT
    build.assert_not_called()
    api.send_metadata_backfill.assert_not_called()


def test_backfill_dataset_reprocesses_current_when_skip_disabled():
    record = dict(
        _PLAIN_RECORD,
        meta_data={"attributes": {"feature_stats": {"x": {"count": 1}}}},
    )
    api = _api(record=record)
    with patch.object(runner, "build_dataset_metadata", return_value=_PAYLOAD):
        result = backfill_dataset(MagicMock(), api, "ing-1", skip_if_current=False)
    assert result.status == STATUS_OK
    api.send_metadata_backfill.assert_called_once()


def test_backfill_dataset_error_when_record_missing_category():
    record = dict(_PLAIN_RECORD, category=None)
    api = _api(record=record)
    with patch.object(runner, "build_dataset_metadata") as build:
        result = backfill_dataset(MagicMock(), api, "ing-1")
    assert result.status == STATUS_ERROR
    build.assert_not_called()


# ---------------------------------------------------------------------------
# backfill_datasets — the sweep
# ---------------------------------------------------------------------------


def test_backfill_datasets_enumerates_registered_runs_when_ids_omitted():
    db = MagicMock()
    db.list_registered_runs.return_value = [
        {"ingestor_id": "a", "table_name": "ta", "task": "tabular_classification"},
        {"ingestor_id": "b", "table_name": "tb", "task": "tabular_classification"},
    ]
    api = _api(record=dict(_PLAIN_RECORD))
    with patch.object(runner, "build_dataset_metadata", return_value=_PAYLOAD):
        results = backfill_datasets(db, api)
    db.list_registered_runs.assert_called_once()
    assert [r.ingestor_id for r in results] == ["a", "b"]
    assert all(r.status == STATUS_OK for r in results)


def test_backfill_datasets_continues_past_a_failing_dataset():
    api = MagicMock()
    # First GET raises (e.g. transient), second succeeds.
    api.get_dataset_metadata.side_effect = [
        RuntimeError("boom"),
        dict(_PLAIN_RECORD, ingestor_id="b"),
    ]
    api.send_metadata_backfill.return_value = {
        "table_name": "tb",
        "created": False,
        "competitions_refolded": 0,
    }
    with patch.object(runner, "build_dataset_metadata", return_value=_PAYLOAD):
        results = backfill_datasets(MagicMock(), api, ["a", "b"])
    assert results[0].status == STATUS_ERROR
    # Exception TYPE only — never the raw message (may embed customer cell values).
    assert results[0].error == "RuntimeError"
    assert results[1].status == STATUS_OK  # sweep continued
