"""backend#3644 — the dataset's physical ``size_bytes`` on the ingest summary.

The ingest summary now reports how many BYTES the dataset actually occupies, so
the platform can show a real size instead of an em dash. Nothing has ever
measured it before, so there is no backfill and no second source to check the
number against: whatever the ingestor reports IS the platform's answer. That
puts three properties under test here, each of which would publish a wrong
figure if it broke.

1. **It is measured, and it is in bytes.** The reported value equals the sum of
   ``os.path.getsize`` over the files the run actually staged — asserted
   against real staged files from a real ingest, so a unit slip (KB, MiB, a
   float GB) or a derived-from-row-count shortcut fails here.

2. **Unknown is OMITTED, never 0.** On the backend ``NULL`` means "not
   reported" and renders an em dash, while ``0`` is a positive claim that the
   dataset holds no data, and nothing coerces one into the other. So a category
   whose data lives in MySQL rather than on disk sends no field at all, and a
   measurement that could not be completed sends no field either.

3. **An overwriting duplicate is counted once.** A manifest may name the same
   file twice — a WARNING, not an error (``duplicate_validator``) — and the
   second copy overwrites the first. A running sum would report a dataset
   larger than the one on disk.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock, patch

import pytest

from tracebloc_ingestor import file_transfer
from tracebloc_ingestor.cli import run as run_mod
from tracebloc_ingestor.cli.conventions import resolve
from tracebloc_ingestor.config import Config
from tracebloc_ingestor.ingestors import base as base_mod
from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
from tracebloc_ingestor.utils.constants import DataFormat, TaskCategory
from tracebloc_ingestor.utils.staged_bytes import (
    StagedBytes,
    measure_staged_bytes,
    record_staged_file,
)

REPO = Path(__file__).resolve().parents[1]
T = REPO / "templates"


# ---------------------------------------------------------------------------
# The meter itself
# ---------------------------------------------------------------------------


def test_meter_sums_the_bytes_of_each_staged_file(tmp_path):
    a = tmp_path / "a.bin"
    b = tmp_path / "b.bin"
    a.write_bytes(b"x" * 10)
    b.write_bytes(b"y" * 25)

    meter = StagedBytes()
    meter.record(str(a))
    meter.record(str(b))

    assert meter.total == 35
    assert meter.file_count == 2


def test_meter_counts_an_overwritten_path_once(tmp_path):
    """A duplicate manifest row re-copies over the SAME destination path.

    Only one file is on disk afterwards, so only its current size counts. A
    running sum would report 10 + 40 = 50 bytes for the 40 bytes that exist.
    """
    dest = tmp_path / "dup.bin"
    meter = StagedBytes()

    dest.write_bytes(b"x" * 10)
    meter.record(str(dest))
    dest.write_bytes(b"y" * 40)
    meter.record(str(dest))

    assert meter.total == 40
    assert meter.file_count == 1


def test_meter_that_could_not_stat_a_file_reports_nothing(tmp_path):
    """An unmeasurable file makes the whole total unreportable.

    The alternative — returning the short sum — publishes a figure the backend
    cannot tell from a complete one. ``None`` is the omit signal.
    """
    good = tmp_path / "good.bin"
    good.write_bytes(b"x" * 10)

    meter = StagedBytes()
    meter.record(str(good))
    assert meter.total == 10

    meter.record(str(tmp_path / "vanished.bin"))
    assert meter.total is None
    # Latched: a later successful measurement does not un-break it.
    meter.record(str(good))
    assert meter.total is None


def test_recording_outside_a_measured_block_is_a_noop(tmp_path):
    """A direct caller of the transfer primitives — a template script, a test —
    behaves exactly as it did before the meter existed."""
    f = tmp_path / "a.bin"
    f.write_bytes(b"x" * 7)
    record_staged_file(str(f))  # must not raise, must not need a meter


def test_each_measured_block_gets_its_own_meter(tmp_path):
    """Two ingests in one process (the e2e harness does this) must not
    accumulate into each other's total."""
    f = tmp_path / "a.bin"
    f.write_bytes(b"x" * 7)

    with measure_staged_bytes() as outer:
        record_staged_file(str(f))
        with measure_staged_bytes() as inner:
            record_staged_file(str(f))
            assert inner.total == 7
        assert outer.total == 7
        record_staged_file(str(f))
        assert outer.total == 7  # same path, still one file


# ---------------------------------------------------------------------------
# The copy funnel — the one place a staged file is reported from
# ---------------------------------------------------------------------------


def test_the_copy_funnel_reports_the_bytes_it_wrote(tmp_path):
    src = tmp_path / "src.bin"
    dest = tmp_path / "dest.bin"
    src.write_bytes(b"z" * 123)

    with measure_staged_bytes() as meter:
        file_transfer._copy_file_with_retry(str(src), str(dest))

    assert meter.total == 123
    assert meter.total == os.path.getsize(dest)


def test_the_copy_funnel_still_copies_without_a_meter(tmp_path):
    src = tmp_path / "src.bin"
    dest = tmp_path / "dest.bin"
    src.write_bytes(b"z" * 5)

    file_transfer._copy_file_with_retry(str(src), str(dest))

    assert dest.read_bytes() == b"z" * 5


# ---------------------------------------------------------------------------
# The wire payload
# ---------------------------------------------------------------------------


def _client():
    from tests.test_api_client_methods import _client as make_client

    return make_client()


def _payload(**overrides) -> Dict[str, Any]:
    from tests.test_api_client_methods import _captured_summary_payload

    return _captured_summary_payload(_client(), **overrides)


def test_summary_includes_size_bytes_when_measured():
    sent = _payload(size_bytes=1234567)
    assert sent["size_bytes"] == 1234567


def test_summary_omits_size_bytes_when_unmeasured():
    """Default ``None`` omits the field entirely, so the payload stays
    byte-identical to today's and a backend predating the field is
    unaffected — the two repos ship in either order."""
    sent = _payload()
    assert "size_bytes" not in sent


def test_summary_omits_size_bytes_rather_than_sending_null():
    sent = _payload(size_bytes=None)
    assert "size_bytes" not in sent
    assert "size_bytes" not in json.dumps(sent)


def test_summary_sends_a_measured_zero():
    """The mirror image of the rule: ``0`` reaching this call means the caller
    MEASURED zero bytes, and a truthiness guard here would silently turn that
    into "not reported". Only the caller may decide to omit."""
    sent = _payload(size_bytes=0)
    assert sent["size_bytes"] == 0


# ---------------------------------------------------------------------------
# End to end: a real ingest, with real file transfer
# ---------------------------------------------------------------------------


def _cfg(**kw: Any) -> Dict[str, Any]:
    base = {"apiVersion": "tracebloc.io/v1", "kind": "IngestConfig", "intent": "train"}
    base.update(kw)
    return base


def _image_classification_config() -> Dict[str, Any]:
    return _cfg(
        table="size_img",
        category="image_classification",
        csv=str(T / "image_classification/data/labels_file_sample.csv"),
        images=str(T / "image_classification/data/images"),
        label="label",
        spec={"file_options": {"extension": ".jpeg", "target_size": [256, 256]}},
    )


def _object_detection_config() -> Dict[str, Any]:
    return _cfg(
        table="size_od",
        category="object_detection",
        csv=str(T / "object_detection/data/labels_file_sample.csv"),
        images=str(T / "object_detection/data/images"),
        annotations=str(T / "object_detection/data/annotations"),
        label="image_label",
        target_size=[1920, 1080],
    )


def _run_real_ingest(
    config: Dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Tuple[Config, Dict[str, Any], List[Dict[str, Any]]]:
    """Run one real ingest; return ``(run_config, summary_kwargs, rows)``.

    Same fake boundary as ``test_ingest_storage_contract``: MySQL and the
    backend API are mocked at their own edges, so validation, record
    processing and — the point here — FILE TRANSFER are all production code
    writing real bytes to a real directory.
    """
    resolved = resolve(config)
    run_config = run_mod._resolve_config(resolved)

    storage = tmp_path / "shared"
    storage.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(Config, "STORAGE_PATH", str(storage))
    monkeypatch.setenv("CLIENT_ENV", "local")

    rows: List[Dict[str, Any]] = []

    database = MagicMock(name="Database")
    database.config = run_config
    database.engine = MagicMock(name="Engine")
    database.create_table.return_value = MagicMock(name="Table")
    database.get_table_schema.return_value = resolved.schema
    database.get_or_create_table_salt.return_value = "3644" * 16
    database.get_label_counts.return_value = {"a": 1}
    database.get_class_histogram_counts.return_value = {"a": 1}
    database.get_samples.return_value = []

    def _insert_batch(_table: str, records: List[Dict[str, Any]]):
        rows.extend(dict(record) for record in records)
        return list(range(len(records))), []

    database.insert_batch.side_effect = _insert_batch

    api_client = MagicMock(name="APIClient")
    api_client.config.TITLE = "size probe"
    api_client.send_ingest_summary.return_value = {"dataset_id": 1}

    ingestor = run_mod._build_ingestor(database, api_client, resolved)
    ingestor.ingest(resolved.source_path)

    assert rows, "the ingest stored no rows"
    api_client.send_ingest_summary.assert_called_once()
    return run_config, api_client.send_ingest_summary.call_args.kwargs, rows


def _bytes_on_disk(dest: str) -> int:
    return sum(
        os.path.getsize(os.path.join(root, name))
        for root, _dirs, files in os.walk(dest)
        for name in files
    )


def test_image_ingest_reports_the_bytes_it_staged(tmp_path, monkeypatch):
    """The headline: a real image ingest reports a real byte count.

    Equality with ``os.path.getsize`` over the destination tree pins BOTH that
    the number is measured (not derived from the row count — there is no
    bytes-per-row constant for any data format) and that its UNIT is bytes. A
    KB/MiB/float-GB slip, or an off-by-a-factor rounding, fails here.
    """
    run_config, summary_kwargs, _rows = _run_real_ingest(
        _image_classification_config(), tmp_path, monkeypatch
    )
    dest = run_config.DEST_PATH
    expected = _bytes_on_disk(dest)

    assert summary_kwargs["size_bytes"] == expected
    assert isinstance(summary_kwargs["size_bytes"], int)
    assert not isinstance(summary_kwargs["size_bytes"], bool)
    # A floor on the UNIT: the template images are ~20-30 KB each, so a value
    # that had been divided down into KB / MiB / GB would be a two- or
    # three-digit number here, or 0.
    assert summary_kwargs["size_bytes"] > 10_000 * len(os.listdir(dest))


def test_reported_size_matches_the_source_files_byte_for_byte(tmp_path, monkeypatch):
    """The staged copies are byte-identical to their sources, so the reported
    size must equal the sum over the DISTINCT source images the manifest named.

    Pins the measurement against data living OUTSIDE the destination tree, so
    it cannot be satisfied by measuring the wrong thing consistently.
    """
    _run_config, summary_kwargs, rows = _run_real_ingest(
        _image_classification_config(), tmp_path, monkeypatch
    )
    images = T / "image_classification/data/images"
    named = {f"{row['filename']}{row['extension']}" for row in rows}
    expected = sum(os.path.getsize(images / name) for name in named)

    assert summary_kwargs["size_bytes"] == expected


def test_a_manifest_that_names_one_image_many_times_is_not_multiplied(
    tmp_path, monkeypatch
):
    """The duplicate rule, on real shipped data rather than a contrivance.

    The bundled image_classification manifest is 576 rows over SIX images — a
    ~96x duplication that the duplicate validator passes with a warning. Every
    row re-copies its image over the same destination path, so the dataset on
    disk is six files. A running sum would have reported ~16 MB for the ~170 KB
    that exist, and nothing downstream could have told that figure from a
    measured one.
    """
    run_config, summary_kwargs, rows = _run_real_ingest(
        _image_classification_config(), tmp_path, monkeypatch
    )
    dest = run_config.DEST_PATH
    staged = os.listdir(dest)

    assert len(rows) > 10 * len(staged), "fixture no longer exercises duplicates"
    assert summary_kwargs["size_bytes"] == _bytes_on_disk(dest)

    images = T / "image_classification/data/images"
    naive_per_row_sum = sum(
        os.path.getsize(images / f"{row['filename']}{row['extension']}") for row in rows
    )
    assert summary_kwargs["size_bytes"] < naive_per_row_sum / 10


def test_object_detection_counts_the_annotation_sidecars_too(tmp_path, monkeypatch):
    """A category staging TWO files per row reports both. The image bytes alone
    would be an under-report of a dataset whose annotations are part of it."""
    run_config, summary_kwargs, rows = _run_real_ingest(
        _object_detection_config(), tmp_path, monkeypatch
    )
    dest = run_config.DEST_PATH
    staged = os.listdir(dest)

    assert summary_kwargs["size_bytes"] == _bytes_on_disk(dest)
    assert len(staged) == 2 * len(rows)  # one image + one .xml per row
    xml_bytes = sum(
        os.path.getsize(os.path.join(dest, name))
        for name in staged
        if name.endswith(".xml")
    )
    assert xml_bytes > 0
    assert summary_kwargs["size_bytes"] > _bytes_on_disk(dest) - xml_bytes


# ---------------------------------------------------------------------------
# The omitted case: a category with no files on disk
# ---------------------------------------------------------------------------

_TABULAR_SCHEMA = {"heart_rate": "INT", "label": "INT"}


def _tabular_ingest(csv_path: Path) -> Dict[str, Any]:
    database = MagicMock(name="Database")
    database.config = Config(TABLE_NAME="size_tab")
    database.get_or_create_table_salt.return_value = "0" * 64
    database.create_table.return_value = MagicMock(name="table")
    database.insert_batch.side_effect = lambda table, batch: (
        list(range(len(batch))),
        [],
    )
    database.get_table_schema.return_value = dict(_TABULAR_SCHEMA)
    database.get_label_counts.return_value = {"0": 2, "1": 1}
    database.get_samples.return_value = []

    api_client = MagicMock(name="APIClient")
    api_client.config.TITLE = "size tab"
    api_client.send_ingest_summary.return_value = {"dataset_id": 1}

    ingestor = CSVIngestor(
        database=database,
        api_client=api_client,
        table_name="size_tab",
        schema=dict(_TABULAR_SCHEMA),
        label_column="label",
        intent="train",
        category=TaskCategory.TABULAR_CLASSIFICATION,
        data_format=DataFormat.TABULAR,
    )
    with patch.object(base_mod, "Session") as session:
        session.return_value.__enter__.return_value = MagicMock()
        ingestor.ingest(str(csv_path), batch_size=50)

    api_client.send_ingest_summary.assert_called_once()
    return api_client.send_ingest_summary.call_args.kwargs


def test_a_db_resident_category_omits_the_field_rather_than_reporting_zero(tmp_path):
    """tabular / time-series / survival data lives in MySQL rows, not on disk,
    so this run stages no files and has nothing to measure.

    ``None`` -> the field is omitted -> the backend stores ``NULL`` = "not
    reported" and renders an em dash. Reporting the 0 bytes it staged would
    instead claim the dataset holds NO DATA, which is both wrong and
    indistinguishable from a measured 0.
    """
    csv_path = tmp_path / "tab.csv"
    csv_path.write_text("heart_rate,label\n70,0\n71,1\n72,0\n", encoding="utf-8")

    summary_kwargs = _tabular_ingest(csv_path)

    assert summary_kwargs["size_bytes"] is None
    # And the client then drops it from the wire payload, rather than sending
    # `"size_bytes": null` or coercing it to 0.
    sent = _payload(size_bytes=summary_kwargs["size_bytes"])
    assert "size_bytes" not in sent
