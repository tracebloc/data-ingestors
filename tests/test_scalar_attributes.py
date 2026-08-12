"""Tests for auto-detected scalar attributes emitted for combine-time alignment
(di#360): image ``resolution`` and text ``encoding`` from the base hook, and
time-series ``timezone`` / ``sampling_frequency`` from the CSV timestamp pass.
"""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock

from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
from tracebloc_ingestor.utils.constants import TaskCategory, DataFormat


def make_ingestor(schema=None, **overrides):
    db = MagicMock()
    db.create_table.return_value = MagicMock()
    api = MagicMock()
    kwargs = dict(
        database=db,
        api_client=api,
        table_name="tbl",
        schema=schema if schema is not None else {"a": "INT"},
        intent="train",
        category=None,
    )
    kwargs.update(overrides)
    return CSVIngestor(**kwargs)


# ---------------------------------------------------------------------------
# Image resolution — from the uniform target_size (no image read)
# ---------------------------------------------------------------------------
def test_image_resolution_from_target_size_height_width():
    ing = make_ingestor(
        schema={"filename": "VARCHAR(64)"},
        category=TaskCategory.IMAGE_CLASSIFICATION,
        data_format=DataFormat.IMAGE,
        file_options={"target_size": [640, 480]},  # [width, height]
    )
    attrs = ing._collect_run_metadata()["attributes"]
    assert attrs["resolution"] == [480, 640]  # emitted [height, width]


def test_color_mode_and_derived_channels_from_file_options():
    ing = make_ingestor(
        schema={"filename": "VARCHAR(64)"},
        category=TaskCategory.IMAGE_CLASSIFICATION,
        data_format=DataFormat.IMAGE,
        file_options={"color_mode": "rgb"},  # user-provided, case-insensitive
    )
    attrs = ing._collect_run_metadata()["attributes"]
    assert attrs["color_mode"] == "RGB"
    assert attrs["channels"] == 3


def test_grayscale_color_mode_channels():
    ing = make_ingestor(
        schema={"filename": "VARCHAR(64)"},
        category=TaskCategory.SEMANTIC_SEGMENTATION,
        data_format=DataFormat.IMAGE,
        file_options={"color_mode": "grayscale", "bit_depth": 16},
    )
    attrs = ing._collect_run_metadata()["attributes"]
    assert attrs["color_mode"] == "grayscale"
    assert attrs["channels"] == 1
    assert attrs["bit_depth"] == 16


def test_invalid_bit_depth_in_file_options_is_skipped():
    # bit_depth set directly in file_options bypasses conventions.resolve()'s
    # 8/16 gate; the base hook must still drop a value the contract rejects
    # (mirrors color_mode canonicalisation).
    ing = make_ingestor(
        schema={"filename": "VARCHAR(64)"},
        category=TaskCategory.IMAGE_CLASSIFICATION,
        data_format=DataFormat.IMAGE,
        file_options={"color_mode": "rgb", "bit_depth": 12},
    )
    attrs = ing._collect_run_metadata()["attributes"]
    assert "bit_depth" not in attrs  # 12 is not a contract-accepted depth
    assert attrs["color_mode"] == "RGB"  # the valid sibling still emits


def test_invalid_color_mode_in_file_options_is_skipped():
    # Directly-set (unvalidated) file_options with a non-canonical mode: the base
    # hook emits nothing rather than shipping a value the contract would reject.
    ing = make_ingestor(
        schema={"filename": "VARCHAR(64)"},
        category=TaskCategory.IMAGE_CLASSIFICATION,
        data_format=DataFormat.IMAGE,
        file_options={"color_mode": "RGBA"},
    )
    attrs = ing._collect_run_metadata().get("attributes", {})
    assert "color_mode" not in attrs
    assert "channels" not in attrs


def test_no_color_mode_when_absent():
    ing = make_ingestor(
        schema={"filename": "VARCHAR(64)"},
        category=TaskCategory.IMAGE_CLASSIFICATION,
        data_format=DataFormat.IMAGE,
        file_options={"target_size": [64, 64]},
    )
    attrs = ing._collect_run_metadata()["attributes"]
    assert "color_mode" not in attrs and "channels" not in attrs


def test_no_resolution_for_non_image():
    ing = make_ingestor(
        schema={"a": "INT"},
        category=TaskCategory.TABULAR_CLASSIFICATION,
        data_format=DataFormat.TABULAR,
        file_options={"target_size": [640, 480]},
    )
    assert "resolution" not in ing._collect_run_metadata().get("attributes", {})


# ---------------------------------------------------------------------------
# Text encoding — utf-8 for NLP categories
# ---------------------------------------------------------------------------
def test_text_encoding_utf8_for_nlp():
    ing = make_ingestor(
        schema={"filename": "VARCHAR(64)", "label": "VARCHAR(8)"},
        category=TaskCategory.TEXT_CLASSIFICATION,
        data_format=DataFormat.TEXT,
        label_column="label",
    )
    attrs = ing._collect_run_metadata()["attributes"]
    assert attrs["encoding"] == "utf-8"


def test_no_encoding_for_tabular():
    ing = make_ingestor(
        schema={"a": "INT"}, category=TaskCategory.TABULAR_CLASSIFICATION
    )
    assert "encoding" not in ing._collect_run_metadata().get("attributes", {})


# ---------------------------------------------------------------------------
# Temporal — timezone + sampling_frequency for time_series_forecasting
# ---------------------------------------------------------------------------
def test_temporal_timezone_and_frequency(make_csv):
    path = make_csv(
        {
            "timestamp": [
                "2024-01-01 00:00:00+00:00",
                "2024-01-01 01:00:00+00:00",
                "2024-01-01 02:00:00+00:00",
                "2024-01-01 03:00:00+00:00",
            ],
            "value": [1.0, 2.0, 3.0, 4.0],
        }
    )
    ing = make_ingestor(
        schema={"timestamp": "TIMESTAMP", "value": "FLOAT"},
        category=TaskCategory.TIME_SERIES_FORECASTING,
        data_format=DataFormat.TABULAR,
    )
    list(ing.read_data(str(path)))

    attrs = ing._collect_run_metadata()["attributes"]
    assert attrs["timezone"] == "UTC"
    # Regular hourly cadence -> pandas infers a frequency (alias spelling is
    # pandas-version dependent, so assert presence, not the exact string).
    assert attrs["sampling_frequency"]


def test_temporal_naive_timestamps_no_timezone(make_csv):
    path = make_csv(
        {
            "timestamp": [
                "2024-01-01 00:00:00",
                "2024-01-02 00:00:00",
                "2024-01-03 00:00:00",
            ],
            "value": [1.0, 2.0, 3.0],
        }
    )
    ing = make_ingestor(
        schema={"timestamp": "TIMESTAMP", "value": "FLOAT"},
        category=TaskCategory.TIME_SERIES_FORECASTING,
    )
    list(ing.read_data(str(path)))

    attrs = ing._collect_run_metadata()["attributes"]
    assert "timezone" not in attrs  # tz-naive
    assert attrs["sampling_frequency"]  # daily cadence still inferable


def test_temporal_not_emitted_for_non_time_series(make_csv):
    # A timestamp column outside time-series forecasting contributes no temporal
    # attributes.
    path = make_csv(
        {"timestamp": ["2024-01-01 00:00:00", "2024-01-02 00:00:00"], "a": [1, 2]}
    )
    ing = make_ingestor(
        schema={"timestamp": "TIMESTAMP", "a": "INT"},
        category=TaskCategory.TABULAR_REGRESSION,
    )
    list(ing.read_data(str(path)))

    attrs = ing._collect_run_metadata().get("attributes", {})
    assert "timezone" not in attrs
    assert "sampling_frequency" not in attrs


# ---------------------------------------------------------------------------
# Text alignment facts: language + normalization (uploader-declared, text-only)
# ---------------------------------------------------------------------------
def test_language_and_normalization_emitted_for_text():
    ing = make_ingestor(
        schema={"filename": "VARCHAR(64)", "label": "VARCHAR(8)"},
        category=TaskCategory.TEXT_CLASSIFICATION,
        data_format=DataFormat.TEXT,
        label_column="label",
        file_options={"language": "en", "normalization": "nfc"},
    )
    attrs = ing._collect_run_metadata()["attributes"]
    assert attrs["encoding"] == "utf-8"
    assert attrs["language"] == "en"
    assert attrs["normalization"] == "nfc"


def test_text_facts_absent_when_not_declared():
    ing = make_ingestor(
        schema={"filename": "VARCHAR(64)", "label": "VARCHAR(8)"},
        category=TaskCategory.TEXT_CLASSIFICATION,
        data_format=DataFormat.TEXT,
        label_column="label",
    )
    attrs = ing._collect_run_metadata()["attributes"]
    assert "language" not in attrs and "normalization" not in attrs


def test_no_text_facts_for_embeddings():
    # embeddings is NLP but NOT a text category in the contract — the backend
    # rejects text facts on it, so none must be emitted (not even encoding).
    ing = make_ingestor(
        schema={"filename": "VARCHAR(64)"},
        category=TaskCategory.EMBEDDINGS,
        data_format=DataFormat.TEXT,
        file_options={"language": "en", "normalization": "nfc"},
    )
    attrs = ing._collect_run_metadata().get("attributes", {})
    assert "encoding" not in attrs
    assert "language" not in attrs
    assert "normalization" not in attrs


# ---------------------------------------------------------------------------
# Survival alignment facts: time_unit + event_indicator (uploader-declared)
# ---------------------------------------------------------------------------
def test_survival_facts_emitted_for_time_to_event():
    ing = make_ingestor(
        schema={"duration": "FLOAT", "label": "INT"},
        category=TaskCategory.TIME_TO_EVENT_PREDICTION,
        label_column="label",
        file_options={
            "time_unit": "days",
            "event_indicator": {"event": 1, "censored": 0},
        },
    )
    attrs = ing._collect_run_metadata()["attributes"]
    assert attrs["time_unit"] == "days"
    assert attrs["event_indicator"] == {"event": 1, "censored": 0}


def test_survival_facts_reject_bad_shape_at_emission():
    # A bad value that bypassed conventions.resolve (set straight on file_options)
    # is dropped, not emitted — it can never reach the contract as a 400.
    ing = make_ingestor(
        schema={"duration": "FLOAT", "label": "INT"},
        category=TaskCategory.TIME_TO_EVENT_PREDICTION,
        label_column="label",
        file_options={
            "time_unit": "fortnights",
            "event_indicator": {"event": True, "censored": 0},
        },
    )
    attrs = ing._collect_run_metadata().get("attributes", {})
    assert "time_unit" not in attrs
    assert "event_indicator" not in attrs


def test_survival_facts_not_emitted_for_tabular():
    ing = make_ingestor(
        schema={"a": "INT"},
        category=TaskCategory.TABULAR_CLASSIFICATION,
        file_options={"time_unit": "days"},
    )
    attrs = ing._collect_run_metadata().get("attributes", {})
    assert "time_unit" not in attrs


# ---------------------------------------------------------------------------
# Embeddings alignment fact: positive_definition (uploader-declared)
# ---------------------------------------------------------------------------
def test_positive_definition_emitted_for_embeddings():
    ing = make_ingestor(
        schema={"filename": "VARCHAR(64)"},
        category=TaskCategory.EMBEDDINGS,
        data_format=DataFormat.TEXT,
        file_options={"positive_definition": "same-question paraphrase"},
    )
    attrs = ing._collect_run_metadata()["attributes"]
    assert attrs["positive_definition"] == "same-question paraphrase"
    # embeddings is not a text category — no encoding/language leaks in.
    assert "encoding" not in attrs and "language" not in attrs


def test_positive_definition_not_emitted_for_text():
    ing = make_ingestor(
        schema={"filename": "VARCHAR(64)", "label": "VARCHAR(8)"},
        category=TaskCategory.TEXT_CLASSIFICATION,
        data_format=DataFormat.TEXT,
        label_column="label",
        file_options={"positive_definition": "nope"},
    )
    attrs = ing._collect_run_metadata()["attributes"]
    assert "positive_definition" not in attrs


def test_temporal_frequency_inferred_for_date_only_schema(make_csv):
    """A date-only (``DATE``) timestamp column must still yield a cadence.

    Daily series are the commonest forecasting shape, and since #489 an
    INFERRED schema types a date-only column ``DATE`` (inference never emits
    ``TIMESTAMP``). Only the DATETIME/TIMESTAMP cast branch accumulated the
    timestamp sample, so a date-only dataset ingested cleanly but silently
    omitted ``sampling_frequency`` from run metadata — a backend WARN with no
    local signal (Bugbot on #490).
    """
    path = make_csv(
        {
            "timestamp": ["2023-10-01", "2023-10-02", "2023-10-03", "2023-10-04"],
            "value": [1.0, 2.0, 3.0, 4.0],
        }
    )
    ing = make_ingestor(
        schema={"timestamp": "DATE", "value": "FLOAT"},
        category=TaskCategory.TIME_SERIES_FORECASTING,
    )
    chunks = list(ing.read_data(str(path)))

    attrs = ing._collect_run_metadata()["attributes"]
    assert attrs["sampling_frequency"], "date-only series lost its cadence"

    # …and the DATE branch's own contract still holds: the stored value stays a
    # plain date. Accumulating the cadence must read the parsed datetimes
    # WITHOUT reintroducing a spurious '00:00:00' into what gets written.
    stored = [record["timestamp"] for record in chunks]
    assert stored, "no records were read"
    assert all(
        isinstance(v, date) and not isinstance(v, datetime) for v in stored
    ), f"DATE column gained a time component: {stored[:2]}"
