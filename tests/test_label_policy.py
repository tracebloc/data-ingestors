"""Tests for the label-policy bucketing module + the BaseIngestor wiring.

The pure-function tests pin the bucketing contract:
  - passthrough is a no-op
  - bucket is stable, deterministic, in [0, NUM_BUCKETS)
  - missing values produce MISSING_LABEL_BUCKET
  - unknown policies raise

The boundary tests pin WHERE the policy fires (#486): inside
``APIClient.send_ingest_summary``, on both label-bearing payload fields, and
NOT on the record written to the cluster's MySQL — the row training reads keeps
the raw target.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tracebloc_ingestor.utils import label_policy
from tracebloc_ingestor.utils.label_policy import (
    BUCKET,
    MISSING_LABEL_BUCKET,
    NUM_BUCKETS,
    PASSTHROUGH,
)

# ---------------------------------------------------------------------------
# Pure function: apply()
# ---------------------------------------------------------------------------


class TestPassthroughPolicy:
    def test_string_value_unchanged(self):
        assert label_policy.apply("benign", PASSTHROUGH) == "benign"

    def test_numeric_value_unchanged(self):
        assert label_policy.apply(42, PASSTHROUGH) == 42

    def test_none_value_unchanged(self):
        assert label_policy.apply(None, PASSTHROUGH) is None

    def test_empty_string_unchanged(self):
        assert label_policy.apply("", PASSTHROUGH) == ""


class TestBucketPolicy:
    def test_returns_int_in_range(self):
        result = label_policy.apply("123.45", BUCKET)
        assert isinstance(result, int)
        assert 0 <= result < NUM_BUCKETS

    def test_stable_for_same_input(self):
        a = label_policy.apply(100, BUCKET)
        b = label_policy.apply(100, BUCKET)
        c = label_policy.apply("100", BUCKET)
        assert a == b == c  # same string repr → same bucket

    def test_different_inputs_likely_different_buckets(self):
        # Not a strict requirement (collisions exist) but a sanity check
        # against pathological hash collapse.
        buckets = {label_policy.apply(i, BUCKET) for i in range(1000)}
        # Should distribute reasonably across NUM_BUCKETS=64.
        assert len(buckets) >= NUM_BUCKETS // 2

    def test_none_value_returns_missing_sentinel(self):
        assert label_policy.apply(None, BUCKET) == MISSING_LABEL_BUCKET

    def test_empty_string_returns_missing_sentinel(self):
        assert label_policy.apply("", BUCKET) == MISSING_LABEL_BUCKET

    def test_whitespace_string_returns_missing_sentinel(self):
        assert label_policy.apply("   ", BUCKET) == MISSING_LABEL_BUCKET

    def test_nan_value_returns_missing_sentinel(self):
        # pandas renders missing numeric cells as float('nan'). `str(nan)`
        # is "nan" — non-empty — so an explicit isnan check is required
        # to keep NaN labels out of the regular bucket space.
        assert label_policy.apply(float("nan"), BUCKET) == MISSING_LABEL_BUCKET

    def test_missing_sentinel_outside_valid_range(self):
        # MISSING_LABEL_BUCKET must not collide with a real bucket.
        assert MISSING_LABEL_BUCKET < 0 or MISSING_LABEL_BUCKET >= NUM_BUCKETS


def test_unknown_policy_raises():
    with pytest.raises(ValueError, match="Unknown label policy"):
        label_policy.apply(42, "ohno")


# ---------------------------------------------------------------------------
# Payload helpers: apply_to_label_counts / apply_to_samples
# ---------------------------------------------------------------------------


class TestApplyToLabelCounts:
    def test_passthrough_returns_equal_mapping(self):
        counts = {"cat": 3, "dog": 2}
        assert label_policy.apply_to_label_counts(counts, PASSTHROUGH) == counts

    def test_passthrough_does_not_alias_the_input(self):
        counts = {"cat": 3}
        out = label_policy.apply_to_label_counts(counts, PASSTHROUGH)
        out["cat"] = 99
        assert counts["cat"] == 3

    def test_bucket_rekeys_to_bucket_ids(self):
        out = label_policy.apply_to_label_counts({"0": 11, "1": 19}, BUCKET)
        # Keys are stringified bucket ids — the wire shape the backend has
        # always received (review on #487).
        assert out == {
            str(label_policy.apply("0", BUCKET)): 11,
            str(label_policy.apply("1", BUCKET)): 19,
        }
        assert all(isinstance(k, str) and 0 <= int(k) < NUM_BUCKETS for k in out)

    def test_bucket_sums_colliding_raw_values(self):
        """64 buckets means collisions are routine. Dropping one of two
        colliding entries would under-report the dataset's row count to the
        backend, so counts are summed."""
        colliding = [
            v
            for v in (str(i) for i in range(500))
            if label_policy.apply(v, BUCKET) == 0
        ]
        assert len(colliding) >= 2, "fixture needs two values in the same bucket"
        a, b = colliding[0], colliding[1]
        out = label_policy.apply_to_label_counts({a: 4, b: 6}, BUCKET)
        assert out == {"0": 10}

    def test_bucket_preserves_total_row_count(self):
        counts = {str(i): i for i in range(1, 40)}
        out = label_policy.apply_to_label_counts(counts, BUCKET)
        assert sum(out.values()) == sum(counts.values())

    def test_bucket_missing_label_key_uses_sentinel(self):
        # get_label_counts maps a SQL NULL label to the "" key.
        out = label_policy.apply_to_label_counts({"": 7}, BUCKET)
        assert out == {str(MISSING_LABEL_BUCKET): 7}

    def test_unknown_policy_raises(self):
        with pytest.raises(ValueError, match="Unknown label policy"):
            label_policy.apply_to_label_counts({"a": 1}, "ohno")


class TestApplyToSamples:
    def test_passthrough_keeps_labels(self):
        samples = [{"data_id": "d1", "label": "cat"}]
        assert label_policy.apply_to_samples(samples, PASSTHROUGH) == samples

    def test_bucket_replaces_each_label(self):
        out = label_policy.apply_to_samples(
            [{"data_id": "d1", "label": "1"}, {"data_id": "d2", "label": "0"}],
            BUCKET,
        )
        # Stringified: data_samples[].label has always been a JSON string.
        assert [s["label"] for s in out] == [
            str(label_policy.apply("1", BUCKET)),
            str(label_policy.apply("0", BUCKET)),
        ]
        assert [s["data_id"] for s in out] == ["d1", "d2"]

    def test_does_not_mutate_the_input_samples(self):
        samples = [{"data_id": "d1", "label": "1"}]
        label_policy.apply_to_samples(samples, BUCKET)
        assert samples == [{"data_id": "d1", "label": "1"}]

    def test_sample_without_label_key_is_left_alone(self):
        out = label_policy.apply_to_samples([{"data_id": "d1"}], BUCKET)
        assert out == [{"data_id": "d1"}]

    def test_unknown_policy_raises(self):
        with pytest.raises(ValueError, match="Unknown label policy"):
            label_policy.apply_to_samples([{"data_id": "d", "label": "1"}], "ohno")


# ---------------------------------------------------------------------------
# The boundary: the stored row keeps the raw target (#486)
# ---------------------------------------------------------------------------
#
# We can't construct CSVIngestor / JSONIngestor without a working DB, but we
# can exercise RecordProcessor directly — it owns the DB-bound record.


def _TestRecordProcessor(label_column, intent="train"):
    """The transform that builds the row handed to the batch writer for INSERT.
    It takes no label policy since #486: bucketing is a boundary control."""
    from tracebloc_ingestor.ingestors.record_processor import RecordProcessor

    return RecordProcessor(
        schema={},  # _map_unique_id doesn't read schema
        intent=intent,
        label_column=label_column,
        annotation_column=None,
        unique_id_column=None,  # → content_hash generation (#350 default)
        ingestor_id="test",
        # #350: content_hash is the default strategy and needs a salt.
        table_salt="0" * 64,
    )


def test_stored_row_keeps_string_label():
    rp = _TestRecordProcessor(label_column="label")
    cleaned = rp._map_unique_id(
        record={"label": "cancer_positive"},
        cleaned_record={},
    )
    assert cleaned["label"] == "cancer_positive"


def test_stored_row_keeps_raw_numeric_target():
    """A regression target reaches MySQL as itself, not as a bucket id — this
    is the row the training client trains on."""
    rp = _TestRecordProcessor(label_column="label")
    cleaned = rp._map_unique_id({"label": 1234.56}, {})
    assert cleaned["label"] == 1234.56


def test_stored_row_keeps_binary_event_indicator():
    """The #486 reproduction: a time_to_event event indicator of 0/1 used to be
    stored as sha256("1")%64 = 33 / sha256("0")%64 = 56, and the engine rejects
    anything but 0/1 for that column."""
    rp = _TestRecordProcessor(label_column="DEATH_EVENT")
    assert rp._map_unique_id({"DEATH_EVENT": "1"}, {})["label"] == "1"
    assert rp._map_unique_id({"DEATH_EVENT": "0"}, {})["label"] == "0"


def test_record_processor_takes_no_label_policy():
    """Pins the collaborator boundary: a policy passed here would be silently
    ignored, so the constructor must refuse it outright."""
    from tracebloc_ingestor.ingestors.record_processor import RecordProcessor

    with pytest.raises(TypeError):
        RecordProcessor(
            schema={},
            intent="train",
            label_column="label",
            annotation_column=None,
            unique_id_column=None,
            label_policy=BUCKET,
            ingestor_id="test",
            table_salt="0" * 64,
        )


def test_stored_row_keeps_missing_label_as_none():
    rp = _TestRecordProcessor(label_column="label")
    cleaned = rp._map_unique_id({"label": None}, {})
    assert cleaned["label"] is None


# ---------------------------------------------------------------------------
# Entrypoint integration: regression YAML config flows through with bucket
# ---------------------------------------------------------------------------


def test_entrypoint_passes_bucket_policy_for_regression(tmp_path, monkeypatch):
    """End-to-end-ish: a tabular_regression YAML reaches CSVIngestor with
    label_policy='bucket' as kwarg, regardless of resolver internals."""
    from pathlib import Path
    from unittest.mock import patch

    examples_dir = Path(__file__).resolve().parent.parent / "examples" / "yaml"
    monkeypatch.setenv("INGEST_CONFIG", str(examples_dir / "tabular_regression.yaml"))

    with patch("tracebloc_ingestor.cli.run.Config") as mock_config_cls, patch(
        "tracebloc_ingestor.cli.run.Database"
    ), patch("tracebloc_ingestor.cli.run.APIClient"), patch(
        "tracebloc_ingestor.cli.run.CSVIngestor"
    ) as mock_csv_cls, patch(
        "tracebloc_ingestor.cli.run.setup_logging"
    ):
        mock_config = MagicMock()
        mock_config.BATCH_SIZE = 4000
        mock_config_cls.return_value = mock_config
        instance = MagicMock()
        instance.__enter__ = MagicMock(return_value=instance)
        instance.__exit__ = MagicMock(return_value=False)
        instance.ingest = MagicMock(return_value=[])
        mock_csv_cls.return_value = instance

        from tracebloc_ingestor.cli.run import main

        rc = main()

    assert rc == 0
    _, kwargs = mock_csv_cls.call_args
    assert kwargs["label_policy"] == BUCKET


def test_entrypoint_passes_passthrough_policy_for_classification(tmp_path, monkeypatch):
    """And the inverse: classification gets passthrough."""
    from pathlib import Path
    from unittest.mock import patch

    examples_dir = Path(__file__).resolve().parent.parent / "examples" / "yaml"
    monkeypatch.setenv("INGEST_CONFIG", str(examples_dir / "image_classification.yaml"))

    with patch("tracebloc_ingestor.cli.run.Config") as mock_config_cls, patch(
        "tracebloc_ingestor.cli.run.Database"
    ), patch("tracebloc_ingestor.cli.run.APIClient"), patch(
        "tracebloc_ingestor.cli.run.CSVIngestor"
    ) as mock_csv_cls, patch(
        "tracebloc_ingestor.cli.run.setup_logging"
    ):
        mock_config = MagicMock()
        mock_config.BATCH_SIZE = 4000
        mock_config_cls.return_value = mock_config
        instance = MagicMock()
        instance.__enter__ = MagicMock(return_value=instance)
        instance.__exit__ = MagicMock(return_value=False)
        instance.ingest = MagicMock(return_value=[])
        mock_csv_cls.return_value = instance

        from tracebloc_ingestor.cli.run import main

        main()

    _, kwargs = mock_csv_cls.call_args
    assert kwargs["label_policy"] == PASSTHROUGH


# ---------------------------------------------------------------------------
# End-to-end: a TTE ingest under `bucket` stores 0/1 and sends buckets (#486)
# ---------------------------------------------------------------------------


def test_tte_ingest_stores_raw_event_indicator_and_sends_buckets(make_csv):
    """The prod reproduction, end to end through CSVIngestor.

    A time_to_event_prediction CSV whose event column is 0/1, ingested with the
    policy the CLI defaults to for regression-class tasks. What lands in MySQL
    must stay 0/1 (the engine rejects anything else, and it is the training
    target); what goes to the backend must be bucketed.
    """
    from unittest.mock import patch

    import pandas as pd

    from tracebloc_ingestor.config import Config
    from tracebloc_ingestor.ingestors import base as base_mod
    from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
    from tracebloc_ingestor.utils.constants import DataFormat, TaskCategory

    schema = {"age": "INT", "time": "INT"}
    frame = pd.DataFrame(
        {
            "age": [45, 60, 75, 80],
            "time": [4, 12, 20, 30],
            "DEATH_EVENT": [1, 1, 0, 1],
        }
    )
    csv_path = make_csv(frame, name="tte.csv")

    db = MagicMock(name="Database")
    db.config = Config(TABLE_NAME="tte_toy")
    db.get_or_create_table_salt.return_value = "0" * 64
    db.create_table.return_value = MagicMock(name="table")
    db.insert_batch.side_effect = lambda table, batch: (list(range(len(batch))), [])
    db.get_table_schema.return_value = dict(schema, label="INT")
    # Read back from the DB, i.e. RAW labels — that is what the boundary gets.
    db.get_label_counts.return_value = {"1": 3, "0": 1}
    db.get_samples.return_value = [
        {"data_id": "d1", "label": "1"},
        {"data_id": "d2", "label": "0"},
    ]
    api = MagicMock(name="APIClient")
    api.config.TITLE = "toy tte"
    api.send_ingest_summary.return_value = {"dataset_id": 1}

    ing = CSVIngestor(
        database=db,
        api_client=api,
        table_name="tte_toy",
        schema=dict(schema),
        label_column="DEATH_EVENT",
        intent="train",
        category=TaskCategory.TIME_TO_EVENT_PREDICTION,
        data_format=DataFormat.TABULAR,
        label_policy=BUCKET,
        file_options={"time_column": "time", "schema": dict(schema)},
    )
    with patch.object(base_mod, "Session") as Sess:
        Sess.return_value.__enter__.return_value = MagicMock()
        failed = ing.ingest(str(csv_path), batch_size=50)

    assert failed == []
    stored_labels = {
        row["label"] for call in db.insert_batch.call_args_list for row in call.args[1]
    }
    # An INT label column arrives as a native int (numpy scalars are coerced so
    # mysql-connector can bind them); what matters is the VALUE, not its type.
    assert {str(v) for v in stored_labels} == {
        "1",
        "0",
    }, "the DB row must keep the event indicator"

    # The policy travels to the boundary rather than being pre-applied.
    assert api.send_ingest_summary.call_args.kwargs["label_policy"] == BUCKET
    assert api.send_ingest_summary.call_args.kwargs["labels"] == {"1": 3, "0": 1}


# ---------------------------------------------------------------------------
# Missing targets: NULL in, sentinel out (Bugbot on #487)
# ---------------------------------------------------------------------------
#
# The write-path bucketing this PR removed used to absorb NaN into
# MISSING_LABEL_BUCKET. The stored row now normalizes a missing target to SQL
# NULL instead — the label column is VARCHAR(255) NULL — and the outbound
# sentinel is reconstructed at the boundary from the DB's own NULL reporting.


def test_stored_row_normalizes_nan_label_to_null():
    """A float nan would otherwise reach the binder and land as "nan"."""
    rp = _TestRecordProcessor(label_column="label")
    assert rp._map_unique_id({"label": float("nan")}, {})["label"] is None


def test_stored_row_normalizes_pandas_na_label_to_null():
    import pandas as pd

    rp = _TestRecordProcessor(label_column="label")
    assert rp._map_unique_id({"label": pd.NA}, {})["label"] is None
    assert rp._map_unique_id({"label": pd.NaT}, {})["label"] is None


def test_stored_row_keeps_empty_string_label():
    """ "" stores fine and already buckets to the sentinel — left untouched so
    classification behavior is unchanged."""
    rp = _TestRecordProcessor(label_column="label")
    assert rp._map_unique_id({"label": ""}, {})["label"] == ""


def test_stored_row_keeps_zero_and_false_labels():
    """Guard against a truthiness-based null check: 0 and False are real
    label values, not missing ones."""
    rp = _TestRecordProcessor(label_column="label")
    assert rp._map_unique_id({"label": 0}, {})["label"] == 0
    assert rp._map_unique_id({"label": False}, {})["label"] is False


def test_null_label_round_trips_to_the_missing_sentinel():
    """The chain the NULL relies on: Database.get_label_counts keys a NULL
    label as "" and get_samples reports "", and the boundary buckets both to
    MISSING_LABEL_BUCKET — so a missing target still reaches the backend as the
    sentinel, without a sentinel ever being stored locally."""
    assert label_policy.apply_to_label_counts({"": 3}, BUCKET) == {
        str(MISSING_LABEL_BUCKET): 3
    }
    assert label_policy.apply_to_samples([{"data_id": "d", "label": ""}], BUCKET) == [
        {"data_id": "d", "label": str(MISSING_LABEL_BUCKET)}
    ]
