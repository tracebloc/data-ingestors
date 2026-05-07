"""Tests for the label-policy bucketing module + the BaseIngestor wiring.

The pure-function tests pin the bucketing contract:
  - passthrough is a no-op
  - bucket is stable, deterministic, in [0, NUM_BUCKETS)
  - missing values produce MISSING_LABEL_BUCKET
  - unknown policies raise

The integration test pins that the policy fires inside ``BaseIngestor``
just before the API payload is built, so the central backend never sees
raw regression target values.
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

    def test_missing_sentinel_outside_valid_range(self):
        # MISSING_LABEL_BUCKET must not collide with a real bucket.
        assert MISSING_LABEL_BUCKET < 0 or MISSING_LABEL_BUCKET >= NUM_BUCKETS


def test_unknown_policy_raises():
    with pytest.raises(ValueError, match="Unknown label policy"):
        label_policy.apply(42, "ohno")


# ---------------------------------------------------------------------------
# Integration: BaseIngestor wiring
# ---------------------------------------------------------------------------
#
# We can't construct CSVIngestor / JSONIngestor without a working DB, but we
# can construct BaseIngestor subclass-shape directly and exercise
# _map_unique_id which is where the label-policy hook lives.

class _TestIngestor:
    """Minimal stand-in mimicking the BaseIngestor surface needed for
    _map_unique_id, without invoking BaseIngestor.__init__ (which calls
    Database.create_table)."""

    def __init__(self, label_column, label_policy_value, intent="train"):
        from tracebloc_ingestor.ingestors.base import BaseIngestor
        # Bind the unbound method so `self` works.
        self._map_unique_id = BaseIngestor._map_unique_id.__get__(self)
        self.label_column = label_column
        self.label_policy = label_policy_value
        self.intent = intent
        self.annotation_column = None
        self.unique_id_column = None  # → UUID generation


def test_base_ingestor_passthrough_does_not_mutate_label():
    ing = _TestIngestor(label_column="label", label_policy_value=PASSTHROUGH)
    cleaned = ing._map_unique_id(
        record={"label": "cancer_positive"},
        cleaned_record={},
    )
    assert cleaned["label"] == "cancer_positive"


def test_base_ingestor_bucket_replaces_label_with_int():
    ing = _TestIngestor(label_column="label", label_policy_value=BUCKET)
    cleaned = ing._map_unique_id(
        record={"label": 1234.56},
        cleaned_record={},
    )
    assert isinstance(cleaned["label"], int)
    assert 0 <= cleaned["label"] < NUM_BUCKETS


def test_base_ingestor_bucket_stable_across_calls():
    """Two records with the same label must land in the same bucket so the
    central backend can group identical labels without seeing them."""
    ing = _TestIngestor(label_column="label", label_policy_value=BUCKET)
    a = ing._map_unique_id({"label": 99.5}, {})
    b = ing._map_unique_id({"label": 99.5}, {})
    assert a["label"] == b["label"]


def test_base_ingestor_bucket_missing_label_uses_sentinel():
    ing = _TestIngestor(label_column="label", label_policy_value=BUCKET)
    cleaned = ing._map_unique_id({"label": None}, {})
    assert cleaned["label"] == MISSING_LABEL_BUCKET


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

    with patch("tracebloc_ingestor.cli.run.Config") as mock_config_cls, \
         patch("tracebloc_ingestor.cli.run.Database"), \
         patch("tracebloc_ingestor.cli.run.APIClient"), \
         patch("tracebloc_ingestor.cli.run.CSVIngestor") as mock_csv_cls, \
         patch("tracebloc_ingestor.cli.run.setup_logging"):
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

    with patch("tracebloc_ingestor.cli.run.Config") as mock_config_cls, \
         patch("tracebloc_ingestor.cli.run.Database"), \
         patch("tracebloc_ingestor.cli.run.APIClient"), \
         patch("tracebloc_ingestor.cli.run.CSVIngestor") as mock_csv_cls, \
         patch("tracebloc_ingestor.cli.run.setup_logging"):
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
