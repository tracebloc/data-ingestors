"""Tests for CSVIngestor.feature_stats — per-numeric-column sufficient
statistics emitted on the global-metadata channel (data-ingestors#360).

The five additive aggregates (count/sum/sum_sq/min/max) let the backend fold
per-dataset stats into global mean/std/min/max at combine time (backend#1037),
so the correctness that matters is: right values, accumulated across chunks,
features only (never the target/id/annotation), and surfaced in meta_data.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
from tracebloc_ingestor.utils.constants import TaskCategory


def make_csv_ingestor(schema=None, **overrides):
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


def test_feature_stats_basic_int_and_float(make_csv):
    path = make_csv({"age": [18, 20, 40], "score": [1.5, 2.5, 3.0]})
    ing = make_csv_ingestor(schema={"age": "INT", "score": "FLOAT"})
    list(ing.read_data(str(path)))

    stats = ing.feature_stats()
    assert set(stats) == {"age", "score"}

    age = stats["age"]
    assert age["count"] == 3
    assert age["sum"] == 78.0
    assert age["sum_sq"] == 18**2 + 20**2 + 40**2
    # INT column reports integer min/max, not 18.0 / 40.0.
    assert age["min"] == 18 and isinstance(age["min"], int)
    assert age["max"] == 40 and isinstance(age["max"], int)

    score = stats["score"]
    assert score["count"] == 3
    assert score["sum"] == pytest.approx(7.0)
    assert score["sum_sq"] == pytest.approx(1.5**2 + 2.5**2 + 3.0**2)
    assert score["min"] == pytest.approx(1.5)
    assert score["max"] == pytest.approx(3.0)


def test_feature_stats_accumulate_across_chunks(make_csv):
    # Force multiple chunks so the cross-chunk accumulation path is exercised
    # (the whole point of folding stats in the per-chunk cast pass).
    path = make_csv({"x": list(range(1, 11))})  # 1..10
    ing = make_csv_ingestor(
        schema={"x": "INT"}, csv_options={"chunk_size": 3}
    )
    list(ing.read_data(str(path)))

    x = ing.feature_stats()["x"]
    assert x["count"] == 10
    assert x["sum"] == 55.0
    assert x["sum_sq"] == sum(i * i for i in range(1, 11))
    assert x["min"] == 1
    assert x["max"] == 10


def test_feature_stats_excludes_id_and_annotation_always(make_csv):
    # A data_id (row identifier) and annotation column are never features,
    # regardless of category.
    path = make_csv({"feat": [1, 2, 3], "rowid": [7, 8, 9], "ann": [1, 1, 2]})
    ing = make_csv_ingestor(
        schema={"feat": "FLOAT", "rowid": "INT", "ann": "INT"},
        unique_id_column="rowid",
        annotation_column="ann",
    )
    list(ing.read_data(str(path)))

    stats = ing.feature_stats()
    assert set(stats) == {"feat"}


def test_feature_stats_excludes_label_for_classification(make_csv):
    # For classification the label is a class, not a numeric feature — excluded.
    path = make_csv({"feat": [1.0, 2.0, 3.0], "label": [0, 1, 0]})
    ing = make_csv_ingestor(
        schema={"feat": "FLOAT", "label": "INT"},
        label_column="label",
        category=TaskCategory.TABULAR_CLASSIFICATION,
    )
    list(ing.read_data(str(path)))

    stats = ing.feature_stats()
    assert set(stats) == {"feat"}
    assert "label" not in stats


@pytest.mark.parametrize(
    "category",
    [
        TaskCategory.TABULAR_REGRESSION,
        TaskCategory.TIME_SERIES_FORECASTING,
        TaskCategory.TIME_TO_EVENT_PREDICTION,
    ],
)
def test_feature_stats_includes_target_for_regression_class(make_csv, category):
    # Regression-class tasks: the numeric target IS a column the backend must
    # normalize globally (backend#1037), so its aggregate stats are emitted
    # under its column name. The row-id is still excluded.
    path = make_csv(
        {"feat": [1, 2, 3], "target": [10, 20, 30], "rowid": [7, 8, 9]}
    )
    ing = make_csv_ingestor(
        schema={"feat": "FLOAT", "target": "FLOAT", "rowid": "INT"},
        label_column="target",
        unique_id_column="rowid",
        category=category,
    )
    list(ing.read_data(str(path)))

    stats = ing.feature_stats()
    # The target is re-keyed from its original name ("target") to the
    # standardized "label" so it matches the enriched schema's role:"target".
    assert set(stats) == {"feat", "label"}
    assert "target" not in stats
    assert stats["label"] == {
        "count": 3,
        "sum": 60.0,
        "sum_sq": 10.0**2 + 20.0**2 + 30.0**2,
        "min": pytest.approx(10.0),
        "max": pytest.approx(30.0),
    }
    assert "rowid" not in stats


def test_target_already_named_label_is_left_in_place(make_csv):
    # If the user's label column is already "label", the re-key is a no-op
    # (no self-clobber) and the target stats are present under "label".
    path = make_csv({"feat": [1, 2], "label": [10, 20]})
    ing = make_csv_ingestor(
        schema={"feat": "FLOAT", "label": "FLOAT"},
        label_column="label",
        category=TaskCategory.TIME_SERIES_FORECASTING,
    )
    list(ing.read_data(str(path)))

    stats = ing.feature_stats()
    assert set(stats) == {"feat", "label"}
    assert stats["label"]["count"] == 2


def test_feature_stats_ignores_nulls(make_csv):
    # Missing cells are dropped: count is the non-null count and the aggregates
    # ignore the blanks.
    path = make_csv({"v": [10, None, 30]})
    ing = make_csv_ingestor(schema={"v": "INT"})
    list(ing.read_data(str(path)))

    v = ing.feature_stats()["v"]
    assert v["count"] == 2
    assert v["sum"] == 40.0
    assert v["min"] == 10 and v["max"] == 30


def test_feature_stats_omits_all_null_column(make_csv):
    # An all-null numeric column has no min/max, so it is omitted entirely
    # rather than emitted with undefined bounds.
    path = make_csv({"present": [1, 2], "blank": [None, None]})
    ing = make_csv_ingestor(schema={"present": "INT", "blank": "FLOAT"})
    list(ing.read_data(str(path)))

    stats = ing.feature_stats()
    assert "present" in stats
    assert "blank" not in stats


def test_feature_stats_empty_when_no_numeric_columns(make_csv):
    path = make_csv({"name": ["x", "y"], "when": ["2024-01-01", "2024-01-02"]})
    ing = make_csv_ingestor(schema={"name": "VARCHAR(10)", "when": "DATE"})
    list(ing.read_data(str(path)))

    assert ing.feature_stats() == {}
    # And the emit hook contributes nothing to the payload.
    assert ing._collect_run_metadata() == {}


def test_feature_stats_nested_under_attributes(make_csv):
    # backend#1037 contract: per-column extras live under
    # attributes.feature_stats, not at the top level of meta_data.
    path = make_csv({"age": [1, 2, 3]})
    ing = make_csv_ingestor(schema={"age": "INT"})
    list(ing.read_data(str(path)))

    meta = ing._collect_run_metadata()
    assert meta["attributes"]["feature_stats"]["age"]["count"] == 3
    # feature_stats must NOT sit at the top level anymore.
    assert "feature_stats" not in meta


def test_apply_run_metadata_merges_into_attributes(make_csv):
    # The apply step folds feature_stats into file_options["attributes"] without
    # clobbering other facts already in that shared namespace (forward-compat
    # with the per-category attributes slice).
    path = make_csv({"age": [1, 2, 3]})
    ing = make_csv_ingestor(schema={"age": "INT"})
    ing.file_options["attributes"] = {"language": "en"}
    list(ing.read_data(str(path)))

    ing._apply_run_metadata()

    attrs = ing.file_options["attributes"]
    assert attrs["language"] == "en"  # pre-existing key preserved
    assert attrs["feature_stats"]["age"]["count"] == 3


def test_apply_run_metadata_noop_without_numeric(make_csv):
    path = make_csv({"name": ["x", "y"]})
    ing = make_csv_ingestor(schema={"name": "VARCHAR(10)"})
    list(ing.read_data(str(path)))

    ing._apply_run_metadata()
    assert "attributes" not in ing.file_options
