"""Equivalence harness — YAML path produces the same ingestor configuration
as the existing template scripts.

This pins the #44 acceptance criterion:

    > For each of the seven existing templates plus segmentation, an
    > equivalent ``examples/yaml/<name>.yaml`` exists and produces the same
    > end state (same MySQL rows, same backend POSTs).

We don't run real DB / API I/O. Instead we capture the kwargs each path
hands to the ingestor and assert they match. The framework code that
turns those kwargs into MySQL rows + backend POSTs is the same on both
paths, so equivalent kwargs imply equivalent end-state.

Two intentional, documented divergences from the templates (per #44 design):

1. **``unique_id_column`` defaults to None** (UUID generation, no source
   column leaves the cluster) for all categories. The existing
   ``keypoint_detection`` and ``semantic_segmentation`` templates set
   ``unique_id_column="filename"`` — leaking the filename to the central
   backend. Customers wanting that behavior must opt in explicitly via
   ``data_id: {strategy: column, column: filename}`` in their YAML; the
   harness configs below do so for those two categories to preserve
   template-equivalent behavior.

2. **``label_policy: bucket``** is required by the schema for
   regression-class categories (regression, time-series, time-to-event).
   The existing templates send the raw target value through. The YAML
   path applies hash-bucket. This is a deliberate behavior change per
   parent client#85 — raw targets shouldn't leak. The equivalence test
   asserts ``label_policy="bucket"`` for those three categories rather
   than asserting payload equivalence on the label.

A third, non-functional difference: tabular templates set
``file_options={"number_of_columns": len(schema)}``. The
``number_of_columns`` field is dead code (no consumer in the package;
``grep`` confirms). YAML path omits it; harness allows the diff.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
import yaml

from tracebloc_ingestor.cli.conventions import resolve
from tracebloc_ingestor.utils.constants import (
    DataFormat,
    FileExtension,
    Intent,
    TaskCategory,
)
from tracebloc_ingestor.utils.label_policy import BUCKET, PASSTHROUGH


REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_ROOT / "examples" / "yaml"


# ---------------------------------------------------------------------------
# One case per existing template. Each case is a tuple of
#   (yaml_config, expected_kwargs_for_ingestor)
# The yaml_config is built inline (rather than reading from examples/yaml/)
# so we can control intent / label_column / data_id strategy to match the
# template exactly.
# ---------------------------------------------------------------------------

def _yaml(**fields) -> Dict[str, Any]:
    """Helper: build a YAML config dict with apiVersion/kind boilerplate."""
    return {
        "apiVersion": "tracebloc.io/v1",
        "kind": "IngestConfig",
        **fields,
    }


CASES = [
    # -----------------------------------------------------------------------
    # image_classification — template uses 512×512, intent=TEST, label_column="label"
    # -----------------------------------------------------------------------
    pytest.param(
        _yaml(
            category=TaskCategory.IMAGE_CLASSIFICATION,
            table="image_classification_train",
            intent="test",  # template default is Intent.TEST
            csv="/data/labels.csv",
            images="/data/images/",
            label="label",
        ),
        {
            "category": TaskCategory.IMAGE_CLASSIFICATION,
            "data_format": DataFormat.IMAGE,
            "intent": Intent.TEST,
            "label_column": "label",
            "label_policy": PASSTHROUGH,
            "unique_id_column": None,
            "annotation_column": None,
            "file_options": {"target_size": [512, 512], "extension": FileExtension.JPG},
        },
        id="image_classification",
    ),

    # -----------------------------------------------------------------------
    # object_detection — template uses 448×448, label_column="image_label"
    # -----------------------------------------------------------------------
    pytest.param(
        _yaml(
            category=TaskCategory.OBJECT_DETECTION,
            table="object_detection_train",
            intent="train",
            csv="/data/labels.csv",
            images="/data/images/",
            annotations="/data/annotations/",
            label="image_label",
        ),
        {
            "category": TaskCategory.OBJECT_DETECTION,
            "data_format": DataFormat.IMAGE,
            "intent": Intent.TRAIN,
            "label_column": "image_label",
            "label_policy": PASSTHROUGH,
            "unique_id_column": None,
            "annotation_column": None,
            "file_options": {"target_size": [448, 448], "extension": FileExtension.JPG},
        },
        id="object_detection",
    ),

    # -----------------------------------------------------------------------
    # keypoint_detection — template uses 448×448, label_column="image_label",
    # annotation_column="Annotation", unique_id_column="filename" (opt-in
    # column-mapping per #44 default-UUID change).
    # -----------------------------------------------------------------------
    pytest.param(
        _yaml(
            category=TaskCategory.KEYPOINT_DETECTION,
            table="keypoint_detection_train",
            intent="train",
            csv="/data/labels.csv",
            images="/data/images/",
            label="image_label",
            data_id={"strategy": "column", "column": "filename"},
        ),
        {
            "category": TaskCategory.KEYPOINT_DETECTION,
            "data_format": DataFormat.IMAGE,
            "intent": Intent.TRAIN,
            "label_column": "image_label",
            "label_policy": PASSTHROUGH,
            "unique_id_column": "filename",
            "annotation_column": "Annotation",  # convention default per category
            "file_options": {"target_size": [448, 448], "extension": FileExtension.JPG},
        },
        id="keypoint_detection",
    ),

    # -----------------------------------------------------------------------
    # semantic_segmentation — template uses 512×512, label_column="image_label",
    # unique_id_column="filename" (opt-in).
    # -----------------------------------------------------------------------
    pytest.param(
        _yaml(
            category=TaskCategory.SEMANTIC_SEGMENTATION,
            table="semantic_segmentation_train",
            intent="train",
            csv="/data/labels.csv",
            images="/data/images/",
            masks="/data/masks/",
            label="image_label",
            data_id={"strategy": "column", "column": "filename"},
        ),
        {
            "category": TaskCategory.SEMANTIC_SEGMENTATION,
            "data_format": DataFormat.IMAGE,
            "intent": Intent.TRAIN,
            "label_column": "image_label",
            "label_policy": PASSTHROUGH,
            "unique_id_column": "filename",
            "annotation_column": None,
            "file_options": {"target_size": [512, 512], "extension": FileExtension.JPG},
        },
        id="semantic_segmentation",
    ),

    # -----------------------------------------------------------------------
    # text_classification — template uses extension=.txt, label_column="label"
    # -----------------------------------------------------------------------
    pytest.param(
        _yaml(
            category=TaskCategory.TEXT_CLASSIFICATION,
            table="text_classification_train",
            intent="train",
            csv="/data/labels.csv",
            texts="/data/texts/",
            schema={"text_id": "VARCHAR(255)", "label": "VARCHAR(64)"},
            label="label",
        ),
        {
            "category": TaskCategory.TEXT_CLASSIFICATION,
            "data_format": DataFormat.TEXT,
            "intent": Intent.TRAIN,
            "label_column": "label",
            "label_policy": PASSTHROUGH,
            "unique_id_column": None,
            "annotation_column": None,
            "file_options": {"extension": FileExtension.TXT},
        },
        id="text_classification",
    ),

    # -----------------------------------------------------------------------
    # tabular_classification — template label_column="name"
    # -----------------------------------------------------------------------
    pytest.param(
        _yaml(
            category=TaskCategory.TABULAR_CLASSIFICATION,
            table="tabular_classification_train",
            intent="train",
            csv="/data/data.csv",
            schema={"age": "INT", "name": "VARCHAR(255)"},
            label="name",
        ),
        {
            "category": TaskCategory.TABULAR_CLASSIFICATION,
            "data_format": DataFormat.TABULAR,
            "intent": Intent.TRAIN,
            "label_column": "name",
            "label_policy": PASSTHROUGH,
            "unique_id_column": None,
            "annotation_column": None,
            # number_of_columns is dead code in templates — YAML path omits it.
            "file_options": {},
        },
        id="tabular_classification",
    ),

    # -----------------------------------------------------------------------
    # tabular_regression — regression-class, label_policy MUST be bucket
    # (#44 deliberate behavior change vs template's raw passthrough).
    # -----------------------------------------------------------------------
    pytest.param(
        _yaml(
            category=TaskCategory.TABULAR_REGRESSION,
            table="tabular_regression_train",
            intent="train",
            csv="/data/data.csv",
            schema={
                "square_feet": "FLOAT",
                "bedrooms": "INT",
                "price": "FLOAT",
            },
            label={"column": "price", "policy": "bucket"},
        ),
        {
            "category": TaskCategory.TABULAR_REGRESSION,
            "data_format": DataFormat.TABULAR,
            "intent": Intent.TRAIN,
            "label_column": "price",
            "label_policy": BUCKET,  # ← intentional divergence
            "unique_id_column": None,
            "annotation_column": None,
            "file_options": {},
        },
        id="tabular_regression",
    ),

    # -----------------------------------------------------------------------
    # time_series_forecasting — regression-class, label_policy=bucket.
    # Template label_column="max_magnitude".
    # -----------------------------------------------------------------------
    pytest.param(
        _yaml(
            category=TaskCategory.TIME_SERIES_FORECASTING,
            table="time_series_forecasting_train",
            intent="train",
            csv="/data/data.csv",
            schema={
                "timestamp": "VARCHAR(64)",
                "region": "VARCHAR(32)",
                "max_magnitude": "FLOAT",
            },
            label={"column": "max_magnitude", "policy": "bucket"},
        ),
        {
            "category": TaskCategory.TIME_SERIES_FORECASTING,
            "data_format": DataFormat.TABULAR,
            "intent": Intent.TRAIN,
            "label_column": "max_magnitude",
            "label_policy": BUCKET,
            "unique_id_column": None,
            "annotation_column": None,
            "file_options": {},
        },
        id="time_series_forecasting",
    ),

    # -----------------------------------------------------------------------
    # time_to_event_prediction — regression-class, label_policy=bucket,
    # plus time_column="time" (template's value).
    # -----------------------------------------------------------------------
    pytest.param(
        _yaml(
            category=TaskCategory.TIME_TO_EVENT_PREDICTION,
            table="time_to_event_prediction_train",
            intent="train",
            csv="/data/data.csv",
            time_column="time",
            schema={
                "patient_id": "VARCHAR(64)",
                "time": "INT",
                "DEATH_EVENT": "INT",
            },
            label={"column": "DEATH_EVENT", "policy": "bucket"},
        ),
        {
            "category": TaskCategory.TIME_TO_EVENT_PREDICTION,
            "data_format": DataFormat.TABULAR,
            "intent": Intent.TRAIN,
            "label_column": "DEATH_EVENT",
            "label_policy": BUCKET,
            "unique_id_column": None,
            "annotation_column": None,
            # time_column bridged into file_options for TimeToEventValidator.
            "file_options": {"time_column": "time"},
        },
        id="time_to_event_prediction",
    ),
]


@pytest.mark.parametrize("yaml_config,expected", CASES)
def test_yaml_resolves_to_template_equivalent_kwargs(yaml_config, expected):
    """For each existing template, the equivalent YAML must produce
    identical ingestor kwargs (with the documented intentional divergences:
    UUID-by-default for data_id, BUCKET-for-regression label policy)."""
    resolved = resolve(yaml_config)

    for key, want in expected.items():
        got = getattr(resolved, key)
        assert got == want, (
            f"{yaml_config['category']}: {key} mismatch\n"
            f"  expected: {want!r}\n"
            f"  got:      {got!r}"
        )


# ---------------------------------------------------------------------------
# All existing templates have a YAML equivalent that's exercisable here.
# ---------------------------------------------------------------------------

def test_every_existing_template_has_a_case():
    """The acceptance criterion enumerates 'seven existing templates plus
    segmentation' (= 8). The repo also added keypoint_detection (+1 = 9).
    Every template directory under templates/ must have a parametrized
    case in this file."""
    template_dirs = sorted(p.name for p in (REPO_ROOT / "templates").iterdir() if p.is_dir())
    template_categories = {
        d for d in template_dirs
        if d != "example_data"  # the only non-template dir
    }

    case_categories = {c.values[0]["category"] for c in CASES}

    missing = template_categories - case_categories
    assert not missing, (
        f"templates/ contains category dirs without an equivalence case: "
        f"{sorted(missing)}"
    )


# ---------------------------------------------------------------------------
# End-to-end-but-mocked: the YAML configs above flow through the entrypoint
# and the same kwargs land on the ingestor constructor.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("yaml_config,expected", CASES)
def test_yaml_config_reaches_ingestor_via_entrypoint(
    yaml_config, expected, tmp_path, monkeypatch
):
    """Smoke test that the kwargs verified above also land on the ingestor
    when run through the full ``cli.run.main`` flow with mocked DB / API."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.safe_dump(yaml_config), encoding="utf-8")
    monkeypatch.setenv("INGEST_CONFIG", str(cfg_file))

    with patch("tracebloc_ingestor.cli.run.Config") as mock_config_cls, \
         patch("tracebloc_ingestor.cli.run.Database"), \
         patch("tracebloc_ingestor.cli.run.APIClient"), \
         patch("tracebloc_ingestor.cli.run.CSVIngestor") as mock_csv_cls, \
         patch("tracebloc_ingestor.cli.run.JSONIngestor") as mock_json_cls, \
         patch("tracebloc_ingestor.cli.run.setup_logging"):
        mock_config = MagicMock()
        mock_config.BATCH_SIZE = 4000
        mock_config_cls.return_value = mock_config
        for cls_mock in (mock_csv_cls, mock_json_cls):
            inst = MagicMock()
            inst.__enter__ = MagicMock(return_value=inst)
            inst.__exit__ = MagicMock(return_value=False)
            inst.ingest = MagicMock(return_value=[])
            cls_mock.return_value = inst

        from tracebloc_ingestor.cli.run import main
        rc = main()

    assert rc == 0
    # All current cases use csv source; assert CSV path was taken.
    assert mock_csv_cls.call_count == 1, (
        f"expected CSVIngestor for {yaml_config['category']}, got {mock_csv_cls.call_count} call(s)"
    )

    _, kwargs = mock_csv_cls.call_args
    for key, want in expected.items():
        got = kwargs.get(key)
        assert got == want, (
            f"{yaml_config['category']}: ingestor kwarg {key} mismatch\n"
            f"  expected: {want!r}\n"
            f"  got:      {got!r}"
        )
