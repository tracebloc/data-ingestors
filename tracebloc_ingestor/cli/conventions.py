"""Translate a validated ``ingest.yaml`` config into ingestor kwargs.

This module is the convention-over-configuration layer of #44. The schema
(``schema/ingest.v1.json``) defines what the customer can write; this module
defines what every elision means. Setting ``category: image_classification``
implies::

    data_format        = "image"
    file_options       = {"target_size": [512, 512], "extension": ".jpg"}
    csv_options        = {chunk_size: 1000, delimiter: ",", quotechar: '"',
                          escapechar: "\\\\"}
    unique_id_column   = None        # UUID generation, no PII leakage
    label_policy       = "passthrough"

Customers override only when they deviate. The function is pure (no I/O,
no env reads, no globals); the caller passes a config dict and gets a
:class:`ResolvedConfig` back. That keeps the resolver trivially testable
and lets the entrypoint compose it with environment setup separately.

Pre-condition: the input dict must already have passed ``jsonschema``
validation against ``schema/ingest.v1.json``. ``resolve()`` does not
re-validate; it assumes well-formed input.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Optional

from ..utils.constants import DataFormat, Intent, TaskCategory


# ---------------------------------------------------------------------------
# Category groupings — used both here and by the entrypoint when deciding
# which sidecar paths matter. Single source of truth.
# ---------------------------------------------------------------------------

IMAGE_CATEGORIES: FrozenSet[str] = frozenset({
    TaskCategory.IMAGE_CLASSIFICATION,
    TaskCategory.OBJECT_DETECTION,
    TaskCategory.KEYPOINT_DETECTION,
    TaskCategory.SEMANTIC_SEGMENTATION,
    TaskCategory.INSTANCE_SEGMENTATION,
})

TEXT_CATEGORIES: FrozenSet[str] = frozenset({
    TaskCategory.TEXT_CLASSIFICATION,
    TaskCategory.TOKEN_CLASSIFICATION,
})

TABULAR_CATEGORIES: FrozenSet[str] = frozenset({
    TaskCategory.TABULAR_CLASSIFICATION,
    TaskCategory.TABULAR_REGRESSION,
})

TIME_SERIES_CATEGORIES: FrozenSet[str] = frozenset({
    TaskCategory.TIME_SERIES_FORECASTING,
})

TIME_TO_EVENT_CATEGORIES: FrozenSet[str] = frozenset({
    TaskCategory.TIME_TO_EVENT_PREDICTION,
})

MLM_CATEGORIES: FrozenSet[str] = frozenset({
    TaskCategory.MASKED_LANGUAGE_MODELING,
})

# Categories where the label is a numeric prediction target rather than
# class metadata. The schema requires `label.policy` for these so the raw
# value never ships to the central backend by default.
REGRESSION_CLASS_CATEGORIES: FrozenSet[str] = frozenset(
    {TaskCategory.TABULAR_REGRESSION}
    | TIME_SERIES_CATEGORIES
    | TIME_TO_EVENT_CATEGORIES
)


# ---------------------------------------------------------------------------
# Default values per category. Single source of truth so the entrypoint and
# tests both read from here rather than redefining locally.
# ---------------------------------------------------------------------------

DEFAULT_CSV_OPTIONS: Dict[str, Any] = {
    "chunk_size": 1000,
    "delimiter": ",",
    "quotechar": '"',
    "escapechar": "\\",
}

# Per-category image defaults. The values match what the existing templates
# in ``templates/*`` set explicitly, so YAML-driven runs default-equivalent
# to script-driven runs (verified by the equivalence harness in
# ``tests/test_template_equivalence.py``).
#
# The asymmetry across categories — 512×512 for classification / segmentation,
# 448×448 for detection / keypoints — is inherited from the templates'
# customer-tuned values. Refining these defaults after first customer
# migrations is explicitly out-of-scope for #44 per the ticket.
DEFAULT_IMAGE_FILE_OPTIONS_BY_CATEGORY: Dict[str, Dict[str, Any]] = {
    # IMAGE_CLASSIFICATION + OBJECT_DETECTION defaults now match the bundled
    # onboarding samples under `templates/*/data/images/` (#198, #199) so the
    # framework's own samples round-trip through the documented happy-path
    # config with zero overrides. Production users with differently-sized data
    # should set `spec.file_options` in their YAML.
    TaskCategory.IMAGE_CLASSIFICATION:    {"target_size": [256, 256], "extension": ".jpeg"},
    TaskCategory.SEMANTIC_SEGMENTATION:   {"target_size": [512, 512], "extension": ".jpg"},
    TaskCategory.OBJECT_DETECTION:        {"target_size": [1920, 1080], "extension": ".jpg"},
    # keypoint_detection: no target_size default — the customer's pose model
    # dictates input resolution, so the schema requires it top-level.
    TaskCategory.KEYPOINT_DETECTION:      {"extension": ".jpg"},
    # No template exists yet for instance_segmentation; mirror semantic for
    # forward-compatibility, revisit when the template lands.
    TaskCategory.INSTANCE_SEGMENTATION:   {"target_size": [512, 512], "extension": ".jpg"},
}

DEFAULT_TEXT_FILE_OPTIONS: Dict[str, Any] = {
    "extension": ".txt",
}

DEFAULT_MLM_FILE_OPTIONS: Dict[str, Any] = {
    "extension": ".txt",
}


# ---------------------------------------------------------------------------
# Resolved configuration — what the entrypoint actually consumes.
# ---------------------------------------------------------------------------

@dataclass
class ResolvedConfig:
    """A fully-resolved ingest configuration.

    Every field is filled in: customer-supplied values win, convention
    defaults from ``category`` cover the rest. The entrypoint maps these
    fields to ``CSVIngestor`` / ``JSONIngestor`` constructor kwargs and to
    ``os.environ`` for the legacy path-resolution layer in
    ``file_transfer.py`` (which still reads ``SRC_PATH``/``DEST_PATH`` from
    env — the YAML-side cleanup is a follow-up after the dominant flow lands).
    """

    # ----- Identity -----
    category: str
    table_name: str
    intent: str
    data_format: str

    # ----- Source -----
    source_type: str  # "csv" or "json"
    source_path: str  # absolute path inside the pod

    # ----- Sidecar directories (None when irrelevant for the category) -----
    images: Optional[str] = None
    annotations: Optional[str] = None
    masks: Optional[str] = None
    texts: Optional[str] = None
    sequences: Optional[str] = None

    # ----- Tabular / time-series -----
    schema: Dict[str, str] = field(default_factory=dict)
    time_column: Optional[str] = None  # time_to_event_prediction only

    # ----- Label -----
    label_column: str = ""
    label_policy: str = "passthrough"  # "passthrough" | "bucket"

    # ----- data_id -----
    unique_id_column: Optional[str] = None  # None ⇒ UUID generation

    # ----- Pass-through to ingestors -----
    annotation_column: Optional[str] = None
    csv_options: Dict[str, Any] = field(default_factory=dict)
    file_options: Dict[str, Any] = field(default_factory=dict)

    # ----- Custom processors (specs only; entrypoint loads classes) -----
    processor_specs: List[Dict[str, Any]] = field(default_factory=list)

    # ----- Deferred-feature passthroughs -----
    # Schema-accepted in v1 for forward-compatibility, but the runtime path
    # to honour them isn't built yet (validator-name resolution + the
    # sidecar mounting story both wait for client#86). The entrypoint
    # warns when these are non-empty so customers know the keys are inert.
    validators_override: List[str] = field(default_factory=list)
    sidecars: List[Dict[str, Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------

def resolve(config: Dict[str, Any]) -> ResolvedConfig:
    """Translate a validated ingest.yaml dict into a :class:`ResolvedConfig`.

    Args:
        config: The parsed YAML body. Must already have passed jsonschema
            validation against ``schema/ingest.v1.json``.

    Returns:
        A :class:`ResolvedConfig` with convention defaults filled in.

    The resolver makes three classes of decisions, in order:
      1. **Identity** — direct copies of required fields plus
         ``data_format`` derived from ``category``.
      2. **Source dispatch** — ``source_type`` and ``source_path`` from the
         ``csv:`` or ``json:`` shorthand (the schema enforces exactly one).
      3. **Per-category defaults** — ``file_options`` / ``csv_options`` /
         label / data_id, with customer values overriding.
    """
    category = config["category"]

    # 1. Identity
    resolved = ResolvedConfig(
        category=category,
        table_name=config["table"],
        intent=config["intent"],
        data_format=_data_format_for(category),
        source_type="csv" if "csv" in config else "json",
        source_path=config.get("csv") or config["json"],
    )

    # 2. Sidecar directories — set whatever the customer specified; the
    #    schema's conditional `if/then` already enforced that the right ones
    #    are present for each category.
    for key in ("images", "annotations", "masks", "texts", "sequences"):
        if key in config:
            setattr(resolved, key, config[key])

    # 3. Schema (tabular / time-series / time-to-event)
    if "schema" in config:
        resolved.schema = dict(config["schema"])
    if "time_column" in config:
        resolved.time_column = config["time_column"]

    # 4. Label — string shorthand or object form
    label = config.get("label")
    if isinstance(label, str):
        resolved.label_column = label
        # passthrough is the default; classification-class only — schema
        # forbids string-shorthand for regression-class, so this is safe.
    elif isinstance(label, dict):
        resolved.label_column = label["column"]
        resolved.label_policy = label.get("policy", "passthrough")

    # 5. data_id — strategy: uuid (default, no source col leaves the cluster)
    #    or column (loud, opt-in, captured in unique_id_column for the
    #    BaseIngestor warning we added in #43).
    data_id = config.get("data_id") or {}
    if data_id.get("strategy") == "column":
        resolved.unique_id_column = data_id["column"]
    # else: leave unique_id_column = None ⇒ UUID generation

    # 6. csv_options — merge customer overrides over defaults.
    csv_overrides = (config.get("spec") or {}).get("csv_options") or {}
    resolved.csv_options = {**DEFAULT_CSV_OPTIONS, **csv_overrides}

    # 7. file_options — per-category defaults, merged with customer overrides.
    spec_file_options = (config.get("spec") or {}).get("file_options") or {}
    resolved.file_options = {**_default_file_options_for(category), **spec_file_options}

    # 7a. For time_to_event_prediction the validator (TimeToEventValidator)
    #     reads `time_column` from file_options. Bridge the top-level field
    #     so the validator gets it without customers having to repeat the
    #     value in spec.file_options. ``setdefault`` so an explicit
    #     spec.file_options.time_column (the advanced override) wins over
    #     the documented top-level shorthand, consistent with how every
    #     other spec.file_options key behaves.
    if category == TaskCategory.TIME_TO_EVENT_PREDICTION and resolved.time_column:
        resolved.file_options.setdefault("time_column", resolved.time_column)

    # 7b. Bridge top-level `target_size` (any image category — customer
    #     override, or the only source for keypoint_detection) and
    #     `number_of_keypoints` (keypoint_detection only — schema requires it).
    #     Precedence is spec.file_options > top-level > category default; since
    #     spec was already merged in step 7, top-level only fills in when spec
    #     didn't set it.
    if "target_size" in config and "target_size" not in spec_file_options:
        resolved.file_options["target_size"] = list(config["target_size"])
    if (
        "number_of_keypoints" in config
        and "number_of_keypoints" not in spec_file_options
    ):
        resolved.file_options["number_of_keypoints"] = config["number_of_keypoints"]

    # 8. annotation_column — keypoint_detection's existing template uses
    #    column "Annotation" (the keypoint coords carried in the CSV). The
    #    YAML schema doesn't expose this directly in v1; we honour the
    #    convention here. Other categories rely on sidecar files only.
    if category == TaskCategory.KEYPOINT_DETECTION:
        resolved.annotation_column = "Annotation"

    # 9. Processor specs — pass through verbatim. The entrypoint will import
    #    the script and instantiate the named class. We don't do that here
    #    so the resolver stays I/O-free and unit-testable.
    spec = config.get("spec") or {}
    resolved.processor_specs = list(spec.get("processors") or [])

    # 10. Deferred passthroughs — captured so the entrypoint can warn about
    #     them. Honouring these is part of the v1.1 surface (validator-name
    #     resolution and the sidecar mounting story from client#86).
    resolved.validators_override = list(spec.get("validators") or [])
    resolved.sidecars = list(spec.get("sidecars") or [])

    return resolved


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _data_format_for(category: str) -> str:
    """Map ``category`` to the ``DataFormat`` value the framework expects."""
    if category in IMAGE_CATEGORIES:
        return DataFormat.IMAGE
    if category in TEXT_CATEGORIES:
        return DataFormat.TEXT
    if (
        category in TABULAR_CATEGORIES
        or category in TIME_SERIES_CATEGORIES
        or category in TIME_TO_EVENT_CATEGORIES
    ):
        return DataFormat.TABULAR
    if category in MLM_CATEGORIES:
        return DataFormat.TEXT
    raise ValueError(
        f"Unknown category {category!r}; cannot derive data_format. "
        "If this is a new category, add it to the relevant CATEGORY set "
        "in conventions.py and to the schema enum."
    )


def _default_file_options_for(category: str) -> Dict[str, Any]:
    """Return the default ``file_options`` dict for a category.

    Image categories use per-category defaults to match the templates'
    customer-tuned values (see ``DEFAULT_IMAGE_FILE_OPTIONS_BY_CATEGORY``);
    text uses a single shared default; tabular / time-series carry no
    ``file_options``.
    """
    if category in IMAGE_CATEGORIES:
        return dict(DEFAULT_IMAGE_FILE_OPTIONS_BY_CATEGORY[category])
    if category in TEXT_CATEGORIES:
        return dict(DEFAULT_TEXT_FILE_OPTIONS)
    if category in MLM_CATEGORIES:
        return dict(DEFAULT_MLM_FILE_OPTIONS)
    # Tabular / time-series categories don't carry file_options.
    return {}


__all__ = [
    "ResolvedConfig",
    "resolve",
    "IMAGE_CATEGORIES",
    "TEXT_CATEGORIES",
    "TABULAR_CATEGORIES",
    "TIME_SERIES_CATEGORIES",
    "TIME_TO_EVENT_CATEGORIES",
    "REGRESSION_CLASS_CATEGORIES",
    "DEFAULT_CSV_OPTIONS",
    "DEFAULT_IMAGE_FILE_OPTIONS_BY_CATEGORY",
    "DEFAULT_TEXT_FILE_OPTIONS",
    "MLM_CATEGORIES",
    "DEFAULT_MLM_FILE_OPTIONS",
]
