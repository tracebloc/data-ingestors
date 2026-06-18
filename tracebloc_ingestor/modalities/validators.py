"""Per-category validator factories (structural refactor — backend#796, P3b).

One factory per task category, each ``(options) -> [validators]``. These are
the bodies of the old ``utils.validators_mapping.map_validators`` if/elif arms,
moved verbatim so the validator sets are byte-for-byte identical — now attached
to each ModalitySpec (``build_validators``) instead of dispatched by a ladder.
``map_validators`` is now a thin lookup over the registry.
"""

from __future__ import annotations

from typing import Any, Dict, List

from ..utils.constants import FileExtension
from ..validators.base import BaseValidator
from ..validators.bio_label_validator import BIOLabelValidator
from ..validators.data_validator import DataValidator
from ..validators.duplicate_validator import DuplicateValidator
from ..validators.file_pairing_validator import FilePairingValidator
from ..validators.file_validator import FileTypeValidator
from ..validators.image_validator import ImageResolutionValidator
from ..validators.ingestable_records_validator import IngestableRecordsValidator
from ..validators.keypoint_annotation_validator import KeypointAnnotationValidator
from ..validators.keypoint_visibility_validator import KeypointVisibilityValidator
from ..validators.label_column_validator import LabelColumnValidator
from ..validators.label_diversity_validator import LabelDiversityValidator
from ..validators.numeric_columns_validator import NumericColumnsValidator
from ..validators.table_name_validator import TableNameValidator
from ..validators.text_content_validator import TextContentValidator
from ..validators.time_before_today_validator import TimeBeforeTodayValidator
from ..validators.time_format_validator import TimeFormatValidator
from ..validators.time_ordered_validator import TimeOrderedValidator
from ..validators.time_to_event_validator import TimeToEventValidator
from ..validators.xml_validator import PascalVOCXMLValidator


def _label_diversity_validator(options: Dict[str, Any]) -> LabelDiversityValidator:
    """Construct a LabelDiversityValidator using the user-configured label
    column name (or the framework default ``label``). Centralised so every
    classification-family factory wires the same instance shape.

    Issue #251: a classification dataset with one distinct label value is
    unlearnable and the backend rejects it at ``/global_meta/prepare/`` with
    ``HTTP 400: "Please provide atleast 2 labels."``. Catching it at preflight
    surfaces the actual cause (and lists the offending label value(s)) instead
    of cascading to a misleading "Backend failed to prepare the dataset"
    message after the rows have already landed in MySQL.
    """
    return LabelDiversityValidator(
        label_column=options.get("label_column") or "label",
        # Read the label column with the SAME NA / dtype rules CSVIngestor
        # uses, or the distinct-label count disagrees with what's actually
        # ingested (bugbot #252). Prefer ``full_schema`` (base.py passes the
        # UNSTRIPPED schema here): ``schema``/``file_options["schema"]`` has the
        # label column removed, so it can't carry the label's type — the very
        # column this validator reads. Fall back to ``schema`` for direct
        # callers / tests that pass an unstripped map.
        schema=options.get("full_schema") or options.get("schema"),
    )


def _zero_record_validator(
    options: Dict[str, Any], subdir: str = "images"
) -> IngestableRecordsValidator:
    """0-record fail-fast guard for file-bearing vision categories — the #303
    pattern extended from NLP to vision. Rejects a header-only / empty CSV (and
    a manifest whose every referenced file is missing) BEFORE the table is
    created, so a zero-record vision dataset fails early with a clear message
    instead of an orphan empty table + a late, misleading registration error.
    ``subdir`` mirrors the category's image ``FileTypeValidator(path=...)``."""
    return IngestableRecordsValidator(
        file_subdir=subdir, extension=options.get("extension", FileExtension.JPG)
    )


def image_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    return [
        FileTypeValidator(allowed_extension=options["extension"], path="images"),
        _zero_record_validator(options),
        ImageResolutionValidator(expected_resolution=options["target_size"]),
        # Fail fast when the configured label column is absent from the CSV
        # (else every record cleans to label=None and the backend rejects each
        # row with HTTP 400 "label: may not be null"). image_classification is
        # the only vision category whose label is a CSV column — object
        # detection / segmentation / keypoint source labels from XML / masks /
        # annotation files, so they do NOT get this validator.
        LabelColumnValidator(label_column=options.get("label_column") or "label"),
        _label_diversity_validator(options),
        TableNameValidator(),
        DuplicateValidator(),
    ]


def object_detection(options: Dict[str, Any]) -> List[BaseValidator]:
    return [
        FileTypeValidator(allowed_extension=options["extension"], path="images"),
        FileTypeValidator(allowed_extension=".xml", path="annotations"),
        _zero_record_validator(options),
        PascalVOCXMLValidator(),
        FilePairingValidator(
            image_path="images",
            sidecar_path="annotations",
            sidecar_label="annotation",
        ),
        ImageResolutionValidator(expected_resolution=options["target_size"]),
        _label_diversity_validator(options),
        TableNameValidator(),
        DuplicateValidator(),
    ]


def tabular_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Reject a header-only / empty CSV up front (file_subdir=None -> row-count
    # check only, no file pairing). DataValidator catches this too, but only
    # when a schema is supplied; this closes the no-schema path that would
    # otherwise create an empty table and fail late at registration.
    validators.append(IngestableRecordsValidator(file_subdir=None))
    # Add data validator if schema is provided
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    validators.append(_label_diversity_validator(options))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators


def _nlp_content_validators(
    options: Dict[str, Any], subdir: str
) -> List[BaseValidator]:
    """The NLP content-hygiene pair, gated on the text categories (text/token
    classification, MLM): reject a manifest that would ingest zero records
    (header-only CSV, or every referenced file missing), and reject text files
    whose CONTENT is binary / non-UTF-8 (warn on empty docs). ``subdir`` is the
    per-category text directory under ``SRC_PATH`` (``"texts"`` / ``"sequences"``),
    matching the category's ``FileTypeValidator(path=...)``."""
    extension = options.get("extension", FileExtension.TXT)
    return [
        IngestableRecordsValidator(file_subdir=subdir, extension=extension),
        TextContentValidator(texts_path=subdir, extension=extension),
    ]


def text_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Add text file validator
    validators.append(
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="texts",
        ),
    )
    # Reject a zero-record manifest and binary/empty text content up front.
    validators.extend(_nlp_content_validators(options, "texts"))
    # Fail fast when the configured label column is absent from the CSV header
    # (otherwise every record cleans to label=None and the backend rejects each
    # row with HTTP 400 "label: may not be null" — a late, confusing failure).
    # Token classification already covers this via BIOLabelValidator, which
    # resolves and rejects a missing label column itself.
    validators.append(
        LabelColumnValidator(label_column=options.get("label_column") or "label")
    )
    # Add data validator if schema is provided
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    validators.append(_label_diversity_validator(options))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators


def token_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Validate text file extensions (one .txt of whitespace-tokenized words
    # per sample, same layout as text classification).
    validators.append(
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="texts",
        ),
    )
    # Reject a zero-record manifest and binary/empty text content up front.
    validators.extend(_nlp_content_validators(options, "texts"))
    # Validate BIO labels: one tag per word, valid BIO/IOB2 format. Honor a
    # custom label column name when one is configured in the YAML.
    validators.append(
        BIOLabelValidator(
            texts_path="texts",
            extension=options.get("extension", FileExtension.TXT),
            label_column=options.get("label_column") or "label",
        )
    )
    # Add data validator if schema is provided
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators


def time_series_forecasting(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    schema = options.get("schema", {})
    validators.append(TimeFormatValidator(schema=schema))
    validators.append(TimeOrderedValidator())
    validators.append(TimeBeforeTodayValidator())
    validators.append(NumericColumnsValidator(schema=schema))
    if options.get("schema"):
        schema_without_timestamp = {
            k: v for k, v in options["schema"].items() if k.lower() != "timestamp"
        }
        if schema_without_timestamp:
            validators.append(DataValidator(schema=schema_without_timestamp))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators


def tabular_regression(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Reject a header-only / empty CSV up front (no-schema path has no
    # DataValidator to catch it; file_subdir=None -> row-count check only).
    validators.append(IngestableRecordsValidator(file_subdir=None))
    # Add data validator if schema is provided
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators


def time_to_event_prediction(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Add time to event validator with schema to identify time column
    if options.get("schema"):
        validators.append(
            TimeToEventValidator(
                schema=options["schema"],
                time_column=options.get("time_column"),
            )
        )
    else:
        # If no schema, use default time column name
        validators.append(
            TimeToEventValidator(time_column=options.get("time_column", "time"))
        )
    # Add data validator if schema is provided
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators


def semantic_segmentation(options: Dict[str, Any]) -> List[BaseValidator]:
    return [
        FileTypeValidator(allowed_extension=options["extension"], path="images"),
        FileTypeValidator(allowed_extension=FileExtension.PNG, path="masks"),
        _zero_record_validator(options),
        FilePairingValidator(
            image_path="images",
            sidecar_path="masks",
            sidecar_label="mask",
            # Documented + shipped convention for semantic_segmentation masks
            # is `<filename>_mask.png` (#196). Strip the suffix before matching
            # so image_001.jpg pairs with image_001_mask.png. object_detection's
            # pairing is plain stem (no suffix) — the default.
            sidecar_suffix="_mask",
        ),
        ImageResolutionValidator(expected_resolution=options["target_size"]),
        # Masks are pixel-wise label maps: validate they're readable PNGs and
        # share the images' resolution. The default ImageResolution instance only
        # scans <SRC>/images, so without this a corrupt mask, or a mask whose size
        # differs from its image, would slip through to training.
        ImageResolutionValidator(
            expected_resolution=options["target_size"],
            name="Mask Resolution Validator",
            subdir="masks",
        ),
        _label_diversity_validator(options),
        TableNameValidator(),
        DuplicateValidator(),
    ]


def keypoint_detection(options: Dict[str, Any]) -> List[BaseValidator]:
    # ``number_of_keypoints`` is required by the ingest schema for
    # keypoint_detection (see ``schema/ingest.v1.json``) and plumbed into
    # ``file_options`` by ``cli/conventions.py``. Passing it to
    # ``KeypointAnnotationValidator`` enables the per-row count check that
    # rejects datasets whose annotations drift from the declared K.
    return [
        FileTypeValidator(allowed_extension=options["extension"], path="images"),
        _zero_record_validator(options),
        ImageResolutionValidator(expected_resolution=options["target_size"]),
        KeypointAnnotationValidator(
            num_keypoints=options.get("number_of_keypoints"),
            # Bound keypoint coords by the declared image size (images are
            # enforced to target_size by ImageResolutionValidator above).
            expected_resolution=options.get("target_size"),
        ),
        KeypointVisibilityValidator(),
        _label_diversity_validator(options),
        TableNameValidator(),
        DuplicateValidator(),
    ]


def masked_language_modeling(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Validate text file extensions
    validators.append(
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="sequences",
        ),
    )
    # Reject a zero-record manifest and binary/empty text content up front.
    validators.extend(_nlp_content_validators(options, "sequences"))
    # Add data validator if schema is provided
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    validators.append(TableNameValidator())
    validators.append(DuplicateValidator())
    return validators
