"""Per-category validator factories (structural refactor — backend#796, P3b).

One factory per task category, each ``(options) -> [validators]``, attached to a
``ModalitySpec`` (``build_validators``).

Each factory returns ONLY the validators **specific to that category**. The
universally-applicable validators are composed once in
``utils.validators_mapping.map_validators`` around the factory's output, driven
by declarative ``ModalitySpec`` traits, so they are not repeated per category:

    [IngestableRecordsValidator]            # 0-record guard — every category
      + <factory(options)>                  # category-specific (this file)
      + [LabelDiversityValidator]           # iff spec.is_classification
      + [TableNameValidator, DuplicateValidator]   # every category

So a factory here never lists the 0-record guard, label-diversity, table-name,
or duplicate validators — adding one would double it.
"""

from __future__ import annotations

from typing import Any, Dict, List

from ..utils.constants import FileExtension
from ..validators.base import BaseValidator
from ..validators.bio_label_validator import BIOLabelValidator
from ..validators.contrastive_pairs_validator import ContrastivePairsValidator
from ..validators.data_validator import DataValidator
from ..validators.file_pairing_validator import FilePairingValidator
from ..validators.file_validator import FileTypeValidator
from ..validators.image_validator import ImageResolutionValidator
from ..validators.keypoint_annotation_validator import KeypointAnnotationValidator
from ..validators.keypoint_visibility_validator import KeypointVisibilityValidator
from ..validators.label_column_validator import LabelColumnValidator
from ..validators.label_diversity_validator import LabelDiversityValidator
from ..validators.numeric_columns_validator import NumericColumnsValidator
from ..validators.sentence_pair_validator import SentencePairValidator
from ..validators.text_content_validator import TextContentValidator
from ..validators.time_before_today_validator import TimeBeforeTodayValidator
from ..validators.time_format_validator import TimeFormatValidator
from ..validators.time_ordered_validator import TimeOrderedValidator
from ..validators.time_to_event_validator import TimeToEventValidator
from ..validators.xml_validator import PascalVOCXMLValidator


def label_diversity_validator(options: Dict[str, Any]) -> LabelDiversityValidator:
    """Construct a LabelDiversityValidator using the user-configured label
    column name (or the framework default ``label``). Composed centrally by
    ``map_validators`` for every ``is_classification`` category.

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


def _text_content_validator(
    options: Dict[str, Any], subdir: str
) -> TextContentValidator:
    """NLP text-content check: reject binary / non-UTF-8 files, warn on empty
    docs. ``subdir`` is the per-category text directory (``texts`` / ``sequences``).
    The 0-record guard that used to be paired with this is now applied centrally
    in ``map_validators`` (via ``spec.file_subdir``)."""
    return TextContentValidator(
        texts_path=subdir, extension=options.get("extension", FileExtension.TXT)
    )


def image_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    return [
        FileTypeValidator(allowed_extension=options["extension"], path="images"),
        ImageResolutionValidator(expected_resolution=options["target_size"]),
        # Fail fast when the configured label column is absent from the CSV
        # (else every record cleans to label=None and the backend rejects each
        # row with HTTP 400 "label: may not be null"). image_classification is
        # the only vision category whose label is a CSV column — object
        # detection / segmentation / keypoint source labels from XML / masks /
        # annotation files, so they do NOT get this validator.
        LabelColumnValidator(label_column=options.get("label_column") or "label"),
    ]


def object_detection(options: Dict[str, Any]) -> List[BaseValidator]:
    return [
        FileTypeValidator(allowed_extension=options["extension"], path="images"),
        FileTypeValidator(allowed_extension=".xml", path="annotations"),
        PascalVOCXMLValidator(),
        FilePairingValidator(
            image_path="images",
            sidecar_path="annotations",
            sidecar_label="annotation",
        ),
        ImageResolutionValidator(expected_resolution=options["target_size"]),
    ]


def semantic_segmentation(options: Dict[str, Any]) -> List[BaseValidator]:
    return [
        FileTypeValidator(allowed_extension=options["extension"], path="images"),
        FileTypeValidator(allowed_extension=FileExtension.PNG, path="masks"),
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
    ]


def keypoint_detection(options: Dict[str, Any]) -> List[BaseValidator]:
    # ``number_of_keypoints`` is required by the ingest schema for
    # keypoint_detection (see ``schema/ingest.v1.json``) and plumbed into
    # ``file_options`` by ``cli/conventions.py``. Passing it to
    # ``KeypointAnnotationValidator`` enables the per-row count check that
    # rejects datasets whose annotations drift from the declared K.
    return [
        FileTypeValidator(allowed_extension=options["extension"], path="images"),
        ImageResolutionValidator(expected_resolution=options["target_size"]),
        KeypointAnnotationValidator(
            num_keypoints=options.get("number_of_keypoints"),
            # Bound keypoint coords by the declared image size (images are
            # enforced to target_size by ImageResolutionValidator above).
            expected_resolution=options.get("target_size"),
        ),
        KeypointVisibilityValidator(),
    ]


def text_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = [
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="texts",
        ),
        _text_content_validator(options, "texts"),
        # Fail fast when the configured label column is absent from the CSV
        # header (otherwise every record cleans to label=None and the backend
        # rejects each row with HTTP 400 "label: may not be null"). Token
        # classification covers this via BIOLabelValidator instead.
        LabelColumnValidator(label_column=options.get("label_column") or "label"),
    ]
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    return validators


def token_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = [
        # One .txt of whitespace-tokenized words per sample (same layout as
        # text classification).
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="texts",
        ),
        _text_content_validator(options, "texts"),
        # Validate BIO labels: one tag per word, valid BIO/IOB2 format; also
        # rejects a missing label column. Honor a custom label column name.
        BIOLabelValidator(
            texts_path="texts",
            extension=options.get("extension", FileExtension.TXT),
            label_column=options.get("label_column") or "label",
        ),
    ]
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    return validators


def sentence_pair_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    # SUPERVISED text classification (the class label travels in the labels CSV,
    # exactly like text_classification) — so it carries the same FileType +
    # content + LabelColumn validators, and map_validators adds LabelDiversity
    # (is_classification) + DataValidator (with a schema) around them. What's
    # DISTINCT: each .txt is a STRUCTURED tab-separated ``text_a\ttext_b`` pair,
    # so beyond the shared TextContentValidator (UTF-8/binary hygiene) it adds a
    # centralized SentencePairValidator that rejects any file that isn't exactly
    # 2 non-empty tab fields (no plain prose, no empty side, one record/file).
    validators: List[BaseValidator] = [
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="texts",
        ),
        _text_content_validator(options, "texts"),
        SentencePairValidator(
            texts_path="texts",
            extension=options.get("extension", FileExtension.TXT),
        ),
        # Fail fast when the configured label column is absent from the CSV
        # header (otherwise every record cleans to label=None and the backend
        # rejects each row with HTTP 400 "label: may not be null") — same as
        # text_classification.
        LabelColumnValidator(label_column=options.get("label_column") or "label"),
    ]
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    return validators


def masked_language_modeling(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = [
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="sequences",
        ),
        _text_content_validator(options, "sequences"),
    ]
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    return validators


def causal_language_modeling(options: Dict[str, Any]) -> List[BaseValidator]:
    # Self-supervised, like MLM — only a ``filename`` column is required, no
    # label column, so no LabelColumn/BIO/LabelDiversity validators. Each sample
    # is one ``.txt`` of RAW text: plain text (pretraining) or a tab-separated
    # ``prompt\tcompletion`` pair (SFT). Both are valid UTF-8 text content, so
    # the shared TextContentValidator (binary/non-UTF-8 reject, empty warn) is
    # the whole content check — there is no extra structural rule to enforce.
    # Stages from ``texts/`` (raw text), not ``sequences/`` (pre-tokenized).
    validators: List[BaseValidator] = [
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="texts",
        ),
        _text_content_validator(options, "texts"),
    ]
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    return validators


def seq2seq(options: Dict[str, Any]) -> List[BaseValidator]:
    # Self-supervised, like causal_language_modeling — only a ``filename``
    # column is required, no label column, so no LabelColumn/BIO/LabelDiversity
    # validators. Each sample is one ``.txt`` of RAW text: a tab-separated
    # ``source\ttarget`` pair (same shape as causal LM's ``prompt\tcompletion``).
    # Both sides are valid UTF-8 text, so the shared TextContentValidator
    # (binary/non-UTF-8 reject, empty warn) is the whole content check — there
    # is no extra structural rule to enforce. Stages from ``texts/`` (raw text),
    # not ``sequences/`` (pre-tokenized).
    validators: List[BaseValidator] = [
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="texts",
        ),
        _text_content_validator(options, "texts"),
    ]
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    return validators


def embeddings(options: Dict[str, Any]) -> List[BaseValidator]:
    # Self-supervised (contrastive), like seq2seq — only a ``filename`` column
    # is required, no label column, so no LabelColumn/BIO/LabelDiversity
    # validators. Each sample is one ``.txt`` of RAW text staged from ``texts/``.
    # UNLIKE seq2seq / causal LM (free-form text), the on-disk shape is
    # STRUCTURED: a tab-separated ``anchor\tpositive`` pair OR an
    # ``anchor\tpositive\tnegative`` triplet. So beyond the shared
    # TextContentValidator (UTF-8 / binary hygiene) it adds a structural
    # ContrastivePairsValidator that rejects any file that isn't exactly 2 or 3
    # non-empty tab fields.
    validators: List[BaseValidator] = [
        FileTypeValidator(
            allowed_extension=options.get("extension", FileExtension.TXT),
            path="texts",
        ),
        _text_content_validator(options, "texts"),
        ContrastivePairsValidator(
            texts_path="texts",
            extension=options.get("extension", FileExtension.TXT),
        ),
    ]
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    return validators


def tabular_classification(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    return validators


def tabular_regression(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    return validators


def time_series_forecasting(options: Dict[str, Any]) -> List[BaseValidator]:
    schema = options.get("schema", {})
    validators: List[BaseValidator] = [
        TimeFormatValidator(schema=schema),
        TimeOrderedValidator(),
        TimeBeforeTodayValidator(),
        NumericColumnsValidator(schema=schema),
    ]
    if options.get("schema"):
        schema_without_timestamp = {
            k: v for k, v in options["schema"].items() if k.lower() != "timestamp"
        }
        if schema_without_timestamp:
            validators.append(DataValidator(schema=schema_without_timestamp))
    return validators


def time_to_event_prediction(options: Dict[str, Any]) -> List[BaseValidator]:
    validators: List[BaseValidator] = []
    # Time-to-event validator identifies + checks the (non-negative, numeric)
    # time column. With a schema it can resolve the column from it; otherwise it
    # falls back to the default/declared name.
    if options.get("schema"):
        validators.append(
            TimeToEventValidator(
                schema=options["schema"],
                time_column=options.get("time_column"),
            )
        )
    else:
        validators.append(
            TimeToEventValidator(time_column=options.get("time_column", "time"))
        )
    if options.get("schema"):
        validators.append(DataValidator(schema=options["schema"]))
    return validators
