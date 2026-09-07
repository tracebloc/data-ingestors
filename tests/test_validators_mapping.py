"""Tests for map_validators — the per-category validator factory."""

from __future__ import annotations


from tracebloc_ingestor.config import Config
from tracebloc_ingestor.utils.validators_mapping import map_validators
from tracebloc_ingestor.utils.constants import TaskCategory, FileExtension
from tracebloc_ingestor.validators.file_validator import FileTypeValidator
from tracebloc_ingestor.validators.image_validator import ImageResolutionValidator
from tracebloc_ingestor.validators.data_validator import DataValidator
from tracebloc_ingestor.validators.table_name_validator import TableNameValidator
from tracebloc_ingestor.validators.duplicate_validator import DuplicateValidator
from tracebloc_ingestor.validators.xml_validator import PascalVOCXMLValidator
from tracebloc_ingestor.validators.time_to_event_validator import TimeToEventValidator
from tracebloc_ingestor.validators.time_format_validator import TimeFormatValidator
from tracebloc_ingestor.validators.numeric_columns_validator import (
    NumericColumnsValidator,
)
from tracebloc_ingestor.validators.keypoint_annotation_validator import (
    KeypointAnnotationValidator,
)
from tracebloc_ingestor.validators.keypoint_visibility_validator import (
    KeypointVisibilityValidator,
)
from tracebloc_ingestor.validators.label_diversity_validator import (
    LabelDiversityValidator,
)
from tracebloc_ingestor.validators.ingestable_records_validator import (
    IngestableRecordsValidator,
)

IMAGE_OPTS = {"extension": FileExtension.JPG, "target_size": [224, 224]}


def _types(validators):
    return [type(v) for v in validators]


def test_image_classification():
    from tracebloc_ingestor.validators.ingestable_records_validator import (
        IngestableRecordsValidator,
    )
    from tracebloc_ingestor.validators.label_column_validator import (
        LabelColumnValidator,
    )

    v = map_validators(TaskCategory.IMAGE_CLASSIFICATION, IMAGE_OPTS)
    # The 0-record guard, label-diversity, table-name and duplicate validators
    # are composed by map_validators around the category-specific middle, so the
    # guard now leads and the classification + tail validators trail.
    assert _types(v) == [
        IngestableRecordsValidator,
        FileTypeValidator,
        ImageResolutionValidator,
        LabelColumnValidator,
        LabelDiversityValidator,
        TableNameValidator,
        DuplicateValidator,
    ]


def test_vision_categories_have_zero_record_guard():
    """0-record fail-fast (header-only / empty CSV) is wired into every
    file-bearing vision category — image/object/semantic/keypoint — so a
    zero-record vision dataset is rejected at preflight instead of creating an
    orphan empty table (extends #303 from NLP to vision)."""
    from tracebloc_ingestor.validators.ingestable_records_validator import (
        IngestableRecordsValidator,
    )

    for cat in (
        TaskCategory.IMAGE_CLASSIFICATION,
        TaskCategory.OBJECT_DETECTION,
        TaskCategory.SEMANTIC_SEGMENTATION,
        TaskCategory.KEYPOINT_DETECTION,
    ):
        rec = [
            x
            for x in map_validators(cat, IMAGE_OPTS)
            if isinstance(x, IngestableRecordsValidator)
        ]
        assert len(rec) == 1, cat
        assert rec[0].file_subdir == "images", cat


def test_label_column_guard_is_image_classification_only():
    """LabelColumnValidator gates only image_classification among vision
    categories — object detection / segmentation / keypoint source labels from
    XML / masks / annotation files, not a CSV label column, so adding it there
    would wrongly reject every such dataset."""
    from tracebloc_ingestor.validators.label_column_validator import (
        LabelColumnValidator,
    )

    assert LabelColumnValidator in _types(
        map_validators(TaskCategory.IMAGE_CLASSIFICATION, IMAGE_OPTS)
    )
    for cat in (
        TaskCategory.OBJECT_DETECTION,
        TaskCategory.SEMANTIC_SEGMENTATION,
        TaskCategory.KEYPOINT_DETECTION,
    ):
        assert LabelColumnValidator not in _types(map_validators(cat, IMAGE_OPTS)), cat


def test_classification_categories_include_label_diversity():
    """Single-label classification is caught at preflight across every
    classification-family category — image/object/semantic/keypoint/
    tabular/text — but NOT token_classification (its label is a per-token
    BIO sequence, not a single class) or the regression / self-supervised
    families (issue #251)."""
    for cat in (
        TaskCategory.IMAGE_CLASSIFICATION,
        TaskCategory.OBJECT_DETECTION,
        TaskCategory.SEMANTIC_SEGMENTATION,
        TaskCategory.KEYPOINT_DETECTION,
        TaskCategory.TABULAR_CLASSIFICATION,
        TaskCategory.TEXT_CLASSIFICATION,
        TaskCategory.SENTENCE_PAIR_CLASSIFICATION,
    ):
        assert LabelDiversityValidator in _types(map_validators(cat, IMAGE_OPTS)), cat

    for cat in (
        TaskCategory.TOKEN_CLASSIFICATION,
        TaskCategory.TABULAR_REGRESSION,
        TaskCategory.TIME_SERIES_FORECASTING,
        TaskCategory.TIME_TO_EVENT_PREDICTION,
        TaskCategory.MASKED_LANGUAGE_MODELING,
        TaskCategory.CAUSAL_LANGUAGE_MODELING,
        TaskCategory.SEQ2SEQ,
        TaskCategory.EMBEDDINGS,
    ):
        assert LabelDiversityValidator not in _types(
            map_validators(cat, {"schema": {"a": "INT"}})
        ), cat


def test_label_diversity_uses_full_schema_for_label_type():
    """base.py strips the label column out of file_options["schema"] (it's a
    framework column, not a table column) but passes the UNSTRIPPED schema as
    `full_schema`. The label-diversity validator must read the label's type
    from `full_schema`, else it never applies the ingestor's NA/dtype rules to
    the label column (bugbot #252)."""
    ldv = next(
        v
        for v in map_validators(
            TaskCategory.TABULAR_CLASSIFICATION,
            {
                # file_options["schema"] — label already stripped by base.py.
                "schema": {"age": "INT"},
                "label_column": "churned",
                # full, unstripped schema base.py also passes.
                "full_schema": {"age": "INT", "churned": "VARCHAR(8)"},
            },
        )
        if isinstance(v, LabelDiversityValidator)
    )
    # The validator must see the label column's type via the full schema.
    assert ldv._schema_type_for("churned") == "VARCHAR(8)"


def test_object_detection_includes_xml_validator():
    v = map_validators(TaskCategory.OBJECT_DETECTION, IMAGE_OPTS)
    types = _types(v)
    assert PascalVOCXMLValidator in types
    # two FileTypeValidators: images + annotations
    assert types.count(FileTypeValidator) == 2


def test_semantic_segmentation():
    v = map_validators(TaskCategory.SEMANTIC_SEGMENTATION, IMAGE_OPTS)
    types = _types(v)
    assert types.count(FileTypeValidator) == 2
    assert ImageResolutionValidator in types


def test_keypoint_detection_includes_keypoint_validators():
    v = map_validators(TaskCategory.KEYPOINT_DETECTION, IMAGE_OPTS)
    types = _types(v)
    assert KeypointAnnotationValidator in types
    assert KeypointVisibilityValidator in types


def test_tabular_classification_with_schema():
    v = map_validators(TaskCategory.TABULAR_CLASSIFICATION, {"schema": {"a": "INT"}})
    types = _types(v)
    assert DataValidator in types
    assert types[-2:] == [TableNameValidator, DuplicateValidator]


def test_tabular_classification_without_schema_omits_data_validator():
    v = map_validators(TaskCategory.TABULAR_CLASSIFICATION, {})
    assert DataValidator not in _types(v)


def test_tabular_regression_with_schema():
    v = map_validators(TaskCategory.TABULAR_REGRESSION, {"schema": {"x": "FLOAT"}})
    assert DataValidator in _types(v)


def test_text_classification_defaults_extension():
    v = map_validators(TaskCategory.TEXT_CLASSIFICATION, {})
    # The 0-record guard leads; the category's own FileTypeValidator follows.
    assert _types(v)[0] is IngestableRecordsValidator
    assert FileTypeValidator in _types(v)


def test_text_classification_includes_label_column_validator_before_diversity():
    """A text-classification CSV missing the configured label column must fail
    fast at preflight (not ingest label=None -> late backend 400). The presence
    check runs BEFORE the diversity check, which only reads the column lazily."""
    from tracebloc_ingestor.validators.label_column_validator import (
        LabelColumnValidator,
    )

    types = _types(map_validators(TaskCategory.TEXT_CLASSIFICATION, {}))
    assert LabelColumnValidator in types
    assert types.index(LabelColumnValidator) < types.index(LabelDiversityValidator)


def test_text_classification_threads_custom_label_column():
    from tracebloc_ingestor.validators.label_column_validator import (
        LabelColumnValidator,
    )

    v = map_validators(TaskCategory.TEXT_CLASSIFICATION, {"label_column": "sentiment"})
    lc = next(x for x in v if isinstance(x, LabelColumnValidator))
    assert lc.label_column == "sentiment"


def test_token_classification_excludes_label_column_validator():
    """Token classification already rejects a missing label column via
    BIOLabelValidator, so it must NOT also carry LabelColumnValidator."""
    from tracebloc_ingestor.validators.label_column_validator import (
        LabelColumnValidator,
    )

    assert LabelColumnValidator not in _types(
        map_validators(TaskCategory.TOKEN_CLASSIFICATION, {})
    )


def test_token_classification_includes_bio_validator():
    from tracebloc_ingestor.validators.bio_label_validator import BIOLabelValidator

    v = map_validators(TaskCategory.TOKEN_CLASSIFICATION, {})
    types = _types(v)
    assert types[0] is IngestableRecordsValidator  # 0-record guard leads
    assert FileTypeValidator in types
    assert BIOLabelValidator in types
    assert TableNameValidator in types and DuplicateValidator in types


def test_token_classification_with_schema_adds_data_validator():
    v = map_validators(TaskCategory.TOKEN_CLASSIFICATION, {"schema": {"a": "INT"}})
    assert DataValidator in _types(v)


def test_token_classification_threads_custom_label_column():
    from tracebloc_ingestor.validators.bio_label_validator import BIOLabelValidator

    v = map_validators(TaskCategory.TOKEN_CLASSIFICATION, {"label_column": "ner_tags"})
    bio = next(x for x in v if isinstance(x, BIOLabelValidator))
    assert bio.label_column == "ner_tags"


def test_token_classification_defaults_label_column_when_unset():
    from tracebloc_ingestor.validators.bio_label_validator import BIOLabelValidator

    v = map_validators(TaskCategory.TOKEN_CLASSIFICATION, {"label_column": None})
    bio = next(x for x in v if isinstance(x, BIOLabelValidator))
    assert bio.label_column == "label"


def test_time_series_forecasting_validator_set():
    v = map_validators(
        TaskCategory.TIME_SERIES_FORECASTING,
        {"schema": {"timestamp": "TIMESTAMP", "value": "FLOAT"}},
    )
    types = _types(v)
    assert TimeFormatValidator in types
    assert NumericColumnsValidator in types
    # schema minus timestamp is non-empty -> a DataValidator is added
    assert DataValidator in types


def test_time_series_forecasting_timestamp_only_schema_no_data_validator():
    v = map_validators(
        TaskCategory.TIME_SERIES_FORECASTING,
        {"schema": {"timestamp": "TIMESTAMP"}},
    )
    assert DataValidator not in _types(v)


def test_time_to_event_with_schema():
    v = map_validators(
        TaskCategory.TIME_TO_EVENT_PREDICTION,
        {"schema": {"time": "INT"}, "time_column": "time"},
    )
    types = _types(v)
    assert TimeToEventValidator in types
    assert DataValidator in types


def test_time_to_event_without_schema():
    v = map_validators(TaskCategory.TIME_TO_EVENT_PREDICTION, {})
    types = _types(v)
    assert TimeToEventValidator in types
    assert DataValidator not in types


def test_unknown_category_returns_empty():
    assert map_validators("not_a_category", {}) == []


# --- P4b: config injection seam -------------------------------------------
# map_validators is the single place the run's resolved Config is bound to
# every validator it builds (BaseValidator.bind_config). These pin that the
# INJECTED config wins over the env-backed module-global at the read site, and
# that omitting it preserves the prior module-global fallback.


def test_map_validators_injects_config_over_module_global(monkeypatch):
    # The env-backed module-global config would resolve these values...
    monkeypatch.setenv("TABLE_NAME", "env_table")
    monkeypatch.setenv("SRC_PATH", "/env/src")
    # ...but the run's resolved Config carries DIFFERENT ones.
    injected = Config(TABLE_NAME="injected_table", SRC_PATH="/injected/src")

    validators = map_validators(TaskCategory.TABULAR_REGRESSION, {}, injected)

    assert validators
    # Every validator the factory built got the run's Config bound to it.
    assert all(v._config is injected for v in validators)
    # And path-reading validators resolve the injected value, not the env one.
    tbl = next(v for v in validators if isinstance(v, TableNameValidator))
    assert (tbl._config or Config()).TABLE_NAME == "injected_table"
    dup = next(v for v in validators if isinstance(v, DuplicateValidator))
    # DEST_PATH is derived as STORAGE_PATH/TABLE_NAME from the injected config.
    assert dup.dest_path.endswith("/injected_table")
    assert "env_table" not in dup.dest_path


def test_map_validators_without_config_falls_back_to_module_global(monkeypatch):
    monkeypatch.setenv("TABLE_NAME", "env_table")
    # No config arg -> validators stay unbound and read the module-global.
    validators = map_validators(TaskCategory.TABULAR_REGRESSION, {})

    assert validators
    assert all(v._config is None for v in validators)
    dup = next(v for v in validators if isinstance(v, DuplicateValidator))
    assert dup.dest_path.endswith("/env_table")


def test_nlp_categories_include_content_hygiene_validators():
    """The text categories (text/token classification, masked & causal LM,
    seq2seq, embeddings) gain BOTH the zero-record guard and the (text-only)
    UTF-8 content validator. The zero-record guard now also covers file-bearing
    vision categories, but TextContentValidator stays NLP-only (it decodes UTF-8
    text, meaningless for images); tabular has neither."""
    from tracebloc_ingestor.validators.ingestable_records_validator import (
        IngestableRecordsValidator,
    )
    from tracebloc_ingestor.validators.text_content_validator import (
        TextContentValidator,
    )

    for cat in (
        TaskCategory.TEXT_CLASSIFICATION,
        TaskCategory.TOKEN_CLASSIFICATION,
        TaskCategory.SENTENCE_PAIR_CLASSIFICATION,
        TaskCategory.MASKED_LANGUAGE_MODELING,
        TaskCategory.CAUSAL_LANGUAGE_MODELING,
        TaskCategory.SEQ2SEQ,
        TaskCategory.EMBEDDINGS,
    ):
        types = _types(map_validators(cat, {}))
        # 0-record guard leads (composed centrally); the category's own
        # FileTypeValidator + text-content validator follow.
        assert types[0] is IngestableRecordsValidator, cat
        assert FileTypeValidator in types, cat
        assert TextContentValidator in types, cat

    # Vision file-bearing categories get the zero-record guard but NOT the
    # text-content validator.
    for cat in (
        TaskCategory.IMAGE_CLASSIFICATION,
        TaskCategory.OBJECT_DETECTION,
        TaskCategory.SEMANTIC_SEGMENTATION,
        TaskCategory.KEYPOINT_DETECTION,
    ):
        types = _types(map_validators(cat, IMAGE_OPTS))
        assert IngestableRecordsValidator in types, cat
        assert TextContentValidator not in types, cat

    # Tabular gets the centralized 0-record guard (every category does) but NOT
    # the text-content validator (no files, not text).
    types = _types(map_validators(TaskCategory.TABULAR_CLASSIFICATION, IMAGE_OPTS))
    assert IngestableRecordsValidator in types
    assert TextContentValidator not in types


def test_mlm_content_validators_target_sequences_subdir():
    """MLM stages files under ``sequences/`` (not ``texts/``); the content
    validators must point at that subdirectory."""
    from tracebloc_ingestor.validators.ingestable_records_validator import (
        IngestableRecordsValidator,
    )
    from tracebloc_ingestor.validators.text_content_validator import (
        TextContentValidator,
    )

    v = map_validators(TaskCategory.MASKED_LANGUAGE_MODELING, {})
    rec = next(x for x in v if isinstance(x, IngestableRecordsValidator))
    txt = next(x for x in v if isinstance(x, TextContentValidator))
    assert rec.file_subdir == "sequences"
    assert txt.texts_path == "sequences"


def test_causal_lm_content_validators_target_texts_subdir():
    """Causal LM stages RAW text under ``texts/`` (not the pre-tokenized
    ``sequences/`` MLM uses); the content validators must point there."""
    from tracebloc_ingestor.validators.ingestable_records_validator import (
        IngestableRecordsValidator,
    )
    from tracebloc_ingestor.validators.text_content_validator import (
        TextContentValidator,
    )

    v = map_validators(TaskCategory.CAUSAL_LANGUAGE_MODELING, {})
    rec = next(x for x in v if isinstance(x, IngestableRecordsValidator))
    txt = next(x for x in v if isinstance(x, TextContentValidator))
    assert rec.file_subdir == "texts"
    assert txt.texts_path == "texts"


def test_causal_lm_excludes_all_label_validators():
    """Causal LM is self-supervised: only ``filename`` is required, no label
    column. It must carry none of the label-oriented validators (mirrors MLM)."""
    from tracebloc_ingestor.validators.label_column_validator import (
        LabelColumnValidator,
    )
    from tracebloc_ingestor.validators.bio_label_validator import BIOLabelValidator

    types = _types(map_validators(TaskCategory.CAUSAL_LANGUAGE_MODELING, {}))
    assert LabelColumnValidator not in types
    assert BIOLabelValidator not in types
    assert LabelDiversityValidator not in types


def test_causal_lm_with_schema_adds_data_validator():
    v = map_validators(TaskCategory.CAUSAL_LANGUAGE_MODELING, {"schema": {"a": "INT"}})
    assert DataValidator in _types(v)


def test_seq2seq_content_validators_target_texts_subdir():
    """seq2seq stages RAW text under ``texts/`` (not the pre-tokenized
    ``sequences/`` MLM uses); the content validators must point there."""
    from tracebloc_ingestor.validators.ingestable_records_validator import (
        IngestableRecordsValidator,
    )
    from tracebloc_ingestor.validators.text_content_validator import (
        TextContentValidator,
    )

    v = map_validators(TaskCategory.SEQ2SEQ, {})
    rec = next(x for x in v if isinstance(x, IngestableRecordsValidator))
    txt = next(x for x in v if isinstance(x, TextContentValidator))
    assert rec.file_subdir == "texts"
    assert txt.texts_path == "texts"


def test_seq2seq_excludes_all_label_validators():
    """seq2seq is self-supervised: only ``filename`` is required, no label
    column. It must carry none of the label-oriented validators (mirrors causal
    LM)."""
    from tracebloc_ingestor.validators.label_column_validator import (
        LabelColumnValidator,
    )
    from tracebloc_ingestor.validators.bio_label_validator import BIOLabelValidator

    types = _types(map_validators(TaskCategory.SEQ2SEQ, {}))
    assert LabelColumnValidator not in types
    assert BIOLabelValidator not in types
    assert LabelDiversityValidator not in types


def test_seq2seq_with_schema_adds_data_validator():
    v = map_validators(TaskCategory.SEQ2SEQ, {"schema": {"a": "INT"}})
    assert DataValidator in _types(v)


def test_embeddings_content_validators_target_texts_subdir():
    """embeddings stages RAW text under ``texts/`` (not the pre-tokenized
    ``sequences/`` MLM uses); the content validators must point there."""
    from tracebloc_ingestor.validators.ingestable_records_validator import (
        IngestableRecordsValidator,
    )
    from tracebloc_ingestor.validators.text_content_validator import (
        TextContentValidator,
    )

    v = map_validators(TaskCategory.EMBEDDINGS, {})
    rec = next(x for x in v if isinstance(x, IngestableRecordsValidator))
    txt = next(x for x in v if isinstance(x, TextContentValidator))
    assert rec.file_subdir == "texts"
    assert txt.texts_path == "texts"


def test_embeddings_includes_contrastive_pairs_validator():
    """embeddings carries the structural ContrastivePairsValidator (the
    pair/triplet check) ON TOP of the shared content hygiene — that structural
    check is what distinguishes it from seq2seq / causal LM (free-form text)."""
    from tracebloc_ingestor.validators.contrastive_pairs_validator import (
        ContrastivePairsValidator,
    )
    from tracebloc_ingestor.validators.text_content_validator import (
        TextContentValidator,
    )

    types = _types(map_validators(TaskCategory.EMBEDDINGS, {}))
    assert ContrastivePairsValidator in types
    assert TextContentValidator in types
    # seq2seq / causal LM do NOT structurally constrain their .txt content.
    assert ContrastivePairsValidator not in _types(
        map_validators(TaskCategory.SEQ2SEQ, {})
    )
    assert ContrastivePairsValidator not in _types(
        map_validators(TaskCategory.CAUSAL_LANGUAGE_MODELING, {})
    )


def test_embeddings_threads_custom_extension_to_contrastive_validator():
    """A custom extension reaches both the FileType and the contrastive
    structural validator so the resolved path matches what text_transfer copies."""
    from tracebloc_ingestor.validators.contrastive_pairs_validator import (
        ContrastivePairsValidator,
    )

    v = map_validators(TaskCategory.EMBEDDINGS, {"extension": ".text"})
    cp = next(x for x in v if isinstance(x, ContrastivePairsValidator))
    assert cp.extension == ".text"


def test_embeddings_excludes_all_label_validators():
    """embeddings is self-supervised: only ``filename`` is required, no label
    column. It must carry none of the label-oriented validators (mirrors
    seq2seq)."""
    from tracebloc_ingestor.validators.label_column_validator import (
        LabelColumnValidator,
    )
    from tracebloc_ingestor.validators.bio_label_validator import BIOLabelValidator

    types = _types(map_validators(TaskCategory.EMBEDDINGS, {}))
    assert LabelColumnValidator not in types
    assert BIOLabelValidator not in types
    assert LabelDiversityValidator not in types


def test_embeddings_with_schema_adds_data_validator():
    v = map_validators(TaskCategory.EMBEDDINGS, {"schema": {"a": "INT"}})
    assert DataValidator in _types(v)


def test_sentence_pair_content_validators_target_texts_subdir():
    """sentence_pair_classification stages RAW text under ``texts/`` (the same
    layout as text_classification); the content validators must point there."""
    from tracebloc_ingestor.validators.ingestable_records_validator import (
        IngestableRecordsValidator,
    )
    from tracebloc_ingestor.validators.text_content_validator import (
        TextContentValidator,
    )

    v = map_validators(TaskCategory.SENTENCE_PAIR_CLASSIFICATION, {})
    rec = next(x for x in v if isinstance(x, IngestableRecordsValidator))
    txt = next(x for x in v if isinstance(x, TextContentValidator))
    assert rec.file_subdir == "texts"
    assert txt.texts_path == "texts"


def test_sentence_pair_includes_structural_and_label_validators():
    """sentence_pair carries the structural SentencePairValidator (the exactly-2
    tab-fields check) ON TOP of the shared content hygiene — that structural
    check is what distinguishes it from text_classification (free-form text). As
    a SUPERVISED category it also keeps LabelColumnValidator, which must run
    BEFORE the (centrally-composed) LabelDiversityValidator."""
    from tracebloc_ingestor.validators.sentence_pair_validator import (
        SentencePairValidator,
    )
    from tracebloc_ingestor.validators.label_column_validator import (
        LabelColumnValidator,
    )
    from tracebloc_ingestor.validators.text_content_validator import (
        TextContentValidator,
    )

    types = _types(map_validators(TaskCategory.SENTENCE_PAIR_CLASSIFICATION, {}))
    assert SentencePairValidator in types
    assert TextContentValidator in types
    assert LabelColumnValidator in types
    assert types.index(LabelColumnValidator) < types.index(LabelDiversityValidator)
    # text_classification does NOT structurally constrain its .txt content.
    assert SentencePairValidator not in _types(
        map_validators(TaskCategory.TEXT_CLASSIFICATION, {})
    )


def test_sentence_pair_threads_custom_extension_to_structural_validator():
    """A custom extension reaches the structural validator so the resolved path
    matches what text_transfer copies."""
    from tracebloc_ingestor.validators.sentence_pair_validator import (
        SentencePairValidator,
    )

    v = map_validators(
        TaskCategory.SENTENCE_PAIR_CLASSIFICATION, {"extension": ".text"}
    )
    sp = next(x for x in v if isinstance(x, SentencePairValidator))
    assert sp.extension == ".text"


def test_sentence_pair_threads_custom_label_column():
    from tracebloc_ingestor.validators.label_column_validator import (
        LabelColumnValidator,
    )

    v = map_validators(
        TaskCategory.SENTENCE_PAIR_CLASSIFICATION, {"label_column": "relation"}
    )
    lc = next(x for x in v if isinstance(x, LabelColumnValidator))
    assert lc.label_column == "relation"


def test_sentence_pair_with_schema_adds_data_validator():
    v = map_validators(
        TaskCategory.SENTENCE_PAIR_CLASSIFICATION, {"schema": {"a": "INT"}}
    )
    assert DataValidator in _types(v)


def test_text_classification_content_validators_target_texts_subdir():
    from tracebloc_ingestor.validators.ingestable_records_validator import (
        IngestableRecordsValidator,
    )
    from tracebloc_ingestor.validators.text_content_validator import (
        TextContentValidator,
    )

    v = map_validators(TaskCategory.TEXT_CLASSIFICATION, {})
    rec = next(x for x in v if isinstance(x, IngestableRecordsValidator))
    txt = next(x for x in v if isinstance(x, TextContentValidator))
    assert rec.file_subdir == "texts"
    assert txt.texts_path == "texts"


def test_semantic_segmentation_validates_masks_resolution():
    """Seg masks get their own ImageResolutionValidator(subdir="masks") so a
    corrupt or wrong-sized mask is rejected (the default instance only scans
    images). PR #314."""
    v = map_validators(TaskCategory.SEMANTIC_SEGMENTATION, IMAGE_OPTS)
    res = [x for x in v if isinstance(x, ImageResolutionValidator)]
    subdirs = sorted(x.subdir for x in res)
    assert subdirs == ["images", "masks"], subdirs


def test_keypoint_annotation_validator_gets_image_bounds():
    """KeypointAnnotationValidator is given the declared target_size so it can
    reject keypoints past the image edge. PR #314."""
    from tracebloc_ingestor.validators.keypoint_annotation_validator import (
        KeypointAnnotationValidator,
    )

    v = map_validators(
        TaskCategory.KEYPOINT_DETECTION,
        {**IMAGE_OPTS, "number_of_keypoints": 5},
    )
    kp = next(x for x in v if isinstance(x, KeypointAnnotationValidator))
    assert kp.expected_resolution == IMAGE_OPTS["target_size"]


ALL_CATEGORIES = (
    TaskCategory.IMAGE_CLASSIFICATION,
    TaskCategory.OBJECT_DETECTION,
    TaskCategory.SEMANTIC_SEGMENTATION,
    TaskCategory.KEYPOINT_DETECTION,
    TaskCategory.TEXT_CLASSIFICATION,
    TaskCategory.TOKEN_CLASSIFICATION,
    TaskCategory.SENTENCE_PAIR_CLASSIFICATION,
    TaskCategory.MASKED_LANGUAGE_MODELING,
    TaskCategory.CAUSAL_LANGUAGE_MODELING,
    TaskCategory.SEQ2SEQ,
    TaskCategory.EMBEDDINGS,
    TaskCategory.TABULAR_CLASSIFICATION,
    TaskCategory.TABULAR_REGRESSION,
    TaskCategory.TIME_SERIES_FORECASTING,
    TaskCategory.TIME_TO_EVENT_PREDICTION,
)
_CLASSIFICATION = {
    TaskCategory.IMAGE_CLASSIFICATION,
    TaskCategory.OBJECT_DETECTION,
    TaskCategory.SEMANTIC_SEGMENTATION,
    TaskCategory.KEYPOINT_DETECTION,
    TaskCategory.TEXT_CLASSIFICATION,
    TaskCategory.SENTENCE_PAIR_CLASSIFICATION,
    TaskCategory.TABULAR_CLASSIFICATION,
}


def test_common_validator_frame_composed_for_every_category():
    """map_validators wraps every category's factory output in the common
    frame: 0-record guard first, table-name + duplicate last, and label-diversity
    second-to-last for classification families only — declared once, not per
    factory."""
    opts = {"extension": ".jpg", "target_size": [64, 64], "number_of_keypoints": 5}
    for cat in ALL_CATEGORIES:
        types = _types(map_validators(cat, opts))
        assert types[0] is IngestableRecordsValidator, cat
        assert types[-2:] == [TableNameValidator, DuplicateValidator], cat
        if cat in _CLASSIFICATION:
            assert types[-3] is LabelDiversityValidator, cat
        else:
            assert LabelDiversityValidator not in types, cat


def test_duplicate_validator_receives_the_runs_data_id_source():
    """#377: the tail DuplicateValidator gets the run's data_id strategy and id
    column, so its duplicate-filename warning describes what actually happens
    (content_hash collapses byte-identical rows; uuid / an id column don't)."""
    opts = {
        "extension": ".jpg",
        "target_size": [64, 64],
        "number_of_keypoints": 5,
        "data_id_strategy": "content_hash",
        "unique_id_column": "row_id",
    }
    for cat in ALL_CATEGORIES:
        dup = map_validators(cat, opts)[-1]
        assert isinstance(dup, DuplicateValidator), cat
        assert dup._data_id_strategy == "content_hash", cat
        assert dup._unique_id_column == "row_id", cat


def test_duplicate_validator_data_id_source_defaults_to_unknown():
    """Callers that omit the keys (tests, direct callers) leave the validator on
    its strategy-agnostic wording rather than asserting a wrong outcome."""
    dup = map_validators(TaskCategory.TABULAR_CLASSIFICATION, {})[-1]
    assert dup._data_id_strategy is None
    assert dup._unique_id_column is None
