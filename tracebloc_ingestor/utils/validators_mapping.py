"""Resolve the validator set for a task category.

The per-category validator factories now live on each ``ModalitySpec``
(bodies in ``modalities/validators.py``, attached in ``modalities/registry.py``
— structural refactor backend#796, P3b). This stays the entry point
``BaseIngestor.validate_data`` calls; it is now a thin lookup over the registry
rather than an 11-arm if/elif.

An unknown / ``None`` category returns ``[]`` (no validators) — preserving the
prior fall-through. That is kept loud at the gate by
``tests/test_category_congruence.py``, which requires every schema-enum
category to resolve a non-empty set.

This is also the single seam where the run's resolved :class:`Config` is
injected into the validators (structural refactor backend#796, P4b): the
per-category factories stay ``(options) -> [validators]`` and this function
calls ``bind_config`` on each result. So the path-reading validators read the
ingestor's explicitly-resolved Config rather than a module-global ``Config()``
that snapshots ``os.environ`` — instead of threading ``config=`` through ~40
factory construction sites.
"""

from typing import Any, Dict, List, Optional

from ..config import Config
from ..modalities.registry import REGISTRY
from ..modalities.validators import label_diversity_validator
from ..utils.constants import FileExtension
from ..validators.base import BaseValidator
from ..validators.duplicate_validator import DuplicateValidator
from ..validators.ingestable_records_validator import IngestableRecordsValidator
from ..validators.table_name_validator import TableNameValidator


def map_validators(
    task_category: str,
    options: Dict[str, Any],
    config: Optional[Config] = None,
) -> List[BaseValidator]:
    spec = REGISTRY.get(task_category)
    if spec is None:
        return []

    # Compose the chain from a common frame + the category-specific middle, so
    # the universally-applicable validators live ONCE here instead of being
    # repeated in every per-category factory:
    #   [0-record guard] + <category-specific> + [label-diversity?] + [tail]
    validators: List[BaseValidator] = [
        # Every CSV manifest must yield >= 1 ingestable record. ``file_subdir``
        # (from the spec) adds the "all referenced files missing" check for
        # file-bearing categories; ``None`` (tabular / time) runs the header-only
        # / empty-CSV row check alone.
        IngestableRecordsValidator(
            file_subdir=spec.file_subdir,
            extension=options.get("extension", FileExtension.TXT),
            # Thread the run's dialect so the manifest reads (header probe,
            # referenced-files scan) tokenize like the ingest write path —
            # else a non-comma / BOM manifest false-rejects the exact-filename
            # check (#372/#376). Mirrors the grouped validators.
            csv_options=options.get("csv_options"),
        )
    ]
    # Thread the spec's grouping trait into the factory options so the
    # group/time column names have ONE source of truth (the ModalitySpec) —
    # the per-category factories must never re-hardcode the names that
    # ``ingestors/base.py`` reads from the same trait (review: #359).
    if spec.grouping is not None:
        options = {**options, "grouping": spec.grouping}
    validators += spec.build_validators(options)
    # Classification-family datasets need >= 2 distinct labels.
    if spec.is_classification:
        validators.append(label_diversity_validator(options))
    # Universal tail: a unique table name + de-duplicated record IDs. The
    # duplicate warning's wording depends on how ``data_id`` is derived — an
    # explicit id column and 'uuid' keep every duplicate row, 'content_hash'
    # collapses the byte-identical ones — so thread both through (#377).
    validators += [
        TableNameValidator(),
        DuplicateValidator(
            data_id_strategy=options.get("data_id_strategy"),
            unique_id_column=options.get("unique_id_column"),
        ),
    ]

    # Inject the run's Config into every validator (P4b). Optional so direct
    # callers / tests that omit it keep the prior behavior: the path-reading
    # validators fall back to their module-global ``config`` at the read site.
    if config is not None:
        for validator in validators:
            validator.bind_config(config)
    return validators
