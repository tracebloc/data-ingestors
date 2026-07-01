"""Contrastive Pairs Validator Module.

Structural check for the ``embeddings`` (self-supervised contrastive) modality.

Unlike the other raw-text NLP modalities (causal language modeling, seq2seq),
which also accept free-form text, an embeddings sample has a STRICT on-disk
shape: each ``.txt`` is a single tab-separated record, either

- a **pair** ``anchor<TAB>positive``                       (2 fields), or
- a **triplet** ``anchor<TAB>positive<TAB>negative``        (3 fields).

There is no label column — the supervision signal is the pairing itself. A file
that isn't exactly 2 or 3 non-empty tab fields (e.g. plain prose with no tab, an
empty field, or several records crammed into one file) is malformed: the
training client would silently mis-split it into the wrong number of views. So
we reject it here, against the dataset author, rather than letting it corrupt
training.

The manifest walk, path resolution and record-level checks live in the shared
:class:`~tracebloc_ingestor.validators.tab_separated_record_validator.TabSeparatedRecordValidator`
base (also used by ``sentence_pair_classification``); this subclass only pins
the pair-or-triplet field contract and its phrasing. UTF-8 / binary hygiene
stays the shared ``TextContentValidator``'s job; emptiness it already warns
about, so an empty / whitespace-only file is left untouched here (no double
reporting).
"""

from typing import Tuple

from ..utils.constants import FileExtension
from .tab_separated_record_validator import TabSeparatedRecordValidator


class ContrastivePairsValidator(TabSeparatedRecordValidator):
    """Validate that each referenced ``embeddings`` ``.txt`` is a tab-separated
    pair (``anchor<TAB>positive``) or triplet (``anchor<TAB>positive<TAB>negative``).
    """

    ALLOWED_FIELD_COUNTS: Tuple[int, ...] = (2, 3)
    _ERROR_NOUN = "contrastive pairs"

    def __init__(
        self,
        texts_path: str = "texts",
        extension: str = FileExtension.TXT,
        filename_column: str = "filename",
        name: str = "Contrastive Pairs",
    ):
        super().__init__(
            texts_path=texts_path,
            extension=extension,
            filename_column=filename_column,
            name=name,
        )

    def _expected_fields_phrase(self) -> str:
        return "2 (anchor<TAB>positive) or 3 (anchor<TAB>positive<TAB>negative)"

    def _multiline_hint(self) -> str:
        return (
            "Put one 'anchor<TAB>positive' pair (or "
            "'anchor<TAB>positive<TAB>negative' triplet) per .txt."
        )

    def _field_names(self, count: int) -> str:
        return f"anchor, positive{', negative' if count == 3 else ''}"
