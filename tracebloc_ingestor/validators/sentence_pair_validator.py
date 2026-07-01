"""Sentence-Pair Validator Module.

Structural check for the ``sentence_pair_classification`` modality.

sentence_pair_classification is SUPERVISED text classification (the class label
travels in the labels CSV, exactly like ``text_classification``), but each
``.txt`` holds a STRICT on-disk shape: a single tab-separated pair of sentences

    text_a<TAB>text_b                                   (exactly 2 fields).

This is the ``text_classification`` layout with a tab separating the two
sentences of the pair. A file that isn't exactly 2 non-empty tab fields (e.g.
plain prose with no tab, an empty side, or several records crammed into one
file) is malformed: the training client would fail to split it into the two
sentences the model encodes as a pair. So we reject it here, against the dataset
author, rather than letting it corrupt training.

The manifest walk, path resolution and record-level checks live in the shared
:class:`~tracebloc_ingestor.validators.tab_separated_record_validator.TabSeparatedRecordValidator`
base (also used by ``embeddings``); this subclass only pins the exactly-a-pair
field contract and its phrasing. UTF-8 / binary hygiene stays the shared
``TextContentValidator``'s job; emptiness it already warns about, so an empty /
whitespace-only file is left untouched here (no double reporting).
"""

from typing import Tuple

from ..utils.constants import FileExtension
from .tab_separated_record_validator import TabSeparatedRecordValidator


class SentencePairValidator(TabSeparatedRecordValidator):
    """Validate that each referenced ``sentence_pair_classification`` ``.txt`` is
    a tab-separated sentence pair (``text_a<TAB>text_b``)."""

    ALLOWED_FIELD_COUNTS: Tuple[int, ...] = (2,)
    _ERROR_NOUN = "sentence pair"

    def __init__(
        self,
        texts_path: str = "texts",
        extension: str = FileExtension.TXT,
        filename_column: str = "filename",
        name: str = "Sentence Pair",
    ):
        super().__init__(
            texts_path=texts_path,
            extension=extension,
            filename_column=filename_column,
            name=name,
        )

    def _expected_fields_phrase(self) -> str:
        return "exactly 2 (text_a<TAB>text_b)"

    def _multiline_hint(self) -> str:
        return "Put one 'text_a<TAB>text_b' sentence pair per .txt."

    def _field_names(self, count: int) -> str:
        return "text_a, text_b"
