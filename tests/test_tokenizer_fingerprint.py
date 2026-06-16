"""Tests for the NLP tokenizer fingerprint extracted + registered at ingest.

Covers issue #805 Task 2: the 4 structural integers (vocab_size /
mask_token_id / pad_token_id / tokenizer_type) extracted from a shipped
``tokenizer.json`` and shipped on the global-metadata channel. The FL
guardrail is that ONLY these integers leave the cluster — never vocabulary
content and never a hash.
"""

from __future__ import annotations

import json

import pytest

from tracebloc_ingestor import file_transfer
from tracebloc_ingestor.validators.tokenizer_validator import (
    _special_token_id,
    extract_tokenizer_metadata,
    load_tokenizer_metadata,
)

# A BERT/WordPiece-style tokenizer.json with the full classification +
# MLM special-token set, special tokens declared in added_tokens.
_BERT_STYLE = {
    "version": "1.0",
    "model": {
        "type": "WordPiece",
        "vocab": {
            "[PAD]": 0,
            "[UNK]": 1,
            "[CLS]": 2,
            "[SEP]": 3,
            "[MASK]": 4,
            "hello": 5,
            "world": 6,
        },
    },
    "added_tokens": [
        {"id": 0, "content": "[PAD]", "special": True},
        {"id": 1, "content": "[UNK]", "special": True},
        {"id": 2, "content": "[CLS]", "special": True},
        {"id": 3, "content": "[SEP]", "special": True},
        {"id": 4, "content": "[MASK]", "special": True},
    ],
}

# A classification tokenizer that has [PAD] but no [MASK] (no masking task).
_NO_MASK = {
    "model": {"type": "WordPiece", "vocab": {"[PAD]": 0, "[UNK]": 1, "a": 2}},
    "added_tokens": [{"id": 0, "content": "[PAD]", "special": True}],
}

# A Unigram tokenizer stores its vocab as a [token, score] list.
_UNIGRAM = {
    "model": {
        "type": "Unigram",
        "vocab": [["[PAD]", 0.0], ["[MASK]", 0.0], ["x", -1.0], ["y", -2.0]],
    },
    "added_tokens": [
        {"id": 0, "content": "[PAD]", "special": True},
        {"id": 1, "content": "[MASK]", "special": True},
    ],
}


# ---------------------------------------------------------------------------
# extract_tokenizer_metadata
# ---------------------------------------------------------------------------


def test_extract_full_fingerprint_bert_style():
    meta = extract_tokenizer_metadata(_BERT_STYLE)
    assert meta == {
        "vocab_size": 7,
        "mask_token_id": 4,
        "pad_token_id": 0,
        "tokenizer_type": "WordPiece",
    }


def test_extract_classification_without_mask_yields_none_mask_id():
    meta = extract_tokenizer_metadata(_NO_MASK)
    assert meta["mask_token_id"] is None
    assert meta["pad_token_id"] == 0
    assert meta["vocab_size"] == 3
    assert meta["tokenizer_type"] == "WordPiece"


def test_extract_unigram_list_vocab():
    meta = extract_tokenizer_metadata(_UNIGRAM)
    assert meta["vocab_size"] == 4
    assert meta["mask_token_id"] == 1
    assert meta["pad_token_id"] == 0
    assert meta["tokenizer_type"] == "Unigram"


def test_fl_guardrail_only_four_scalar_keys():
    """No vocabulary content or hash may cross to the backend — only the 4
    scalar integers (one may be a type string / None)."""
    meta = extract_tokenizer_metadata(_BERT_STYLE)
    assert set(meta) == {
        "vocab_size",
        "mask_token_id",
        "pad_token_id",
        "tokenizer_type",
    }
    for value in meta.values():
        assert not isinstance(value, (dict, list))


def test_extract_handles_empty_or_unknown_structure():
    meta = extract_tokenizer_metadata({})
    assert meta == {
        "vocab_size": 0,
        "mask_token_id": None,
        "pad_token_id": None,
        "tokenizer_type": None,
    }


# ---------------------------------------------------------------------------
# _special_token_id
# ---------------------------------------------------------------------------


def test_special_token_id_prefers_added_tokens():
    data = {
        "model": {"vocab": {"[PAD]": 99}},
        "added_tokens": [{"id": 7, "content": "[PAD]"}],
    }
    assert _special_token_id(data, "[PAD]") == 7


def test_special_token_id_falls_back_to_vocab():
    data = {"model": {"vocab": {"[PAD]": 3}}, "added_tokens": []}
    assert _special_token_id(data, "[PAD]") == 3


def test_special_token_id_idless_added_token_falls_back_to_vocab():
    """A malformed added_tokens entry with no ``id`` must not shadow the
    model.vocab mapping that does hold the id (bugbot)."""
    data = {
        "model": {"vocab": {"[PAD]": 5}},
        "added_tokens": [{"content": "[PAD]"}],  # no "id"
    }
    assert _special_token_id(data, "[PAD]") == 5


def test_special_token_id_absent_returns_none():
    assert _special_token_id(_NO_MASK, "[MASK]") is None


# ---------------------------------------------------------------------------
# load_tokenizer_metadata
# ---------------------------------------------------------------------------


def test_load_reads_file(tmp_path):
    p = tmp_path / "tokenizer.json"
    p.write_text(json.dumps(_BERT_STYLE))
    meta = load_tokenizer_metadata(str(p))
    assert meta["vocab_size"] == 7
    assert meta["mask_token_id"] == 4


def test_load_missing_file_returns_none(tmp_path):
    assert load_tokenizer_metadata(str(tmp_path / "nope.json")) is None


def test_load_malformed_json_returns_none(tmp_path):
    p = tmp_path / "tokenizer.json"
    p.write_text("{ not valid json ")
    assert load_tokenizer_metadata(str(p)) is None


# ---------------------------------------------------------------------------
# file_transfer helpers (SRC_PATH / DEST_PATH backed)
# ---------------------------------------------------------------------------


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    """Point file_transfer's Config at tmp src + storage dirs (mirrors the
    fixture in test_file_transfer_transfers.py)."""
    src = tmp_path / "src"
    storage = tmp_path / "storage"
    src.mkdir()
    storage.mkdir()
    monkeypatch.setenv("SRC_PATH", str(src))
    monkeypatch.setenv("TABLE_NAME", "tbl")
    monkeypatch.setattr(file_transfer.config, "STORAGE_PATH", str(storage))
    return src, storage / "tbl"


def test_get_shipped_tokenizer_metadata_reads_src(dirs):
    src, _ = dirs
    (src / "tokenizer.json").write_text(json.dumps(_BERT_STYLE))
    meta = file_transfer.get_shipped_tokenizer_metadata()
    assert meta["vocab_size"] == 7
    assert meta["pad_token_id"] == 0


def test_get_shipped_tokenizer_metadata_none_when_absent(dirs):
    assert file_transfer.get_shipped_tokenizer_metadata() is None


def test_copy_tokenizer_returns_fingerprint_on_copy(dirs):
    src, dest = dirs
    # In production text_transfer creates DEST_PATH before the tokenizer copy;
    # mirror that here since we call the helper in isolation.
    dest.mkdir(parents=True, exist_ok=True)
    (src / "tokenizer.json").write_text(json.dumps(_BERT_STYLE))
    meta = file_transfer._copy_tokenizer_if_present()
    assert (dest / "tokenizer.json").exists()
    assert meta["vocab_size"] == 7
    # Already copied: a second call is a no-op and returns None.
    assert file_transfer._copy_tokenizer_if_present() is None


def test_copy_tokenizer_none_when_absent(dirs):
    assert file_transfer._copy_tokenizer_if_present() is None
