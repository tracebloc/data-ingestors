"""Tests for TokenizerValidator — checks tokenizer.json at config.SRC_PATH."""

from __future__ import annotations

import pytest

from tracebloc_ingestor.validators.tokenizer_validator import TokenizerValidator


@pytest.fixture
def validator():
    return TokenizerValidator()


def test_passes_when_required_tokens_present(clean_env, validator, make_tokenizer):
    src = make_tokenizer(vocab=["a", "b", "[MASK]", "[PAD]"])
    clean_env.setenv("SRC_PATH", str(src))
    result = validator.validate(None)
    assert result.is_valid
    assert result.metadata["vocab_size"] == 4


def test_missing_file_fails(clean_env, validator, tmp_path):
    clean_env.setenv("SRC_PATH", str(tmp_path))
    result = validator.validate(None)
    assert not result.is_valid
    assert "tokenizer.json not found" in result.errors[0]


def test_missing_required_token_fails(clean_env, validator, make_tokenizer):
    src = make_tokenizer(vocab=["a", "b", "[PAD]"])  # no [MASK]
    clean_env.setenv("SRC_PATH", str(src))
    result = validator.validate(None)
    assert not result.is_valid
    assert "[MASK]" in result.errors[0]
    assert result.metadata["missing_tokens"] == ["[MASK]"]


def test_tokens_from_added_tokens_section(clean_env, validator, make_tokenizer):
    # vocab lacks the special tokens; added_tokens supplies them.
    src = make_tokenizer(vocab=["a", "b"], added=["[MASK]", "[PAD]"])
    clean_env.setenv("SRC_PATH", str(src))
    result = validator.validate(None)
    assert result.is_valid


def test_unrecognized_structure_fails(clean_env, validator, make_tokenizer):
    src = make_tokenizer(vocab=None)  # empty model, no added_tokens
    clean_env.setenv("SRC_PATH", str(src))
    result = validator.validate(None)
    assert not result.is_valid
    assert "Could not extract vocabulary" in result.errors[0]


def test_invalid_json_fails(clean_env, validator, make_tokenizer):
    src = make_tokenizer(raw="{not valid json")
    clean_env.setenv("SRC_PATH", str(src))
    result = validator.validate(None)
    assert not result.is_valid
    assert "not valid JSON" in result.errors[0]


def test_extract_vocab_unigram_list_form():
    data = {"model": {"vocab": [["[MASK]", -1.0], ["[PAD]", -2.0]]}}
    vocab = TokenizerValidator._extract_vocab(data)
    assert vocab == {"[MASK]", "[PAD]"}


def test_extract_vocab_added_tokens_string_form():
    data = {"added_tokens": ["[MASK]", "[PAD]"]}
    vocab = TokenizerValidator._extract_vocab(data)
    assert vocab == {"[MASK]", "[PAD]"}


def test_extract_vocab_returns_none_when_empty():
    assert TokenizerValidator._extract_vocab({}) is None


def test_custom_required_tokens():
    v = TokenizerValidator(required_tokens=("[CLS]",))
    assert v.required_tokens == {"[CLS]"}
