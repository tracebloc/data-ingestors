"""Tests for TableNameValidator — regex naming rules read from config.TABLE_NAME."""

from __future__ import annotations

import pytest

from tracebloc_ingestor.validators.table_name_validator import TableNameValidator


@pytest.fixture
def validator():
    return TableNameValidator()


def test_valid_name_passes(clean_env, validator):
    clean_env.setenv("TABLE_NAME", "cats_dogs_train")
    result = validator.validate(None)
    assert result.is_valid
    assert result.errors == []
    assert result.metadata["table_names_checked"] == 1


def test_missing_table_name_fails(clean_env, validator):
    # TABLE_NAME unset -> config returns "" -> the no-name branch.
    result = validator.validate(None)
    assert not result.is_valid
    assert "No table name found" in result.errors[0]
    assert result.metadata["table_names_checked"] == 0


@pytest.mark.parametrize(
    "bad_name",
    [
        "1table",         # starts with a digit
        "my-table",       # hyphen
        "my table",       # space
        "table$",         # symbol
        "_leading",       # underscore start (pattern requires a letter)
    ],
)
def test_invalid_characters_fail(clean_env, validator, bad_name):
    clean_env.setenv("TABLE_NAME", bad_name)
    result = validator.validate(None)
    assert not result.is_valid
    assert any("invalid characters" in e for e in result.errors)
    assert result.metadata["invalid_names"]


def test_reserved_keyword_warns_but_passes(clean_env, validator):
    clean_env.setenv("TABLE_NAME", "select")
    result = validator.validate(None)
    assert result.is_valid
    assert any("reserved keyword" in w for w in result.warnings)


def test_validate_table_names_empty_list(validator):
    result = validator._validate_table_names([])
    assert not result.is_valid
    assert "No table names to validate" in result.errors[0]


def test_validate_table_names_non_string(validator):
    result = validator._validate_table_names([123])
    assert not result.is_valid
    assert any("not a string" in e for e in result.errors)


def test_validate_table_names_whitespace_only(validator):
    result = validator._validate_table_names(["   "])
    assert not result.is_valid
    assert any("empty table name" in e for e in result.errors)


def test_is_reserved_keyword_case_insensitive(validator):
    assert validator._is_reserved_keyword("SELECT")
    assert validator._is_reserved_keyword("from")
    assert not validator._is_reserved_keyword("customers")
