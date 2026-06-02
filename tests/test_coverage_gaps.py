"""Targeted tests closing the long tail of exception / edge branches.

These exist to exercise defensive error paths and small helpers that the
behavioural tests don't reach, pushing module coverage toward 100%.
"""

from __future__ import annotations

import importlib
import logging
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
from requests.packages.urllib3.util.retry import Retry

from tracebloc_ingestor.utils.constants import DataFormat, TaskCategory
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.config import Config


# ===========================================================================
# utils: constants, logging
# ===========================================================================

def test_dataformat_helpers():
    formats = DataFormat.get_all_formats()
    assert DataFormat.IMAGE in formats
    assert DataFormat.is_valid_format(DataFormat.TABULAR) is True
    assert DataFormat.is_valid_format("nonsense") is False


def test_setup_logging_without_config():
    setup_logging(None)  # exercises the default-level else branch
    assert logging.getLogger().handlers


def test_config_log_level_string_override():
    assert Config(LOG_LEVEL="DEBUG").LOG_LEVEL == logging.DEBUG


# ===========================================================================
# cli: conventions, run
# ===========================================================================

def test_data_format_for_unknown_raises():
    from tracebloc_ingestor.cli.conventions import _data_format_for
    with pytest.raises(ValueError):
        _data_format_for("not_a_category")


def test_build_ingestor_unknown_source_type_raises():
    from tracebloc_ingestor.cli import run as run_mod
    from tracebloc_ingestor.cli.conventions import ResolvedConfig
    resolved = ResolvedConfig(
        category=None, table_name="t", intent="train",
        data_format="image", source_type="parquet", source_path="/x",
    )
    with pytest.raises(ValueError, match="Unknown source_type"):
        run_mod._build_ingestor(MagicMock(), MagicMock(), resolved)


# ===========================================================================
# validators/base: BaseValidator helpers
# ===========================================================================

from tracebloc_ingestor.validators.base import BaseValidator, ValidationResult


class _Concrete(BaseValidator):
    def validate(self, data, **kwargs):
        return self._create_result(True)


def test_base_validator_load_data_dataframe_passthrough():
    v = _Concrete("x")
    df = pd.DataFrame({"a": [1]})
    assert v._load_data(df) is df


def test_base_validator_load_data_reads_csv(make_csv):
    v = _Concrete("x")
    path = make_csv({"a": [1, 2]})
    loaded = v._load_data(str(path))
    assert len(loaded) == 2


def test_base_validator_load_data_unsupported_returns_none():
    assert _Concrete("x")._load_data(12345) is None


def test_base_validator_load_data_bad_path_returns_none():
    assert _Concrete("x")._load_data("/no/such/file.csv") is None


def test_base_validator_parse_json_valid():
    v = _Concrete("x")
    assert v._parse_json({"c": '{"a": 1}'}, "c") == {"a": 1}


def test_base_validator_parse_json_nan_returns_none():
    v = _Concrete("x")
    assert v._parse_json({"c": float("nan")}, "c") is None


def test_base_validator_parse_json_bad_returns_none():
    v = _Concrete("x")
    assert v._parse_json({"c": "{bad"}, "c") is None


def test_base_validator_str_and_repr():
    v = _Concrete("My Validator")
    assert "My Validator" in str(v)
    assert "my_validator_validator" in repr(v)


# ===========================================================================
# validators_mapping: schema branches
# ===========================================================================

def test_text_classification_with_schema_adds_data_validator():
    from tracebloc_ingestor.utils.validators_mapping import map_validators
    from tracebloc_ingestor.validators.data_validator import DataValidator
    v = map_validators(TaskCategory.TEXT_CLASSIFICATION, {"schema": {"a": "INT"}})
    assert any(isinstance(x, DataValidator) for x in v)


def test_mlm_with_schema_adds_data_validator():
    from tracebloc_ingestor.utils.validators_mapping import map_validators
    from tracebloc_ingestor.validators.data_validator import DataValidator
    v = map_validators(TaskCategory.MASKED_LANGUAGE_MODELING, {"schema": {"a": "INT"}})
    assert any(isinstance(x, DataValidator) for x in v)


# ===========================================================================
# data_validator: edge branches
# ===========================================================================

from tracebloc_ingestor.validators.data_validator import DataValidator


def test_data_validator_load_non_csv_returns_none():
    assert DataValidator(schema={"a": "INT"})._load_data("/x.parquet", 100) is None


def test_data_validator_load_bad_path_returns_none():
    # .csv path that doesn't exist -> read raises -> None
    assert DataValidator(schema={"a": "INT"})._load_data("/no/such.csv", 100) is None


def test_data_validator_validate_exception_path():
    v = DataValidator(schema={"a": "INT"})
    # _load_data raising bubbles into the outer except.
    with patch.object(v, "_load_data", side_effect=RuntimeError("boom")):
        result = v.validate(pd.DataFrame({"a": [1]}))
    assert not result.is_valid
    assert "validation error" in result.errors[0].lower()


def test_data_validator_boolean_other_dtype():
    # datetime dtype hits the catch-all boolean branch.
    df = pd.DataFrame({"b": pd.to_datetime(["2024-01-01", "2024-01-02"])})
    result = DataValidator(schema={"b": "BOOLEAN"}).validate(df)
    assert not result.is_valid


def test_data_validator_date_exception(monkeypatch):
    v = DataValidator(schema={"d": "DATE"})
    with patch("tracebloc_ingestor.validators.data_validator.pd.to_datetime",
               side_effect=RuntimeError("bad")):
        res = v._validate_date(pd.Series(["2024-01-01"]), "d", "DATE")
    assert not res["is_valid"]


# ===========================================================================
# duplicate_validator: exception fallbacks
# ===========================================================================

from tracebloc_ingestor.validators.duplicate_validator import DuplicateValidator


def test_duplicate_validate_exception(monkeypatch):
    v = DuplicateValidator(dest_path="/x")
    with patch.object(v, "_check_directory_exists", side_effect=RuntimeError("boom")):
        res = v.validate(None)
    assert not res.is_valid
    assert "Duplicate validation error" in res.errors[0]


def test_duplicate_check_directory_exists_exception():
    v = DuplicateValidator(dest_path="/x")
    with patch("tracebloc_ingestor.validators.duplicate_validator.Path",
               side_effect=RuntimeError("boom")):
        assert v._check_directory_exists() is False


def test_duplicate_create_directory_exception():
    v = DuplicateValidator(dest_path="/x")
    with patch("tracebloc_ingestor.validators.duplicate_validator.Path",
               side_effect=RuntimeError("boom")):
        assert v._create_directory_if_needed() is False


# ===========================================================================
# table_name / tokenizer / keypoint / numeric: outer except
# ===========================================================================

def test_table_name_validate_exception():
    from tracebloc_ingestor.validators.table_name_validator import TableNameValidator
    v = TableNameValidator()
    with patch.object(v, "_validate_table_names", side_effect=RuntimeError("boom")):
        res = v.validate(None)
    assert not res.is_valid


def test_tokenizer_validate_generic_exception(clean_env, make_tokenizer):
    from tracebloc_ingestor.validators.tokenizer_validator import TokenizerValidator
    src = make_tokenizer(vocab=["[MASK]", "[PAD]"])
    clean_env.setenv("SRC_PATH", str(src))
    v = TokenizerValidator()
    with patch.object(v, "_extract_vocab", side_effect=RuntimeError("boom")):
        res = v.validate(None)
    assert not res.is_valid


def test_keypoint_annotation_validate_exception():
    from tracebloc_ingestor.validators.keypoint_annotation_validator import (
        KeypointAnnotationValidator,
    )
    v = KeypointAnnotationValidator()
    with patch.object(v, "_load_data", side_effect=RuntimeError("boom")):
        res = v.validate("x")
    assert not res.is_valid


def test_keypoint_visibility_validate_exception():
    from tracebloc_ingestor.validators.keypoint_visibility_validator import (
        KeypointVisibilityValidator,
    )
    v = KeypointVisibilityValidator()
    with patch.object(v, "_load_data", side_effect=RuntimeError("boom")):
        res = v.validate("x")
    assert not res.is_valid


def test_numeric_columns_validate_exception():
    from tracebloc_ingestor.validators.numeric_columns_validator import (
        NumericColumnsValidator,
    )
    v = NumericColumnsValidator(schema={"a": "INT"})
    with patch.object(v, "_load_data", side_effect=RuntimeError("boom")):
        res = v.validate("x")
    assert not res.is_valid


def test_numeric_columns_load_bad_path_returns_none():
    from tracebloc_ingestor.validators.numeric_columns_validator import (
        NumericColumnsValidator,
    )
    assert NumericColumnsValidator()._load_data("/no/such.csv") is None


# ===========================================================================
# time validators: outer except + _load_data None paths
# ===========================================================================

@pytest.mark.parametrize("modname,clsname", [
    ("time_format_validator", "TimeFormatValidator"),
    ("time_ordered_validator", "TimeOrderedValidator"),
    ("time_before_today_validator", "TimeBeforeTodayValidator"),
])
def test_time_validators_generic_exception(make_csv, modname, clsname):
    mod = importlib.import_module(f"tracebloc_ingestor.validators.{modname}")
    cls = getattr(mod, clsname)
    v = cls()
    path = make_csv({"timestamp": ["2024-01-01"]})
    with patch.object(v, "_load_data", side_effect=RuntimeError("boom")):
        res = v.validate(str(path))
    assert not res.is_valid


@pytest.mark.parametrize("modname,clsname", [
    ("time_format_validator", "TimeFormatValidator"),
    ("time_ordered_validator", "TimeOrderedValidator"),
    ("time_before_today_validator", "TimeBeforeTodayValidator"),
])
def test_time_validators_load_non_csv_none(modname, clsname):
    mod = importlib.import_module(f"tracebloc_ingestor.validators.{modname}")
    v = getattr(mod, clsname)()
    assert v._load_data("/x.parquet") is None


def test_time_before_today_empty_after_dropna(make_csv):
    from tracebloc_ingestor.validators.time_before_today_validator import (
        TimeBeforeTodayValidator,
    )
    # all-invalid timestamps -> valid set empty -> skips future check, stays valid
    path = make_csv({"timestamp": ["not-a-date", "also-bad"]})
    res = TimeBeforeTodayValidator().validate(str(path))
    assert res.is_valid


def test_time_to_event_load_unsupported_type():
    from tracebloc_ingestor.validators.time_to_event_validator import TimeToEventValidator
    assert TimeToEventValidator()._load_data(12345, None) is None


def test_time_to_event_load_non_csv():
    from tracebloc_ingestor.validators.time_to_event_validator import TimeToEventValidator
    assert TimeToEventValidator()._load_data("/x.parquet", None) is None


def test_time_to_event_generic_exception():
    from tracebloc_ingestor.validators.time_to_event_validator import TimeToEventValidator
    v = TimeToEventValidator()
    with patch.object(v, "_load_data", side_effect=RuntimeError("boom")):
        res = v.validate(pd.DataFrame({"time": [1]}))
    assert not res.is_valid


# ===========================================================================
# image_validator: PIL-not-available + small branches
# ===========================================================================

def test_image_validator_pil_unavailable(monkeypatch, clean_env):
    from tracebloc_ingestor.validators import image_validator as iv
    monkeypatch.setattr(iv, "PIL_AVAILABLE", False)
    clean_env.setenv("SRC_PATH", "/tmp")
    res = iv.ImageResolutionValidator().validate(None)
    assert not res.is_valid
    assert any("PIL" in e for e in res.errors)


def test_image_validator_list_with_non_image(tmp_path):
    from tracebloc_ingestor.validators.image_validator import ImageResolutionValidator
    txt = tmp_path / "a.txt"
    txt.write_text("x")
    v = ImageResolutionValidator()
    # a list with a non-image file -> warning branch, returns empty list
    assert v._get_image_files([str(txt)], True, True) == []


def test_image_validator_list_with_non_path_item():
    from tracebloc_ingestor.validators.image_validator import ImageResolutionValidator
    v = ImageResolutionValidator()
    assert v._get_image_files([123], True, True) == []


# ===========================================================================
# api/client: error-response branches + LoggingRetry
# ===========================================================================

from tracebloc_ingestor.api.client import APIClient, LoggingRetry


def _client(**ov):
    d = dict(BACKEND_TOKEN="tok", CLIENT_USERNAME=None, CLIENT_PASSWORD=None, EDGE_ENV="prod")
    d.update(ov)
    return APIClient(Config(**d))


def _err_with_response():
    err = requests.exceptions.HTTPError("boom")
    err.response = MagicMock(text="server detail")
    return err


def test_logging_retry_increment():
    with patch.object(Retry, "increment", return_value=MagicMock(total=1)):
        r = LoggingRetry(total=3)
        r.increment(method="GET", url="http://x")


def test_authenticate_error_with_response_text():
    cfg = Config(BACKEND_TOKEN=None, CLIENT_USERNAME="u", CLIENT_PASSWORD="p", EDGE_ENV="prod")
    with patch("requests.Session.post", side_effect=_err_with_response()):
        with pytest.raises(ValueError, match="Authentication failed"):
            APIClient(cfg)


def test_send_batch_error_with_response_text():
    client = _client()
    with patch.object(client.session, "post", side_effect=_err_with_response()):
        assert client.send_batch([(1, {"data_id": "a"})], "tbl", "ing") is False


def test_send_global_meta_error_with_response_text():
    client = _client()
    with patch.object(client.session, "post", side_effect=_err_with_response()):
        assert client.send_global_meta_meta("tbl", {}, {}) is False


def test_generate_edge_label_error_with_response_text():
    client = _client()
    with patch.object(client.session, "get", side_effect=_err_with_response()):
        assert client.send_generate_edge_label_meta("tbl", "ing", "train") is False


def test_prepare_dataset_error_with_response_text():
    client = _client()
    with patch.object(client.session, "get", side_effect=_err_with_response()):
        assert client.prepare_dataset(
            TaskCategory.IMAGE_CLASSIFICATION, "ing", "image", "train"
        ) is False


def test_create_dataset_error_with_response_text():
    client = _client()
    with patch.object(client.session, "post", side_effect=_err_with_response()):
        with pytest.raises(requests.exceptions.RequestException):
            client.create_dataset(ingestor_id="ing", category=TaskCategory.IMAGE_CLASSIFICATION)


# ===========================================================================
# file_transfer: copy-failure except branches
# ===========================================================================

from tracebloc_ingestor import file_transfer


@pytest.fixture
def ft_dirs(tmp_path, monkeypatch):
    src = tmp_path / "src"
    storage = tmp_path / "storage"
    src.mkdir()
    storage.mkdir()
    monkeypatch.setenv("SRC_PATH", str(src))
    monkeypatch.setenv("TABLE_NAME", "tbl")
    monkeypatch.setattr(file_transfer.config, "STORAGE_PATH", str(storage))
    return src, storage / "tbl"


def _seed(src, sub, name):
    d = src / sub
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_bytes(b"x")
    return d / name


def test_image_transfer_copy_failure_raises(ft_dirs):
    src, _ = ft_dirs
    _seed(src, "images", "a.jpg")
    with patch.object(file_transfer, "_copy_file_with_retry", side_effect=OSError("disk")):
        with pytest.raises(ValueError):
            file_transfer.image_transfer({"filename": "a"}, {"extension": ".jpg"})


def test_annotation_transfer_copy_failure_raises(ft_dirs):
    src, _ = ft_dirs
    s = _seed(src, "annotations", "a.xml")
    with patch.object(file_transfer, "_copy_file_with_retry", side_effect=OSError("disk")):
        with pytest.raises(ValueError):
            file_transfer.annotation_transfer({"filename": "a"}, {}, ".xml", str(s), "a.xml")


def test_text_transfer_copy_failure_raises(ft_dirs):
    src, _ = ft_dirs
    _seed(src, "texts", "a.txt")
    with patch.object(file_transfer, "_copy_file_with_retry", side_effect=OSError("disk")):
        with pytest.raises(ValueError):
            file_transfer.text_transfer({"filename": "a"}, {"extension": ".txt"})


def test_mask_transfer_copy_failure_raises(ft_dirs):
    src, _ = ft_dirs
    s = _seed(src, "masks", "m.png")
    with patch.object(file_transfer, "_copy_file_with_retry", side_effect=OSError("disk")):
        with pytest.raises(ValueError):
            file_transfer.mask_transfer({"filename": "x"}, str(s), ".png", "m")


def test_annotation_transfer_missing_filename_returns_none(ft_dirs):
    assert file_transfer.annotation_transfer({}, {}, ".xml") is None


def test_image_transfer_missing_filename_returns_none(ft_dirs):
    assert file_transfer.image_transfer({}, {"extension": ".jpg"}) is None


def test_map_object_detection_missing_image_returns_none(ft_dirs):
    # no image seeded -> image-not-found branch in map_file_transfer
    assert file_transfer.map_file_transfer(
        TaskCategory.OBJECT_DETECTION, {"filename": "ghost"}, {"extension": ".jpg"}
    ) is None


def test_map_semantic_segmentation_missing_image_returns_none(ft_dirs):
    assert file_transfer.map_file_transfer(
        TaskCategory.SEMANTIC_SEGMENTATION,
        {"filename": "ghost", "mask_id": "m"}, {"extension": ".jpg"},
    ) is None


def test_map_semantic_segmentation_missing_filename_returns_none(ft_dirs):
    assert file_transfer.map_file_transfer(
        TaskCategory.SEMANTIC_SEGMENTATION, {}, {"extension": ".jpg"}
    ) is None
