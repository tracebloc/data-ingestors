"""#226 Phase 2: error messages and logs never carry raw cell content.

Validator/cast errors land in the install log and can egress via failure
reports, so each site surfaces row references (and, for date-format
problems, structure-preserving masks) instead of values. These tests plant
a distinctive sensitive value at every sanitized site and assert it cannot
appear in the produced message.
"""

import logging

import pandas as pd
import pytest

from tracebloc_ingestor.utils import redaction

SECRET = "patient-4711-Müller"


# ── the helpers themselves ───────────────────────────────────────────────────


def test_row_refs_formats_and_caps():
    assert redaction.row_refs([2, 17, 108], total=3) == "rows [2, 17, 108]"
    assert (
        redaction.row_refs([0, 1, 2, 3, 4], total=9) == "rows [0, 1, 2, 3, 4] (+4 more)"
    )


def test_mask_shape_keeps_structure_never_content():
    assert redaction.mask_shape("03/04/2026 10:15") == "##/##/#### ##:##"
    assert redaction.mask_shape("abc-123") == "xxx-###"
    masked = redaction.mask_shape(SECRET)
    assert "4711" not in masked and "Müller" not in masked
    long = redaction.mask_shape("x" * 100)
    assert len(long) <= 25 and long.endswith("…")


# ── DataValidator (numeric gate) ─────────────────────────────────────────────


def test_data_validator_error_never_contains_cell_content():
    from tracebloc_ingestor.validators.data_validator import DataValidator

    df = pd.DataFrame({"x": [1.0, SECRET, 3.0]})
    result = DataValidator(schema={"x": "FLOAT"}).validate(df)
    assert not result.is_valid
    joined = " ".join(result.errors)
    assert SECRET not in joined
    assert "rows [1]" in joined


# ── CSV cast layer ───────────────────────────────────────────────────────────


def test_csv_float_overflow_error_redacts():
    from tracebloc_ingestor.ingestors.csv_ingestor import _raise_on_overflow

    original = pd.Series(["8e777", "2.5"])
    converted = pd.to_numeric(original, errors="coerce")
    with pytest.raises(ValueError) as exc:
        _raise_on_overflow("x", original, converted, "FLOAT")
    assert "8e777" not in str(exc.value)
    assert "rows [0]" in str(exc.value)


def test_csv_cast_date_error_masks_shape():
    from tracebloc_ingestor.ingestors.csv_ingestor import _cast_datetime_strict

    with pytest.raises(ValueError) as exc:
        _cast_datetime_strict(pd.Series(["not-a-date-4711"]), "d", "DATE")
    msg = str(exc.value)
    assert "not-a-date-4711" not in msg
    assert "masked shapes" in msg and "rows [0]" in msg


# ── int64 overflow (coercion) ────────────────────────────────────────────────


def test_int64_overflow_error_redacts():
    from tracebloc_ingestor.utils import coercion

    huge = "99999999999999999999"  # > int64, could be a real identifier
    err = coercion.int_range_error(pd.Series([huge, "5"]), "acct", "INT")
    assert err is not None
    assert huge not in err
    assert "rows [0]" in err


# ── RecordProcessor logs ─────────────────────────────────────────────────────


def _rp(unique_id_column=None):
    from tracebloc_ingestor.ingestors.record_processor import RecordProcessor

    return RecordProcessor(
        schema={"x": "FLOAT"},
        intent="train",
        label_column="y",
        annotation_column=None,
        unique_id_column=unique_id_column,
        ingestor_id="run-1",
        # #350: content_hash is now the default strategy and needs a salt.
        table_salt="0" * 64,
    )


def test_missing_unique_id_warning_never_logs_the_record(caplog):
    rp = _rp(unique_id_column="uid")
    with caplog.at_level(logging.WARNING):
        out = rp.process({"x": 1.0, "y": "a", "uid": "", "note": SECRET})
    assert out is None
    assert SECRET not in caplog.text
    assert "uid" in caplog.text  # the column name IS the useful part


def test_hot_loop_does_not_log_record_content(caplog):
    rp = _rp()
    with caplog.at_level(logging.DEBUG):
        rp.process({"x": 1.0, "y": SECRET})
    assert SECRET not in caplog.text


def test_process_catch_all_never_logs_the_exception_message(caplog, monkeypatch):
    """The catch-all in ``RecordProcessor.process`` is a content leak by default.

    Everything raised inside that ``try`` has the record's own cells in scope: a
    driver rejecting a value quotes it, and a plain ``ValueError`` from casting a
    cell embeds it. So the handler must surface the exception CLASS, never its
    message (backend#1879).

    Pinned because the fix is a one-line substitution that reads like noise —
    ``str(e)`` back in place would restore the leak silently, and the engine's
    identical hole survived for exactly that reason: the concept existed, the
    coverage did not.
    """
    rp = _rp()
    monkeypatch.setattr(
        rp,
        "_map_unique_id",
        lambda *a, **k: (_ for _ in ()).throw(
            ValueError(f"bad value {SECRET!r} in column x")
        ),
    )
    with caplog.at_level(logging.ERROR):
        out = rp.process({"x": 1.0, "y": "a"})
    assert out is None
    assert SECRET not in caplog.text  # the whole point
    assert "ValueError" in caplog.text  # the class is the actionable part
    assert "Error processing record" in caplog.text


def test_safe_db_error_names_classes_never_messages():
    from sqlalchemy.exc import OperationalError

    from tracebloc_ingestor.utils.redaction import safe_db_error

    err = OperationalError("INSERT …", {}, Exception(f"Duplicate entry '{SECRET}'"))
    safe = safe_db_error(err)
    assert "OperationalError" in safe
    assert SECRET not in safe

    class FakeDriverError(Exception):
        errno = 1406

    wrapped = OperationalError("INSERT …", {}, FakeDriverError(SECRET))
    safe = safe_db_error(wrapped)
    assert "errno=1406" in safe and "data too long" in safe
    assert SECRET not in safe


def test_date_cast_severs_the_exception_chain():
    """A regression restoring `from exc` would resurrect pandas' raw-value
    message in logged tracebacks — pin the severed chain."""
    from tracebloc_ingestor.ingestors.csv_ingestor import _cast_datetime_strict

    with pytest.raises(ValueError) as exc:
        _cast_datetime_strict(pd.Series([SECRET]), "d", "DATE")
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__


def test_json_dtype_error_masks_the_value():
    from tracebloc_ingestor.ingestors.json_ingestor import (
        _validate_value_against_dtype,
    )

    with pytest.raises(ValueError) as exc:
        _validate_value_against_dtype(SECRET, "FLOAT")
    assert SECRET not in str(exc.value)


def test_json_non_dict_record_reports_position_not_content():
    from tracebloc_ingestor.ingestors.json_ingestor import JSONIngestor

    gen = (
        JSONIngestor._iter_validated_records.__wrapped__
        if hasattr(JSONIngestor._iter_validated_records, "__wrapped__")
        else JSONIngestor._iter_validated_records
    )
    ing = object.__new__(JSONIngestor)
    ing.schema = {}
    with pytest.raises(ValueError) as exc:
        list(gen(ing, iter([SECRET])))
    msg = str(exc.value)
    assert SECRET not in msg
    assert "position 0" in msg and "str" in msg


def test_boolean_validator_reports_rows_not_values(caplog):
    from tracebloc_ingestor.validators.data_validator import DataValidator

    df = pd.DataFrame({"flag": ["yes", SECRET, "no"]})
    result = DataValidator(schema={"flag": "BOOLEAN"}).validate(df)
    assert not result.is_valid
    joined = " ".join(result.errors)
    assert SECRET not in joined
    assert "rows [1]" in joined


def test_time_to_event_validator_redacts(tmp_path):
    from tracebloc_ingestor.validators.time_to_event_validator import (
        TimeToEventValidator,
    )

    df = pd.DataFrame({"time": [1.0, SECRET], "event": [1, 0], "label": [1, 0]})
    v = TimeToEventValidator(time_column="time")
    result = v.validate(df)
    assert not result.is_valid
    joined = " ".join(result.errors)
    assert SECRET not in joined
    assert "rows [1]" in joined
    assert "non_numeric_sample" not in result.metadata


# ---------------------------------------------------------------------------
# column_preview (review: #359 — capped column lists in validator errors)
# ---------------------------------------------------------------------------


def test_column_preview_short_list_unchanged():
    assert redaction.column_preview(["a", "b", "c"]) == "['a', 'b', 'c']"


def test_column_preview_caps_wide_panels():
    cols = [f"gene_{i}" for i in range(3000)]
    preview = redaction.column_preview(cols)
    assert "(+2990 more of 3000)" in preview
    assert "gene_9" in preview
    assert "gene_10" not in preview  # capped at 10
    # a single error line stays bounded no matter how wide the panel is
    assert len(preview) < 300
