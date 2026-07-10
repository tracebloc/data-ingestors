"""backend#816 — the semseg ``mask_id`` contract, enforced at preflight.

The training client resolves each mask file from a MySQL ``mask_id`` column
(``segmentation_dataset_pytorch.py``: ``str(row["mask_id"])`` → filename, with
no naming-convention fallback), so a semseg ingest that stores a NULL/absent
``mask_id`` makes the client crash with ``FileNotFoundError`` — not a silent
skip.

The contract is REQUIRED and ENFORCED (di#358, per reviewer): rather than
silently auto-adding ``mask_id`` to the schema, the manifest must declare it and
populate it on every row, and :class:`MaskIdColumnValidator` rejects a manifest
that doesn't at preflight. These tests pin:

- a semseg manifest WITHOUT a ``mask_id`` column → the validator FAILS with an
  actionable message;
- ``mask_id`` empty/NULL on any row → FAILS;
- a valid manifest (``mask_id`` present + populated) → PASSES;
- (record level) a DECLARED ``mask_id`` is stored populated on the processed
  row — the exact ingestor→client boundary the develop regression (#212's
  blanket ``mask_id`` pop) slipped through.
"""

from __future__ import annotations

import pandas as pd

from tracebloc_ingestor.ingestors.record_processor import RecordProcessor
from tracebloc_ingestor.validators.mask_id_validator import MaskIdColumnValidator


# ---------------------------------------------------------------------------
# The preflight validator — the enforcement point for the required contract.
# ---------------------------------------------------------------------------
def test_missing_mask_id_column_is_rejected(make_csv):
    # A semseg manifest whose header omits mask_id (only filename + label).
    path = make_csv(
        pd.DataFrame(
            {"filename": ["image_001", "image_002"], "image_label": ["road", "sky"]}
        )
    )
    result = MaskIdColumnValidator(column="mask_id").validate(str(path))
    assert not result.is_valid
    msg = result.errors[0]
    # names the missing column, explains the client contract + where to look
    assert "'mask_id' not found" in msg
    assert "training client reads 'mask_id'" in msg
    assert "backend#816" in msg
    assert "templates/semantic_segmentation/" in msg
    # lists the columns that ARE present, to guide the fix
    assert "filename" in msg and "image_label" in msg
    assert result.metadata["columns"] == ["filename", "image_label"]


def test_empty_mask_id_on_a_row_is_rejected(make_csv):
    # mask_id present in the header but blank on one row.
    path = make_csv(
        pd.DataFrame(
            {
                "filename": ["image_001", "image_002"],
                "mask_id": ["image_001_mask", ""],
                "image_label": ["road", "sky"],
            }
        )
    )
    result = MaskIdColumnValidator(column="mask_id").validate(str(path))
    assert not result.is_valid
    msg = result.errors[0]
    assert "'mask_id' is empty/NULL on row(s) 2" in msg
    assert "FileNotFoundError" in msg
    assert "templates/semantic_segmentation/" in msg
    assert result.metadata["empty_rows"] == [2]


def test_null_mask_id_on_a_row_is_rejected(make_csv):
    # A truly missing cell (NaN), not just an empty string.
    path = make_csv(
        pd.DataFrame(
            {
                "filename": ["image_001", "image_002"],
                "mask_id": ["image_001_mask", None],
                "image_label": ["road", "sky"],
            }
        )
    )
    result = MaskIdColumnValidator(column="mask_id").validate(str(path))
    assert not result.is_valid
    assert result.metadata["empty_rows"] == [2]


def test_na_sentinel_literal_mask_id_is_rejected(make_csv):
    # mask_id is a DECLARED schema column, so CSVIngestor reads it with the
    # curated NA_SENTINELS set (build_csv_na_values + keep_default_na=False) and
    # stores a literal "none"/"null"/"NA" cell as SQL NULL -> the client crashes
    # with FileNotFoundError. pandas' DEFAULT NA set misses lowercase "none", so
    # the validator MUST use the same NA_SENTINELS to stay byte-identical to the
    # write path and reject it at preflight rather than passing a row that is
    # nulled a layer later. ("none" is the concrete divergence; "NA"/"null"/
    # "None"/"nan" the default set happens to share.)
    path = make_csv(
        pd.DataFrame(
            {
                "filename": ["image_001", "image_002"],
                "mask_id": ["image_001_mask", "none"],
                "image_label": ["road", "sky"],
            }
        )
    )
    result = MaskIdColumnValidator(column="mask_id").validate(str(path))
    assert not result.is_valid, "literal 'none' is nulled by the ingestor"
    assert result.metadata["empty_rows"] == [2]


def test_valid_semseg_manifest_passes(make_csv):
    # mask_id present and populated on every row → passes.
    path = make_csv(
        pd.DataFrame(
            {
                "filename": ["image_001", "image_002", "image_003"],
                "mask_id": ["image_001_mask", "image_002_mask", "image_003_mask"],
                "image_label": ["road", "building", "person"],
            }
        )
    )
    result = MaskIdColumnValidator(column="mask_id").validate(str(path))
    assert result.is_valid
    assert result.metadata["checked"] is True


def test_mask_id_match_is_case_insensitive(make_csv):
    # CSVIngestor resolves headers case-insensitively; mirror that here so a
    # header like "Mask_Id" is accepted, not falsely rejected as missing.
    path = make_csv(
        pd.DataFrame({"filename": ["image_001"], "Mask_Id": ["image_001_mask"]})
    )
    result = MaskIdColumnValidator(column="mask_id").validate(str(path))
    assert result.is_valid


def test_non_csv_input_defers(make_csv):
    # JSON / non-path inputs are handled by their own read path — defer (pass),
    # exactly like LabelColumnValidator.
    result = MaskIdColumnValidator(column="mask_id").validate("data.json")
    assert result.is_valid
    assert result.metadata["checked"] is False


def test_unreadable_csv_is_a_benign_skip(tmp_path):
    # A missing CSV path can't be introspected; the read/transfer path raises
    # its own clear error. Don't double-report — defer.
    result = MaskIdColumnValidator(column="mask_id").validate(str(tmp_path / "no.csv"))
    assert result.is_valid
    assert result.metadata["checked"] is False


# ---------------------------------------------------------------------------
# The declaration half of the contract — mask_id must be a DECLARED schema
# column, or RecordProcessor drops it and the stored table has none (the exact
# backend#816 shape a CSV-only check waves through). Enforced only when a schema
# is supplied; the semseg factory always supplies it, bare construction skips it.
# ---------------------------------------------------------------------------
def test_undeclared_mask_id_in_schema_is_rejected(make_csv):
    # mask_id present + populated in the CSV, but the schema OMITS it: the
    # ingestor would drop the column -> broken table. This is the original #816
    # bug shape; a CSV-only check would pass it.
    path = make_csv(
        pd.DataFrame(
            {
                "filename": ["image_001", "image_002"],
                "mask_id": ["image_001_mask", "image_002_mask"],
                "image_label": ["road", "sky"],
            }
        )
    )
    result = MaskIdColumnValidator(
        column="mask_id", schema={"image_label": "VARCHAR(255)"}
    ).validate(str(path))
    assert not result.is_valid
    msg = result.errors[0]
    assert "DECLARED" in msg and "'mask_id'" in msg
    assert "backend#816" in msg
    assert "templates/semantic_segmentation/" in msg
    assert result.metadata["schema_columns"] == ["image_label"]


def test_no_schema_at_all_is_rejected(make_csv):
    # A semseg ingest with no schema can't store mask_id -> reject. An explicit
    # None counts as "supplied" (the factory passes the resolved schema, which is
    # None when the config omits the schema key entirely).
    path = make_csv(
        pd.DataFrame({"filename": ["image_001"], "mask_id": ["image_001_mask"]})
    )
    result = MaskIdColumnValidator(column="mask_id", schema=None).validate(str(path))
    assert not result.is_valid
    assert "DECLARED" in result.errors[0]


def test_declared_schema_with_mask_id_passes(make_csv):
    # Declared in the schema AND populated in the CSV -> the full contract holds.
    path = make_csv(
        pd.DataFrame(
            {
                "filename": ["image_001", "image_002"],
                "mask_id": ["image_001_mask", "image_002_mask"],
                "image_label": ["road", "sky"],
            }
        )
    )
    result = MaskIdColumnValidator(
        column="mask_id", schema={"mask_id": "VARCHAR(255)"}
    ).validate(str(path))
    assert result.is_valid
    assert result.metadata["checked"] is True


def test_declared_schema_match_is_case_insensitive(make_csv):
    # A schema key "Mask_Id" satisfies the declaration check for column "mask_id"
    # (same case/whitespace-insensitive resolution the ingestor uses).
    path = make_csv(
        pd.DataFrame({"filename": ["image_001"], "mask_id": ["image_001_mask"]})
    )
    result = MaskIdColumnValidator(
        column="mask_id", schema={"Mask_Id": "VARCHAR(255)"}
    ).validate(str(path))
    assert result.is_valid


# ---------------------------------------------------------------------------
# Record level — a DECLARED mask_id is stored populated on the processed row.
# This pins the ingestor→client boundary #212's blanket pop regressed.
# ---------------------------------------------------------------------------
def test_declared_mask_id_is_stored_populated_on_the_row():
    rp = RecordProcessor(
        schema={"mask_id": "VARCHAR(255)", "image_label": "VARCHAR(255)"},
        intent="train",
        label_column=None,
        annotation_column=None,
        unique_id_column=None,
        label_policy=None,
        ingestor_id="t",
    )
    row = rp.process(
        {
            "filename": "image_001.jpg",
            "extension": "jpg",
            "mask_id": "image_001_mask.png",
            "image_label": "road",
        }
    )
    assert row is not None
    assert row.get("mask_id") == "image_001_mask.png"
