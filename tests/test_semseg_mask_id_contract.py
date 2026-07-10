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
import pytest

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
    # 0-based data-row index (row_refs), matching every other validator.
    assert "'mask_id' is empty/NULL at rows [1]" in msg
    assert "FileNotFoundError" in msg
    assert "templates/semantic_segmentation/" in msg
    assert result.metadata["empty_rows"] == [1]


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
    assert result.metadata["empty_rows"] == [1]


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
    assert result.metadata["empty_rows"] == [1]


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


def test_non_lowercase_header_is_rejected(make_csv):
    # The stored column IS the CSV header and the training client reads the mask
    # column by the LITERAL lowercase name 'mask_id' — so a 'Mask_Id' header is
    # rejected with a rename hint, not silently accepted (it would break training
    # after a green preflight).
    path = make_csv(
        pd.DataFrame({"filename": ["image_001"], "Mask_Id": ["image_001_mask"]})
    )
    result = MaskIdColumnValidator(column="mask_id").validate(str(path))
    assert not result.is_valid
    assert "must be named exactly 'mask_id'" in result.errors[0]


def test_missing_column_message_caps_header_list(make_csv):
    # A wide manifest without mask_id: the error names only a capped sample of
    # headers (bounded message); the full list stays in metadata.
    path = make_csv(pd.DataFrame({f"c{i}": ["x"] for i in range(50)}))
    result = MaskIdColumnValidator(column="mask_id").validate(str(path))
    assert not result.is_valid
    assert "more)" in result.errors[0]  # truncation marker
    assert len(result.metadata["columns"]) == 50  # full detail retained


def test_undeclared_message_caps_schema_key_list(make_csv):
    # Symmetric to the header cap: a wide schema that OMITS mask_id names only a
    # capped sample of declared keys in the message; the full list stays in
    # metadata (bugbot: unbounded undeclared-schema error string).
    path = make_csv(pd.DataFrame({"filename": ["x"], "mask_id": ["m"]}))
    schema = {f"c{i}": "VARCHAR(255)" for i in range(50)}  # 50 keys, no mask_id
    result = MaskIdColumnValidator(column="mask_id", schema=schema).validate(str(path))
    assert not result.is_valid
    assert "more)" in result.errors[0]  # truncation marker
    assert len(result.metadata["schema_columns"]) == 50  # full detail retained


def test_semseg_factory_checks_stored_schema_not_full_schema(make_csv):
    # If mask_id is (mis)configured as the label/unique_id/annotation column,
    # BaseIngestor strips it from the STORED table schema and RecordProcessor
    # drops it — so the semseg factory must wire the validator with the stripped
    # options["schema"], not full_schema, or preflight green-lights a manifest
    # whose stored table has no mask_id column (backend#816).
    from tracebloc_ingestor.modalities import validators as modality_validators

    path = make_csv(pd.DataFrame({"filename": ["a"], "mask_id": ["a_mask"]}))
    options = {
        "extension": ".jpg",
        "target_size": [128, 128],
        "full_schema": {"mask_id": "VARCHAR(255)"},  # mask_id IS in the full schema
        "schema": {"other": "INT"},  # ...but stripped out of the stored schema
    }
    mask_validator = next(
        v
        for v in modality_validators.semantic_segmentation(options)
        if isinstance(v, MaskIdColumnValidator)
    )
    result = mask_validator.validate(str(path))
    assert not result.is_valid  # stored schema lacks mask_id -> rejected


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


def test_non_lowercase_schema_key_is_rejected(make_csv):
    # Schema declares "Mask_Id" (header is lowercase "mask_id"): the schema key
    # must be exactly lowercase "mask_id" — the client reads the stored column by
    # that literal name — so it's rejected with a rename hint.
    path = make_csv(
        pd.DataFrame({"filename": ["image_001"], "mask_id": ["image_001_mask"]})
    )
    result = MaskIdColumnValidator(
        column="mask_id", schema={"Mask_Id": "VARCHAR(255)"}
    ).validate(str(path))
    assert not result.is_valid
    assert "Rename the schema key to 'mask_id'" in result.errors[0]


def test_non_lowercase_header_with_lowercase_schema_is_rejected(make_csv):
    # Schema declares lowercase 'mask_id' but the CSV header is 'Mask_ID': the
    # header must be exactly lowercase 'mask_id' (the stored + client-read name),
    # rejected with a rename hint.
    path = make_csv(
        pd.DataFrame({"filename": ["image_001"], "Mask_ID": ["image_001_mask"]})
    )
    result = MaskIdColumnValidator(
        column="mask_id", schema={"mask_id": "VARCHAR(255)"}
    ).validate(str(path))
    assert not result.is_valid
    assert "must be named exactly 'mask_id'" in result.errors[0]


def test_delimiter_from_csv_options_is_honored(tmp_path):
    # A semicolon-delimited manifest (a supported csv_options.delimiter) must be
    # parsed with that delimiter. Without it the whole header collapses into one
    # column and mask_id is falsely reported missing — a false reject of a
    # dataset the ingestor would parse and ingest fine.
    path = tmp_path / "labels_semi.csv"
    path.write_text("filename;mask_id;image_label\nimg_001;img_001_mask;road\n")
    # Default (comma) parse: single collapsed column -> false "missing".
    assert not MaskIdColumnValidator(column="mask_id").validate(str(path)).is_valid
    # With the run's delimiter -> parses correctly and passes.
    honored = MaskIdColumnValidator(
        column="mask_id",
        schema={"mask_id": "VARCHAR(255)"},
        csv_options={"delimiter": ";"},
    ).validate(str(path))
    assert honored.is_valid, honored.errors


def test_quotechar_from_csv_options_is_honored(tmp_path):
    # A custom quotechar (') quotes a mask_id embedding the delimiter on one row,
    # and mask_id is BLANK on another. Only when the validator parses with the
    # run's quotechar (like CSVIngestor) does the quoted row parse cleanly so the
    # blank is caught; without it that row is ragged, the scan errors out, and the
    # blank mask_id slips through as a false pass.
    path = tmp_path / "labels_quoted.csv"
    path.write_text("filename;mask_id;image_label\nimg_001;'a;b';road\nimg_002;;sky\n")
    result = MaskIdColumnValidator(
        column="mask_id",
        schema={"mask_id": "VARCHAR(255)"},
        csv_options={"delimiter": ";", "quotechar": "'"},
    ).validate(str(path))
    assert not result.is_valid  # blank mask_id on the 2nd row is caught
    assert "empty/NULL" in result.errors[0]


def test_index_col_csv_option_does_not_defeat_the_gate(tmp_path):
    # index_col is a valid read_csv option a run might set, but forwarding it into
    # the single-column scan would collide with usecols and (if swallowed)
    # fail-open the gate. It must be dropped from the dialect passthrough so a
    # blank mask_id is still rejected.
    path = tmp_path / "labels_idx.csv"
    path.write_text("id,filename,mask_id\n1,img_001,\n")
    result = MaskIdColumnValidator(
        column="mask_id",
        schema={"filename": "VARCHAR(255)", "mask_id": "VARCHAR(255)"},
        csv_options={"index_col": "id"},
    ).validate(str(path))
    assert not result.is_valid  # blank mask_id caught despite index_col
    assert "empty/NULL" in result.errors[0]


def test_validation_error_does_not_leak_cell_content(tmp_path):
    # A manifest whose header reads fine but whose body has an unterminated quote:
    # the full scan raises a pandas ParserError (which can embed raw cell content).
    # The validator must surface only the exception TYPE + generic guidance, never
    # str(e) — customer cell values stay on-prem (bugbot: leaked-cell-values rule).
    path = tmp_path / "labels_badquote.csv"
    path.write_text('filename,mask_id\nimg_001,ok\nimg_002,"leaky_secret_value\n')
    result = MaskIdColumnValidator(
        column="mask_id", schema={"mask_id": "VARCHAR(255)"}
    ).validate(str(path))
    assert not result.is_valid  # a broken manifest is rejected (not fail-open)
    msg = result.errors[0]
    assert "leaky_secret_value" not in msg  # no raw cell content leaked
    assert result.metadata.get("error_type") == "validation_exception"


def test_non_string_delimiter_rejected_at_construction():
    # csv_options is validated at construction, not mid-scan: a non-string
    # delimiter raises a clear ValueError up front instead of a generic mask-id
    # failure deep in the read (bugbot: validate-config-at-construction rule).
    with pytest.raises(ValueError, match="delimiter"):
        MaskIdColumnValidator(column="mask_id", csv_options={"delimiter": 3})


def test_malformed_manifest_defers_gracefully(tmp_path):
    # A manifest with invalid UTF-8 bytes: the header read can't introspect it,
    # so the validator DEFERS (checked=False) rather than crashing — the
    # ingestor's own read (same dialect, on_bad_lines="error") surfaces the
    # malformation. (On a file large enough that the nrows=0 header buffer clears
    # the corruption, the body scan reaches it and PROPAGATES the parse error to
    # a rejection instead of a misleading partial count — see _empty_value_rows.)
    path = tmp_path / "labels_badbytes.csv"
    path.write_bytes(b"filename,mask_id\nimg_001,\xff\xfe\n")
    result = MaskIdColumnValidator(
        column="mask_id", schema={"mask_id": "VARCHAR(255)"}
    ).validate(str(path))
    assert result.is_valid  # defers (checked=False); does not crash
    assert result.metadata["checked"] is False


def test_case_colliding_headers_inspect_the_schema_exact_column(tmp_path):
    # Header carries BOTH 'Mask_ID' (empty) and 'mask_id' (populated); the schema
    # declares lowercase 'mask_id', which the ingestor keeps verbatim. The
    # emptiness scan must inspect the schema-exact 'mask_id' (populated) — not the
    # case-colliding empty 'Mask_ID' — else it false-rejects a manifest that
    # ingests fine.
    path = tmp_path / "labels_collide.csv"
    path.write_text("filename,Mask_ID,mask_id\nimg_001,,img_001_mask\n")
    result = MaskIdColumnValidator(
        column="mask_id", schema={"mask_id": "VARCHAR(255)"}
    ).validate(str(path))
    assert result.is_valid, result.errors


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
