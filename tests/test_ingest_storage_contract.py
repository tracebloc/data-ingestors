"""backend#1706 — the ingest → trainer storage contract, pinned on real rows.

WHY THIS FILE EXISTS
--------------------
tracebloc-engine#615: keypoint + semantic-segmentation training crashed on the
first batch of every CLI-ingested dataset, because the trainer resolved images
by ``data_id`` while this repo names the file after the manifest ``filename``.
Three facts had to stay in agreement and **nothing pinned any of them**:

1. the ingestor writes ``DEST_PATH/<manifest filename><ext>``,
2. it stores that stem in ``filename`` and something unrelated in ``data_id``,
3. so every trainer dataset reader must key on ``filename``.

When #350 flipped the default ``data_id`` strategy ``uuid`` → ``content_hash``,
nothing failed anywhere. The trainer's own fixtures were hand-authored with
``data_id`` set to the on-disk stem and **no ``filename`` column at all** — a
shape this repo has never produced — so they could not catch it either. The two
shipped Python templates that set ``unique_id_column="filename"`` are exactly
``keypoint_detection`` and ``semantic_segmentation``: the two categories whose
readers keyed on ``data_id``. The templates were papering over the reader bug.

WHAT THIS FILE PINS
-------------------
Each case runs a **real ingest** — the same ``cli.conventions.resolve`` →
``cli.run._build_ingestor`` path ``tracebloc dataset push`` takes, with real
validators, real record processing and real file transfer. Only MySQL and the
backend API are faked, and only at their own boundary: every value asserted
below is produced by production code.

Per case we assert:

* **naming** — every stored row resolves to a file at
  ``DEST/<filename><extension>``;
* **independence** — under an opaque ``data_id`` strategy the id names *no*
  file in ``DEST``, so a reader that keys on ``data_id`` cannot work. That is
  the property the two templates hid;
* **stability** — the captured rows match a committed golden, which
  ``tracebloc-engine`` mirrors verbatim into ``core/tests/contracts/`` and
  replays through its four CV dataset readers. A change here that moves the
  on-disk name or the id shape turns THIS suite red with a diff, and the fix
  is to regenerate the golden and carry it across — which is the moment
  someone looks at the trainer.

Regenerate the golden after an intentional change::

    UPDATE_INGEST_CONTRACT_GOLDEN=1 pytest tests/test_ingest_storage_contract.py
"""

from __future__ import annotations

import json
import os
import re
import uuid as uuid_mod
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock

import pytest

from tracebloc_ingestor.cli import run as run_mod
from tracebloc_ingestor.cli.conventions import resolve
from tracebloc_ingestor.config import Config
from tracebloc_ingestor.storage_contract import (
    DATA_ID_STRATEGIES,
    DEFAULT_DATA_ID_STRATEGY,
    EXTENSION_COLUMN,
    IMAGE_NAME_COLUMN,
    IMAGE_NAME_COLUMNS,
    MASK_NAME_COLUMN,
    OPAQUE_DATA_ID_STRATEGIES,
    RECOGNISED_IMAGE_SUFFIXES,
    ROW_ID_COLUMN,
    has_recognised_extension,
    stored_image_name,
    strip_image_extension,
)

REPO = Path(__file__).resolve().parents[1]
T = REPO / "templates"
GOLDEN_PATH = REPO / "tests" / "fixtures" / "contracts" / "ingested_image_rows.json"

# Pinned so the content-hash ids in the golden are reproducible. Production
# mints a random per-table salt (#225) that never leaves the cluster; its value
# is irrelevant to the contract, only its fixedness is.
TABLE_SALT = "1706" * 16

# ``ingestor_id`` is a fresh UUID per run and carries no contract meaning.
VOLATILE_COLUMNS = ("ingestor_id",)

# A uuid-strategy ``data_id`` is random by construction, so the golden stores
# this sentinel in its place. Everything the contract cares about — that the id
# is opaque and names no file — is asserted directly in the test.
UUID_SENTINEL = "<uuid4>"
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def _cfg(**kw: Any) -> Dict[str, Any]:
    base = {"apiVersion": "tracebloc.io/v1", "kind": "IngestConfig", "intent": "train"}
    base.update(kw)
    return base


# The four file-bearing CV categories — the ones with an image-per-row on disk
# and therefore a reader that has to resolve it. Configs match the bundled
# template data (same shape the e2e suite uses).
def _image_classification() -> Dict[str, Any]:
    return _cfg(
        table="contract_img",
        category="image_classification",
        csv=str(T / "image_classification/data/labels_file_sample.csv"),
        images=str(T / "image_classification/data/images"),
        label="label",
        spec={"file_options": {"extension": ".jpeg", "target_size": [256, 256]}},
    )


def _object_detection() -> Dict[str, Any]:
    return _cfg(
        table="contract_od",
        category="object_detection",
        csv=str(T / "object_detection/data/labels_file_sample.csv"),
        images=str(T / "object_detection/data/images"),
        annotations=str(T / "object_detection/data/annotations"),
        label="image_label",
        target_size=[1920, 1080],
    )


def _keypoint_detection() -> Dict[str, Any]:
    return _cfg(
        table="contract_kp",
        category="keypoint_detection",
        csv=str(T / "keypoint_detection/data/labels_file_sample.csv"),
        images=str(T / "keypoint_detection/data/images"),
        label="image_label",
        target_size=[448, 448],
        number_of_keypoints=9,
    )


def _semantic_segmentation() -> Dict[str, Any]:
    return _cfg(
        table="contract_seg",
        category="semantic_segmentation",
        csv=str(T / "semantic_segmentation/semantic_data/labels_file_sample.csv"),
        images=str(T / "semantic_segmentation/semantic_data/images"),
        masks=str(T / "semantic_segmentation/semantic_data/masks"),
        label="image_label",
        schema={"mask_id": "VARCHAR(255)"},
    )


CATEGORY_CONFIGS = {
    "image_classification": _image_classification,
    "object_detection": _object_detection,
    "keypoint_detection": _keypoint_detection,
    "semantic_segmentation": _semantic_segmentation,
}

# (category, data_id spec, case id). Two strategies per category:
#
# - the category's DEFAULT (no ``data_id`` key at all) — what every
#   `tracebloc dataset push` produces today, and the shape that broke #615;
# - ``column`` over ``filename`` — the alias the keypoint / semseg Python
#   templates set, i.e. the coincidence under which a ``data_id``-keyed reader
#   accidentally worked. Kept so the trainer's replay covers BOTH shapes and a
#   future reader can't regress into only handling one.
CASES: List[Tuple[str, Dict[str, Any] | None, str]] = [
    (category, data_id, f"{category}-{label}")
    for category in CATEGORY_CONFIGS
    for data_id, label in (
        (None, "default"),
        ({"strategy": "column", "column": "filename"}, "column"),
    )
]


def _run_ingest(
    category: str,
    data_id: Dict[str, Any] | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Tuple[Config, Any, List[Dict[str, Any]]]:
    """Run one real ingest; return ``(config, resolved, stored_rows)``.

    MySQL and the backend API are the only fakes, and each is faked at its own
    boundary: rows are captured exactly as ``Database.insert_batch`` would
    receive them, after real validation, real record processing and real file
    transfer.
    """
    config = CATEGORY_CONFIGS[category]()
    if data_id is not None:
        config["data_id"] = data_id

    resolved = resolve(config)
    run_config = run_mod._resolve_config(resolved)

    # DEST_PATH is ``STORAGE_PATH/<table>``; /data/shared isn't writable in CI.
    storage = tmp_path / "shared"
    storage.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(Config, "STORAGE_PATH", str(storage))
    monkeypatch.setenv("CLIENT_ENV", "local")

    rows: List[Dict[str, Any]] = []

    database = MagicMock(name="Database")
    database.config = run_config
    database.engine = MagicMock(name="Engine")
    database.create_table.return_value = MagicMock(name="Table")
    database.get_table_schema.return_value = resolved.schema
    database.get_or_create_table_salt.return_value = TABLE_SALT

    def _insert_batch(_table: str, records: List[Dict[str, Any]]):
        rows.extend(dict(record) for record in records)
        return list(range(len(records))), []

    database.insert_batch.side_effect = _insert_batch

    api_client = MagicMock(name="APIClient")
    for method in (
        "send_batch",
        "send_generate_edge_label_meta",
        "send_global_meta_meta",
        "prepare_dataset",
    ):
        getattr(api_client, method).return_value = True
    api_client.create_dataset.return_value = {"id": 1}

    ingestor = run_mod._build_ingestor(database, api_client, resolved)
    ingestor.ingest(resolved.source_path)

    assert rows, f"{category}: the ingest stored no rows"
    return run_config, resolved, rows


def _normalise(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop per-run volatile fields and mask random uuid ids, so the capture is
    comparable against a committed golden."""
    out = []
    for row in rows:
        clean = {
            key: value for key, value in row.items() if key not in VOLATILE_COLUMNS
        }
        row_id = str(clean.get(ROW_ID_COLUMN, ""))
        if _UUID_RE.match(row_id):
            clean[ROW_ID_COLUMN] = UUID_SENTINEL
        out.append(json.loads(json.dumps(clean, default=str)))
    return out


# ---------------------------------------------------------------------------
# The contract, asserted on real ingested rows.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "category,data_id",
    [pytest.param(c, d, id=i) for c, d, i in CASES],
)
def test_every_stored_row_resolves_through_the_filename_column(
    category, data_id, tmp_path, monkeypatch
):
    """FACT 1+2: the file on disk is named after ``filename``, always.

    This is the single rule the trainer has to implement. It holds for every
    category and every ``data_id`` strategy — the strategy is simply not part
    of how a file is named.
    """
    run_config, _resolved, rows = _run_ingest(category, data_id, tmp_path, monkeypatch)
    dest = run_config.DEST_PATH

    for row in rows:
        assert row[IMAGE_NAME_COLUMN], f"{category}: row stored a blank filename"
        expected = stored_image_name(row[IMAGE_NAME_COLUMN], row[EXTENSION_COLUMN])
        assert os.path.isfile(os.path.join(dest, expected)), (
            f"{category}: no file at DEST/{expected} for stored row "
            f"{row[IMAGE_NAME_COLUMN]!r} — the filename column no longer names "
            f"the file the ingest wrote. DEST holds {sorted(os.listdir(dest))}"
        )


@pytest.mark.parametrize(
    "category",
    list(CATEGORY_CONFIGS),
)
def test_default_strategy_data_id_names_no_file_on_disk(
    category, tmp_path, monkeypatch
):
    """FACT 3, stated as the negative: under the default strategy ``data_id``
    resolves to nothing, so a ``data_id``-keyed reader is simply broken.

    This is #615 reproduced at its source. The keypoint / semseg templates set
    ``unique_id_column="filename"`` and so never exercised it; the CLI / YAML
    path — the one customers use — always does.
    """
    run_config, resolved, rows = _run_ingest(category, None, tmp_path, monkeypatch)
    if resolved.data_id_strategy not in OPAQUE_DATA_ID_STRATEGIES:
        pytest.fail(
            f"{category}: default strategy is {resolved.data_id_strategy!r}, which "
            f"is not opaque — update OPAQUE_DATA_ID_STRATEGIES and re-read #615"
        )

    dest = run_config.DEST_PATH
    on_disk = set(os.listdir(dest))
    for row in rows:
        row_id = str(row[ROW_ID_COLUMN])
        assert row_id not in on_disk
        stem = strip_image_extension(row_id)
        for suffix in RECOGNISED_IMAGE_SUFFIXES:
            assert f"{stem}{suffix}" not in on_disk, (
                f"{category}: data_id {row_id!r} happens to name a file on disk. "
                f"That coincidence is what hid tracebloc-engine#615 for two "
                f"releases — do not let a reader depend on it."
            )
        assert row[IMAGE_NAME_COLUMN] != row_id


@pytest.mark.parametrize(
    "category",
    list(CATEGORY_CONFIGS),
)
def test_column_strategy_aliases_data_id_to_filename(category, tmp_path, monkeypatch):
    """The opt-in alias the templates relied on still works — and is opt-in.

    Pinned so nobody "fixes" the divergence by making the alias the default:
    that would re-hide the bug rather than remove it.
    """
    _config, resolved, rows = _run_ingest(
        category, {"strategy": "column", "column": "filename"}, tmp_path, monkeypatch
    )
    assert resolved.unique_id_column == IMAGE_NAME_COLUMN
    for row in rows:
        assert strip_image_extension(row[ROW_ID_COLUMN]) == row[IMAGE_NAME_COLUMN]


def test_semantic_segmentation_mask_resolves_through_its_own_column(
    tmp_path, monkeypatch
):
    """``mask_id`` follows the same rule as ``filename`` (backend#816): it names
    a file, ``data_id`` does not."""
    run_config, _resolved, rows = _run_ingest(
        "semantic_segmentation", None, tmp_path, monkeypatch
    )
    dest = run_config.DEST_PATH
    for row in rows:
        mask = row[MASK_NAME_COLUMN]
        assert mask, "semseg row stored a blank mask_id"
        stem = strip_image_extension(mask)
        assert any(
            os.path.isfile(os.path.join(dest, f"{stem}{suffix}"))
            for suffix in RECOGNISED_IMAGE_SUFFIXES
        ) or os.path.isfile(
            os.path.join(dest, str(mask))
        ), f"no mask file for mask_id {mask!r} in {sorted(os.listdir(dest))}"


# ---------------------------------------------------------------------------
# The golden — the artifact tracebloc-engine replays.
# ---------------------------------------------------------------------------
# The bundled manifests run to hundreds of rows (object detection lists one row
# per OBJECT), and a golden nobody can read is a golden nobody checks. The
# contract is per-row, so a handful exercises it exactly as well.
GOLDEN_ROWS_PER_CASE = 4


def _effective_strategy(resolved) -> str:
    """The strategy that actually minted ``data_id``.

    ``data_id: {strategy: column}`` sets ``unique_id_column`` and leaves
    ``data_id_strategy`` at its default; the column wins at row time
    (``RecordProcessor._map_unique_id``). Record what happened, not what the
    unused field says.
    """
    return "column" if resolved.unique_id_column else resolved.data_id_strategy


def _capture_all(tmp_path_factory, monkeypatch) -> Dict[str, Any]:
    cases = []
    for category, data_id, case_id in CASES:
        tmp_path = tmp_path_factory.mktemp(case_id.replace("/", "_"))
        run_config, resolved, rows = _run_ingest(
            category, data_id, tmp_path, monkeypatch
        )
        cases.append(
            {
                "id": case_id,
                "category": category,
                "data_id_strategy": _effective_strategy(resolved),
                "unique_id_column": resolved.unique_id_column,
                "total_rows_ingested": len(rows),
                "dest_files": sorted(os.listdir(run_config.DEST_PATH)),
                "rows": _normalise(rows[:GOLDEN_ROWS_PER_CASE]),
            }
        )
    return {
        "_README": [
            "Captured from REAL ingests by tests/test_ingest_storage_contract.py",
            "(data-ingestors). Do not hand-edit: regenerate with",
            "UPDATE_INGEST_CONTRACT_GOLDEN=1 pytest tests/test_ingest_storage_contract.py",
            "",
            "tracebloc-engine mirrors this file verbatim into",
            "core/tests/contracts/ingested_image_rows.json and replays it through",
            "its CV dataset readers (backend#1706). Carry any regeneration across.",
            "",
            f"'ingestor_id' is dropped and a uuid-strategy data_id is masked as "
            f"'{UUID_SENTINEL}' — both are random per run.",
            f"content_hash ids are computed under the fixed test salt {TABLE_SALT!r}.",
            f"'rows' holds the first {GOLDEN_ROWS_PER_CASE} of "
            f"'total_rows_ingested'; the contract is per-row.",
        ],
        "contract": {
            "image_name_columns": list(IMAGE_NAME_COLUMNS),
            "data_id_strategies": list(DATA_ID_STRATEGIES),
            "default_data_id_strategy": DEFAULT_DATA_ID_STRATEGY,
            "opaque_data_id_strategies": sorted(OPAQUE_DATA_ID_STRATEGIES),
            "recognised_image_suffixes": list(RECOGNISED_IMAGE_SUFFIXES),
            "uuid_sentinel": UUID_SENTINEL,
        },
        "cases": cases,
    }


def test_golden_matches_a_real_ingest(tmp_path_factory, monkeypatch):
    """The committed golden IS what the pipeline produces, today.

    The trainer's fixtures are built from this file rather than hand-authored,
    which is the whole point: a hand-authored fixture can express a row shape
    the ingestor never produces (and did — engine#615's keypoint fixture had no
    ``filename`` column at all), so it proves nothing about the real boundary.
    """
    captured = _capture_all(tmp_path_factory, monkeypatch)

    if os.environ.get("UPDATE_INGEST_CONTRACT_GOLDEN"):
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_PATH.write_text(json.dumps(captured, indent=2) + "\n")
        pytest.skip(f"golden regenerated at {GOLDEN_PATH}")

    assert GOLDEN_PATH.is_file(), (
        f"missing golden {GOLDEN_PATH}; regenerate with "
        f"UPDATE_INGEST_CONTRACT_GOLDEN=1"
    )
    golden = json.loads(GOLDEN_PATH.read_text())
    assert captured["contract"] == golden["contract"]
    assert captured["cases"] == golden["cases"], (
        "the ingest no longer produces the committed golden. If the change is "
        "intended, regenerate with UPDATE_INGEST_CONTRACT_GOLDEN=1 AND copy the "
        "file into tracebloc-engine core/tests/contracts/ — the trainer replays "
        "it (backend#1706)."
    )


def test_golden_rows_carry_the_columns_a_reader_needs(tmp_path_factory):
    """Guard against a golden that is technically current but useless: every
    row must still carry the columns a trainer resolves files through."""
    golden = json.loads(GOLDEN_PATH.read_text())
    for case in golden["cases"]:
        for row in case["rows"]:
            assert IMAGE_NAME_COLUMN in row, case["id"]
            assert ROW_ID_COLUMN in row, case["id"]
            assert EXTENSION_COLUMN in row, case["id"]
            name = stored_image_name(row[IMAGE_NAME_COLUMN], row[EXTENSION_COLUMN])
            assert name in case["dest_files"], (
                f"{case['id']}: golden row names {name!r}, which is not in the "
                f"golden's own dest listing {case['dest_files']}"
            )


# ---------------------------------------------------------------------------
# The contract module itself — mirrored verbatim by tracebloc-engine's
# core/tests/contracts/test_ingest_contract_mirror.py. Keep the two tables in
# step; a divergence here is a divergence in what the trainer believes.
# ---------------------------------------------------------------------------
CONTRACT_VALUES = {
    "IMAGE_NAME_COLUMN": "filename",
    "ROW_ID_COLUMN": "data_id",
    "EXTENSION_COLUMN": "extension",
    "MASK_NAME_COLUMN": "mask_id",
    "IMAGE_NAME_COLUMNS": ("filename", "data_id"),
    "DATA_ID_STRATEGIES": ("content_hash", "uuid", "column"),
    "DEFAULT_DATA_ID_STRATEGY": "content_hash",
    "RECOGNISED_IMAGE_SUFFIXES": (".jpeg", ".jpg", ".png"),
}

# (value, stripped stem). Names with internal dots must survive: the pipeline
# strips only a RECOGNISED trailing suffix, so a ``split(".")[0]`` reader would
# look for a file that does not exist.
STRIP_CASES = [
    ("cat1.jpg", "cat1"),
    ("cat1.JPEG", "cat1"),
    ("image.001.jpg", "image.001"),
    ("image.001", "image.001"),
    ("no_extension", "no_extension"),
    ("archive.tar", "archive.tar"),
    ("", ""),
]


@pytest.mark.parametrize("name,expected", [(k, v) for k, v in CONTRACT_VALUES.items()])
def test_contract_values(name, expected):
    import tracebloc_ingestor.storage_contract as contract

    assert getattr(contract, name) == expected


@pytest.mark.parametrize("value,stem", STRIP_CASES)
def test_strip_image_extension(value, stem):
    assert strip_image_extension(value) == stem


@pytest.mark.parametrize("value,stem", STRIP_CASES)
def test_has_recognised_extension_agrees_with_strip(value, stem):
    assert has_recognised_extension(value) is (strip_image_extension(value) != value)


@pytest.mark.parametrize(
    "filename,extension,expected",
    [
        ("cat1", ".jpg", "cat1.jpg"),
        ("cat1.jpg", ".jpg", "cat1.jpg"),  # already suffixed — not doubled (#105)
        ("cat1.JPEG", ".jpg", "cat1.JPEG"),  # keeps its own case + kind
        ("image.001", ".png", "image.001.png"),
    ],
)
def test_stored_image_name(filename, extension, expected):
    assert stored_image_name(filename, extension) == expected


def test_contract_agrees_with_the_transfer_code_it_describes():
    """``storage_contract.has_recognised_extension`` must agree with
    ``file_transfer._has_extension`` on IMAGE suffixes.

    The transfer helper is deliberately broader (it also recognises the sidecar
    ``.xml`` / ``.txt`` suffixes), so this pins agreement on the image subset
    only — that is the subset the trainer mirrors.
    """
    from tracebloc_ingestor.file_transfer import _has_extension

    for suffix in RECOGNISED_IMAGE_SUFFIXES:
        for value in (f"cat1{suffix}", f"cat1{suffix.upper()}"):
            assert has_recognised_extension(value) is _has_extension(value) is True
    assert has_recognised_extension("cat1") is _has_extension("cat1") is False


def test_uuid_sentinel_never_collides_with_a_real_id():
    assert not _UUID_RE.match(UUID_SENTINEL)
    assert _UUID_RE.match(str(uuid_mod.uuid4()))
