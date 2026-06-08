"""Scale / chunked-read correctness for CSVIngestor.read_data.

read_data streams the CSV in chunks (``chunk_size``, default 1000). Every other
test uses < 1000 rows -> a single chunk -> the multi-chunk path was never
exercised, which hid a bug: header-strip + type conversion (``_validate_csv``)
ran only on the FIRST chunk, so rows beyond chunk_size were yielded un-converted
(e.g. a DATE column as raw strings) with un-stripped keys. These feed a CSV
LARGER than chunk_size and assert every row across all chunks is correct.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor
from tracebloc_ingestor.utils.constants import TaskCategory


def _ingestor(schema, chunk_size):
    return CSVIngestor(
        database=MagicMock(), api_client=MagicMock(), table_name="t",
        schema=schema, category=TaskCategory.TABULAR_CLASSIFICATION,
        label_column="label", csv_options={"chunk_size": chunk_size},
    )


def _write(tmp_path, header, rows):
    p = tmp_path / "data.csv"
    p.write_text(header + "\n" + "\n".join(rows) + "\n")
    return str(p)


def test_all_rows_yielded_in_order_across_chunks(tmp_path):
    path = _write(tmp_path, "x,label", [f"{i},lab{i}" for i in range(1, 251)])
    ing = _ingestor({"x": "INT", "label": "str"}, chunk_size=50)  # 250 rows -> 5 chunks
    recs = list(ing.read_data(path))
    assert len(recs) == 250                                   # no drops/dupes at boundaries
    assert [int(r["x"]) for r in recs] == list(range(1, 251))  # in order


def test_date_column_converted_in_every_chunk(tmp_path):
    # The bug: rows past chunk 1 came back as raw 'str'. They must all convert.
    path = _write(tmp_path, "ts,label", [f"2024-01-{i:02d},lab{i}" for i in range(1, 11)])
    ing = _ingestor({"ts": "DATE", "label": "str"}, chunk_size=3)  # 10 rows -> 4 chunks
    recs = list(ing.read_data(path))
    assert len(recs) == 10
    assert all(isinstance(r["ts"], pd.Timestamp) for r in recs)


def test_header_whitespace_stripped_in_every_chunk(tmp_path):
    # Leading-space header; keys must be stripped for ALL chunks, not just the first.
    path = _write(tmp_path, " x ,label", [f"{i},lab{i}" for i in range(1, 11)])
    ing = _ingestor({"x": "INT", "label": "str"}, chunk_size=3)
    recs = list(ing.read_data(path))
    assert len(recs) == 10
    assert all("x" in r and " x " not in r for r in recs)


def test_single_chunk_behaviour_unchanged(tmp_path):
    # The common < chunk_size case still converts (regression guard).
    path = _write(tmp_path, "ts,label", ["2024-01-01,a", "2024-01-02,b"])
    ing = _ingestor({"ts": "DATE", "label": "str"}, chunk_size=1000)
    recs = list(ing.read_data(path))
    assert len(recs) == 2
    assert all(isinstance(r["ts"], pd.Timestamp) for r in recs)
