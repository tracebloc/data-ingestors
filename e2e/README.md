# End-to-end ingestion suite

Runs the **real `tracebloc-ingest` engine** against each bundled `templates/`
dataset, into a **real MySQL**, with an in-process mock backend
(`CLIENT_ENV=local`). It proves the *"every shipped config ingests its bundled
data"* guarantee — the gap that let **8/10 modalities fail first-try** when a
config is copied onto the matching template data (#134).

Unlike the unit suite (which mocks the DB + API), this exercises the full
validate → file-transfer → MySQL insert path end to end.

## Run locally

```bash
docker compose -f e2e/docker-compose.yml up -d        # MySQL on :3306
pip install -r requirements.txt && pip install -e .
MYSQL_HOST=127.0.0.1 MYSQL_PORT=3306 DB_USER=root DB_PASSWORD=root \
  DB_NAME=training_test_datasets pytest e2e/ -v
docker compose -f e2e/docker-compose.yml down -v
```

The suite **auto-skips when no MySQL is reachable**, so the default `pytest`
(unit) run is unaffected. CI runs it with a MySQL service in
`.github/workflows/e2e.yml`.

## Cross-repo contract suites

Two files here assert not just "the ingest succeeded" but "what it stored is
what the **training client** resolves", checked against the client's own
derivation rule (transcribed in each file, since neither repo may import the
other):

- `test_semseg_client_contract_e2e.py` — the semseg `mask_id` contract
  (backend#816).
- `test_image_lookup_contract_e2e.py` — the image-lookup contract for every
  file-bearing CV category: the file is named after `filename`, and `data_id`
  names nothing on disk (tracebloc-engine#615, backend#1706). Its unit-level
  sibling is `tests/test_ingest_storage_contract.py`, which also produces the
  capture tracebloc-engine replays through its dataset readers.

## Known gaps (currently `xfail`)

None — all 11 supported modalities ingest cleanly and are covered. The earlier
xfails have all landed: object_detection (#135), masked_language_modeling
(#137), and semantic_segmentation (#136, fixed by the P5 mask_id work). The
semantic_segmentation end-to-end contract (masks land in DEST_PATH + `mask_id`
stored for the training client) is pinned in
`test_characterization.py::test_characterization[semantic_segmentation]`; the
cross-repo client contract is tracked for sign-off in backend#816.

When a future gap arises, add an `xfail(strict=True)` row tied to its ticket so
a landed fix surfaces loudly — an XPASS fails the suite, forcing the mark's
removal (the prior `strict=False` let fixes land silently).
