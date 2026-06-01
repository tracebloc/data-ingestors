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

## Known gaps (currently `xfail`)

| Modality | Why | Ticket |
|---|---|---|
| object_detection | bundled VisDrone XML uses `difficult=2`; validator only accepts `0/1` | #135 |
| semantic_segmentation | mask sidecar column not wired through the declarative path | #136 |
| masked_language_modeling | template missing the required `tokenizer.json` | #137 |

When a fix lands, the corresponding test XPASSes — drop the `xfail` mark.
