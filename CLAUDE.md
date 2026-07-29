# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Does

Data ingestion pipeline for the tracebloc platform. Validates, preprocesses, and transfers datasets into a Kubernetes-based training environment. Publishes as `tracebloc_ingestor` on PyPI. Only metadata syncs to the tracebloc web app; raw data stays on-premise.

## Install and Test

```bash
pip install -e .            # editable install
pip install -r requirements-dev.txt  # runtime + dev/test deps
pytest                      # run tests
```

Build and publish:
```bash
python setup.py sdist bdist_wheel
twine upload dist/*
```

Docker (runs the CSV ingestor as a Kubernetes job):
```bash
docker build -t tracebloc-ingestor .
# Requires MYSQL_HOST env var; entrypoint waits for MySQL, then exec's the
# `tracebloc-ingest` console script (setup.py -> tracebloc_ingestor.cli.run:main)
```

## Supported Task Categories

Ingestion is organized around **task categories**, not raw file formats. The
single source of truth is the `ModalitySpec` registry in
`tracebloc_ingestor/modalities/registry.py`, which currently defines **16**
categories (kept 1:1 with the model-zoo task families):

`image_classification`, `object_detection`, `keypoint_detection`,
`semantic_segmentation`, `text_classification`, `token_classification`,
`sentence_pair_classification`, `masked_language_modeling`,
`causal_language_modeling`, `seq2seq`, `embeddings`, `tabular_classification`,
`tabular_regression`, `time_series_forecasting`, `time_series_classification`,
`time_to_event_prediction`.

Each spec declares the category's data format, per-row sidecar files (if any),
validator factory, and transfer factory. Data enters through the format-specific
ingestors in `tracebloc_ingestor/ingestors/` (`csv_ingestor.py`,
`json_ingestor.py`, both extending `base.py`); the resolved task category — not
the file extension — drives validation and file transfer.

## Architecture

- **`tracebloc_ingestor/`** -- main package
  - `cli/` -- `tracebloc-ingest` console-script entry point (`run.py`) + YAML-driven config conventions (`conventions.py`)
  - `modalities/` -- the `ModalitySpec` registry (`registry.py`), spec model (`spec.py`), layout schema (`layout.py`), and per-category validator/transfer factories — the single source of truth for the 16 task categories
  - `schema/` -- versioned JSON schemas for the ingest config (`ingest.v1.json`, `layout.v1.json`)
  - `ingestors/` -- base class and format-specific ingestors (CSV, JSON), record processing, batch writing
  - `validators/` -- data validation logic
  - `api/` -- API client for communicating with tracebloc backend
  - `database.py` -- MySQL/SQLAlchemy database operations
  - `file_transfer.py` -- secure file transfer to cluster storage
  - `config.py` -- configuration
  - `utils/` -- shared utilities (incl. `TaskCategory` constants, logging)
- **`templates/`** -- ingestor scripts used inside Docker containers
- **`docker-entrypoint.sh`** -- waits for MySQL, then exec's the `tracebloc-ingest` console script
- **`ingestor-job.yaml`** -- Kubernetes job manifest

## Key Dependencies

Python 3.11+, `sqlalchemy`, `mysql-connector-python`, `pandas`, `Pillow`, `requests`, `tenacity`, `tqdm`.
