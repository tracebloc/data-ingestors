# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Does

Data ingestion pipeline for the tracebloc platform. Validates, preprocesses, and transfers datasets into a Kubernetes-based training environment. Publishes as `tracebloc_ingestor` on PyPI. Only metadata syncs to the tracebloc web app; raw data stays on-premise.

## Install and Test

```bash
pip install -e .            # editable install
pip install -r requirements.txt  # includes dev/test deps
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
# Requires MYSQL_HOST env var; entrypoint waits for MySQL, then runs ingestor.py
```

## Supported Data Types

- **Image** -- classification, detection, segmentation datasets
- **Tabular** -- CSV with structured features
- **JSON** -- structured JSON documents

Ingestor implementations are in `tracebloc_ingestor/ingestors/` (`base.py`, `csv_ingestor.py`, `json_ingestor.py`).

## Architecture

- **`tracebloc_ingestor/`** -- main package
  - `ingestors/` -- base class and format-specific ingestors (CSV, JSON)
  - `validators/` -- data validation logic
  - `api/` -- API client for communicating with tracebloc backend
  - `database.py` -- MySQL/SQLAlchemy database operations
  - `file_transfer.py` -- secure file transfer to cluster storage
  - `config.py` -- configuration
  - `utils/` -- shared utilities
- **`templates/`** -- ingestor scripts used inside Docker containers
- **`docker-entrypoint.sh`** -- waits for MySQL, then runs the ingestor
- **`ingestor-job.yaml`** -- Kubernetes job manifest

## Key Dependencies

Python 3.8+, `sqlalchemy`, `mysql-connector-python`, `pandas`, `Pillow`, `requests`, `tenacity`, `tqdm`.
