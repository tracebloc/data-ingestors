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
  - `storage_contract.py` -- the ingest→trainer storage contract (framework column names, `data_id` strategies, the on-disk naming rule). Stdlib-only and mirrored verbatim by tracebloc-engine's `core/utils/ingest_contract.py`; change it here first (backend#1706)
  - `config.py` -- configuration
  - `utils/` -- shared utilities (incl. `TaskCategory` constants, logging)
- **`templates/`** -- ingestor scripts used inside Docker containers
- **`docker-entrypoint.sh`** -- waits for MySQL, then exec's the `tracebloc-ingest` console script
- **`ingestor-job.yaml`** -- Kubernetes job manifest

## Key Dependencies

Python 3.11+, `sqlalchemy`, `mysql-connector-python`, `pandas`, `Pillow`, `requests`, `tenacity`, `tqdm`.

<!-- org-standards:begin -->
## tracebloc engineering standards (org-wide)

<!-- Canonical source: tracebloc/.github/org-standards.md.
     Synced into every repo's CLAUDE.md between org-standards markers — never
     edit it inside a consuming repo; open a PR against tracebloc/.github.
     Meta-rule: the moment a rule below becomes mechanically enforced (a lint
     rule, a house-rules grep, a required check), delete the sentence here and
     let the check carry it. Prose is only for what tooling can't judge. -->

### Branches & PRs

- Branch model: `develop → staging → main`. Branch off `develop`; every PR targets `develop`. Never open PRs to `staging` or `main` — promotions are the release train's job. (Sole exception: the `docs` repo may target `main`.)
- Before starting any task: `git fetch` and branch from the current tip of `develop` — never build on a stale checkout. A branch that lives more than a day gets `develop` merged back in before review. We move fast; stale starts mean silent divergence and duplicated work.
- One self-contained change per PR. A few hundred changed lines reviews well; at 1000+ split it. Refactors ship in separate PRs from behavior changes.
- Branches are short-lived (aim to merge within a day or two), single-author, and based on `develop` — no stacked PRs on top of other open PRs.
- Names and commits: `feat/ fix/ docs/ sec/ ci/ chore/` + issue number + short slug (`fix/1234-ingest-timeout`); commit subjects `type(scope): summary`, referencing the ticket (`backend#1234`).
- When you open a PR: assign yourself and request exactly one reviewer immediately — a PR without a reviewer stalls by construction. You pick the reviewer: whoever knows the code best. There is no per-repo default, and no automation assigns one — branch protection just refuses to merge without a review.
- When you are the reviewer: first response within one business day.

### Quality bar

- Before every push: run the linter and the tests that cover your change. Never push a branch you believe is red — CI is the backstop, not the first run.
- Read the full diff before opening the PR. You own every line you ship, whoever — or whatever — wrote it.
- AI sessions end with evidence, not assertion: run the relevant check (tests, build, lint) and show the output. A change that could not be verified does not ship.
- After opening or pushing to a PR, stay on it: poll CI and Bugbot on the current head and triage every finding the same day — fix it, or reply on the thread saying why not. No silent dismissals. Unresolved threads block the merge and stall the release train's settle stage; cheap now beats expensive later.
- A finding that recurs across PRs becomes a rule: add it to `.cursor/BUGBOT.md`, and if it is grep-expressible, to code-quality's house-rules — then stop re-arguing it in comments.
- Style and naming rules live in tooling (black/ruff, eslint/prettier, house-rules), never in prose. If a rule matters, encode it; do not restate linter rules in CLAUDE.md files.
- Never commit secrets, tokens, or customer data — not in code, config, tests, issues, or commit messages. gitleaks catches secrets in **code**. Nothing scans PR titles, descriptions or commit messages: the public PII gate that did was retired on 2026-08-06 (backend#1409), so keeping customer names out of PR prose on public repos is on you, not on a check.

### Engineer kanban

- Every ticket on the board carries a `Status` — no card sits at "No Status". New tickets start in `Backlog`. **Bugs are the exception:** label them `work-type:bug` (the Bug template does it) and put them straight into `Ready` — defects don't wait for refinement.
- Picking up work: the team coordinates. `Ready` is the refined queue — bugs excepted, per the line above — and the first choice when it's stocked; pulling from `Backlog` is normal when refinement hasn't caught up — say what you're taking.
- Merging to `develop` moves the card to `On dev` automatically; there is no dev-side review.
- Functional review happens once, on staging: when it passes, comment `/fr-pass` on the PR or drag the card to `Ready for prod`. Self-signoff is allowed.
- `fr-gate` is a required check on promotions. If it blocks, the board or the work isn't ready — fix that. `skip-fr-gate` is audited, for emergencies only.

### Releases & publishing

- The release train is the only path to `staging`, `main`, and every package registry. Never hand-cut a `v*` tag, hand-bump a version file, or publish an artifact — every legal publish path is inventoried in release-train's `PUBLISH-PATHS.md`.
- Findings on a promotion PR are fixed on the source branch (`develop`/`staging`), then the train re-prepares. Never push fixes onto a promotion PR — every push re-rolls its review.

### Filing issues

- Internal work — planning, epics, security findings, infrastructure, anything mentioning a customer — is filed in `backend` (the private catch-all), never in a public repo. When in doubt: `backend`.
- Public repos (`cli`, `client`, `docs`, `data-ingestors`, `model-zoo`, `start-training`, `.github`) only get issues a stranger could act on: about the public artifact itself, with no customer names, internal URLs, or internal paths.

### AI-assisted sessions (Claude Code, etc.)

- An AI session may open PRs and push its own branches. It never: merges a PR, closes another person's PR, deletes another person's branch, or force-pushes — each of those needs an explicit instruction from the human running it.
- If your change makes a statement in any CLAUDE.md, BUGBOT.md, or runbook false, update that file in the same PR.
<!-- org-standards:end -->
