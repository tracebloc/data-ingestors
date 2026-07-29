# Bugbot guide — tracebloc/data-ingestors

## Context

Public Python package (`tracebloc_ingestor` on PyPI) plus a signed GHCR image. It validates,
preprocesses, and ingests datasets (image, text, tabular, time series — 16 task categories)
inside a customer's Kubernetes environment: raw data stays on-premise, only metadata syncs
to the tracebloc backend.

Three properties shape most real defects here:

1. **It runs inside the customer's cluster, on the customer's data.** Anything written to a
   log, error message, or API payload can egress via install logs and failure reports —
   redaction is a product guarantee, not cosmetics (`tracebloc_ingestor/utils/redaction.py`).
2. **It deletes and mutates shared storage.** The MySQL tables and the shared PVC hold the
   customer's staged data; a wrong `rmtree` or row purge destroys data tracebloc does not
   own.
3. **Validation is a promise about ingest.** Preflight validators exist so a dataset that
   passes dry-run also ingests and trains; any drift between what a validator reads and what
   the ingestor reads breaks that promise in one direction or the other.

Release flow: this repo rides the org release train `develop → staging → master`. A develop
push publishes a dev pre-release (`publish-dev.yml`), a master push publishes the package
(`publish-master.yml`), and a `vX.Y.Z` tag builds the signed multi-arch image + GitHub
Release (`release-image.yml`). Package and image release independently; `RELEASING.md`
documents how they stay aligned.

## Always flag

- **A cell value that can reach a log, error message, or the wire.** The policy line
  (`utils/redaction.py` docstring): cell VALUES never appear in errors/logs; column names,
  file names, counts, dtypes, and row indices may. The two recurring leak shapes are
  interpolating a raw exception — `str(exc)` / `logger.exception`; MySQL driver errors embed
  bound parameter values — and echoing an HTTP `response.text` whose request body carried
  vocab or stats. The house pattern logs `type(exc).__name__` only, with a "message
  suppressed" note (`ingestors/csv_ingestor.py:508`,
  `validators/per_group_time_ordered_validator.py:219`, `metadata_backfill_runner.py:195`).

- **A validator that reads data differently from the ingestor.** Preflight readers must
  honor the run's `csv_options` via `utils/csv_dialect.read_dialect_kwargs` — a hardcoded
  `pd.read_csv(..., sep=",", encoding="utf-8")` passes or rejects manifests the real ingest
  treats the opposite way (#371 → #376, #384). Same class: a contract enforced only on the
  `.csv` path while `json_ingestor.py` accepts the same manifest shape, so the JSON dataset
  fails late, cluster-side (#384); and column-name comparisons that skip the case- and
  whitespace-insensitive resolution the read path uses (`utils/columns.resolve_column`, #367).

- **Config validated mid-scan instead of at construction** (learned rule). A bad YAML value
  must fail loudly in the validator's `__init__`, not raise inside the per-file loop where
  it gets caught and misreported as corrupt data (#364, #365, #376).

- **A new key on `meta_data` that isn't consciously wire-facing.** Internal bridges hung on
  `file_options`/`meta_data` for combine-time plumbing must be listed in
  `_META_DATA_INTERNAL_KEYS` so `_meta_data_payload()` strips them before send
  (`ingestors/base.py:96,793`); the canonical copies ship under `attributes` only. This leak
  class recurred three times in one release (#383 → fixed in #385, #387).

- **`feature_stats` / categorical vocab accumulated outside the tabular family.** Stats
  accumulation gates on `TABULAR_FAMILY_CATEGORIES` (`modalities/registry.py`) because a
  manifest category's TEXT column (e.g. keypoint `Visibility` JSON) under the cardinality
  cap would ship raw cell values as vocabulary. The gate must hold on *every* emit path —
  live ingest and `metadata_backfill.py` alike (#385, #389).

- **A filesystem delete that can't prove the target is ours.** `reclaim_source`
  (`file_transfer.py:519`) is the reference: opt-in exact `realpath` match on
  `STORAGE_PATH/.tracebloc-staging/<TABLE_NAME>`, never a blocklist; symlinks resolved
  before every check and before the delete (PVC mounts are symlinks); refuses `/`, the PVC
  root, sibling staging dirs, and anything overlapping the table dir. #381 collected four
  findings against the first draft of exactly this function.

- **Anything that can make the run journal lie about registration.**
  `reclaim_dead_run_rows` (`database.py:808`) purges rows of runs the journal says never
  registered — so a swallowed `mark_ingest_registered` failure (`database.py:758`) turns an
  already-registered dataset into the next run's "orphan" and deletes its rows (#371).
  Journal writes that follow a successful registration must not fail silently.

- **A `data_id` semantics change that collapses or duplicates rows.** `data_id` has a
  UNIQUE upsert. The default strategy is `content_hash` (#350) *except* object detection,
  which stays `uuid`: objdet manifests list one row per object, duplicate
  `(filename, label)` rows are the norm, and content-hashing them silently under-counts
  objects (`cli/conventions.py:293-322`; #383 → #388). Flag any new category or code path
  that changes `data_id` behavior without reasoning about row collapse vs retry
  re-insertion.

- **Reusing a SQLAlchemy connection after a failed statement without a rollback.** The next
  execute raises `PendingRollbackError` and masks the intended handling (#386; same class
  as #219's `_execute_with_retry`). Relatedly, sweep loops (e.g. backfill discovery) need a
  per-item guard — one bad table logged (type only) and skipped, never aborting the whole
  rollout (#395).

- **A release step that self-creates a `v*` tag, or gates "done" on the tag.** `v*` tags
  are ruleset-protected (org admins + release-managers only) — a workflow tagging under
  `github-actions` 403s on its first real run (#402); tags are cut by the release train's
  prod hop. Idempotency checks must gate on the actual artifact (GitHub Release / signed
  image), not tag existence (#402). The version is single-sourced in
  `tracebloc_ingestor/__init__.py` (`setup.py` parses it; the two drifted once — #171/#175),
  and every publisher keeps the `_load_schema()` smoke probe (the v0.3.0-rc1
  missing-schema bug).

- **Validator UX regressions on wide manifests** (learned rules). Column lists in messages
  are capped via `redaction.column_preview` with the full set kept in result *metadata* for
  programmatic consumers (#371, #384); log labels derive from `validator.name` normalized
  at the log site — names do not all end in "Validator" (#396 → #397).

## Known non-issues — do not flag

- **`reclaim_source` swallowing every exception is deliberate.** It runs only after the
  load is durable (rows committed + dataset registered); a reclaim hiccup must never fail a
  successful ingest (`file_transfer.py:519` docstring; call site
  `ingestors/base.py:1303-1310`). Best-effort here was reviewed and kept on purpose.
- **`type(e).__name__` with "message suppressed" is not information loss** — it is the
  redaction policy above. Don't suggest logging the exception message or full traceback on
  paths that reach install logs.
- **Label values in `LabelDiversityValidator` diagnostics are exempt by policy** — labels
  egress by design in the ingest summary's label counts (`utils/redaction.py` docstring).
- **Object detection defaulting to `uuid` while everything else defaults to `content_hash`
  is intentional**, documented at `cli/conventions.py:310` with the row-collapse rationale.
  The known cost (a retried objdet run re-inserts rows) is accepted; the row-ordinal-salted
  hash is a tracked follow-up, not a bug in the default.
- **`\x27` inside a *Python* `re` pattern is a valid escape for `'`** — verified false
  positive on #407. The "broken escape" claim applies to POSIX grep/sed character classes,
  not Python's `re` engine.
- **The e2e suite drives a real MySQL service container** (`e2e/`, `e2e.yml`) while the
  unit suite mocks DB + API (`tests.yml`) — the split is by design, and the
  characterization e2e pins `data_id: uuid` where content-hash would collapse its repeated
  fixture rows (`e2e/test_characterization.py:107,215`).
- `debug_csv_processing.py` at the repo root is a tracked debugging scratch script — only
  `tracebloc_ingestor/` ships in the package.

## Tone

Direct. Name the file and line. Give a concrete fix, not "consider". Lead with the
customer-visible consequence — what egresses, what gets deleted, which dataset silently
mis-ingests.

This repo is **public**: never put a customer name, internal hostname, or internal-only
ticket detail in a finding. A bare `tracebloc/backend#NNNN` reference is fine.

## Working with Bugbot findings (team norm)

Every Bugbot review thread gets a reply, then gets resolved:
- **Fixed**: say what changed and in which commit.
- **False positive**: say why, with evidence (file/line, measured behavior).
Unresolved cursor threads HOLD release-train promotions (soft gate) — an
unaddressed finding blocks the fleet, not just this PR.
