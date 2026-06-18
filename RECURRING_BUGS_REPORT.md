# Recurring-Bug Report — data-ingestors (2026-05-18 → 2026-06-18)

> Scope: bug classes that were fixed, then **reappeared** in a different file / use-case / layer
> within the last month. Built from `git log` on `develop` + the Cursor Bugbot threads.
> The point isn't the individual fixes — it's that the same *class* keeps coming back because
> the fix was applied at one site instead of at the shared seam.

**TL;DR — six bug classes account for ~50 of the month's fixes.** Four are effectively closed;
**two are still live** (Class 1 in 7 validators, Class 6 latent). Each recurs because the
invariant lives in prose, not in one enforced place.

---

## Class 1 — Preflight validator reads data differently than the ingestor  ⚠️ STILL LIVE

**Invariant that keeps breaking:** a validator must read the CSV/JSON with the *exact* same
rules `CSVIngestor`/`JSONIngestor` use, or validation and ingestion disagree
(validate-pass → ingest-fail, or valid data rejected).

The canonical contract: `keep_default_na=False` + `coercion.build_csv_na_values(schema)`,
header `.str.strip()`, case/whitespace-insensitive column resolution, `on_bad_lines="error"`,
string-dtype pin for string columns, **fail-closed** on read errors.

| When | PR(s) | Instance |
|---|---|---|
| 06-08 | #166 | type-convert + strip headers on **every** chunk, not just the first |
| 06-09 | #191 (bugbot #190) | duplicate headers / ragged rows / whitespace-header dtype pin |
| 06-09 | #204 | align per-record JSON validation with CSV |
| 06-15 | #242 | single source of truth for NA policy (`build_csv_na_values`) |
| 06-15 | #262 (#261) | strip whitespace from label values (silent class duplication) |
| 06-15 | #252 | **LabelDiversityValidator — six consecutive bugbot rounds**: fail-closed on read error; partial-read skew; NA rules; stripped-vs-full schema; whitespace headers; pandas-native header parse |
| 06-17 | #292 (#289) | fail fast on missing schema columns |

**Still unfixed (this week's audit), all using bare `pd.read_csv(..., on_bad_lines="warn")`:**
- `BaseValidator._load_data` — `validators/base.py:147` (shared fallback; also swallows read errors → `None` → benign skip)
- `BIOLabelValidator` — case-only column resolve, no NA policy, no header strip
- `TimeFormatValidator` :129 · `TimeOrderedValidator` :78 · `TimeBeforeTodayValidator` :81 · `NumericColumnsValidator` :137 · `TimeToEventValidator` :216

Two concrete sub-bugs still present in 6 validators:
- `on_bad_lines="warn"` (vs ingestor `"error"`) — a ragged row is silently dropped at validation but **errors at ingest**. This *is* the #190 bug, never propagated past `DataValidator`.
- `BaseValidator._load_data` swallows parse errors → "no data" skip while ingest hard-fails. This *is* #252 finding-1, never propagated to the base.

**Why it recurs:** every validator re-implements the read. **Permanent fix:** one shared
`read_for_validation(path, schema)` helper; route all validators through it; one parity test
vs `CSVIngestor`.

---

## Class 2 — NULL / NA / empty-cell semantics  ✅ mostly closed (via Class 1 consolidation)

**Invariant:** null-like tokens (`""`, `NA`, `null`, `None`, `NaN`, JSON `null`) must
round-trip to SQL `NULL` identically across CSV, JSON, every column type, and both the
validator and the ingestor — never become the literal string `"nan"`/`"None"` and never be
counted as a type violation.

| When | PR(s) | Instance |
|---|---|---|
| 06-06 | #150 | stop counting missing values as "non-numeric" |
| 06-08 | #167 | VARCHAR/CHAR/TEXT must tolerate NULL |
| 06-08 | #168 | extend NULL-tolerance to DATE/DATETIME/TIMESTAMP/TIME |
| 06-08 | #170 | JSON `null` tolerate as missing, not a type error |
| 06-08 | #172 | VARCHAR/CHAR empty cells → `None`, not literal `"nan"` |
| 06-08 | #176 | JSON `""` → NULL in `process_record` |
| 06-08 | — | null-like values must round-trip end-to-end as SQL NULL |
| 06-09 | #205 (#195) | tolerate nulls in time-series feature columns |
| 06-15 | #242 (#236/#237) | NA policy + int64 range → single source of truth |

**Why it recurred:** the NA set was redefined per-type and per-layer until `coercion.NA_SENTINELS`
became the single source (#242). Now centralized — but only the layers that *call* it benefit,
which is why Class 1's stragglers still diverge.

---

## Class 3 — Silent half-ingest / wrong success accounting  ✅ closed

**Invariant:** if any record is dropped, any file transfer fails, or backend registration fails,
the run must **not** report success (exit non-zero, surface counts) — a green run must mean a
complete dataset.

| When | PR(s) | Instance |
|---|---|---|
| 05-19 | #100 (#99) | surface file_transfer failures in the ingestion summary |
| 06-09 | #187 | fail loud when backend dataset registration fails |
| 06-11 | #223 | count batch API-send failures as failed records, exit non-zero |
| 06-12 | #230 | propagate hard ingest failures in all modalities; send only inserted records |
| 06-15 | #243 (#234/#235) | dropped records fail the run; JSON read-layer fails fast |
| 06-15 | #265 (#260) | defer table creation until validation passes |
| 06-17 | #279 | regression test: validator-rejected ingest creates no table |

**Why it recurred:** "success" was implicit (no exception thrown) rather than asserted against
the record ledger. Each modality/layer had its own escape hatch. Largely closed by
`IngestionSummary.has_failures` + the deferred-table-creation guard.

---

## Class 4 — Schema-type vocabulary diverges across the 3 layers  ✅ closed, fragile

**Invariant:** the DDL layer (`Database._get_sqlalchemy_type`), the preflight
(`DataValidator.type_validators`), and the cast (`CSVIngestor._validate_csv`) must accept the
**same** SQL-type vocabulary. When one knows a type the others don't, valid schemas are
rejected with "Unknown data type" — or worse, a typo hint suggests a wrong-but-close type.

| When | PR(s) | Instance |
|---|---|---|
| 06-09 | #192 | accept `NUMERIC` (alias for `DECIMAL`) |
| 06-09 | #203 | VARCHAR/CHAR/TEXT accept numeric scalars |
| 06-15 | #249 | `CHAR(N)` must map to a SQLAlchemy type |
| 06-15 | #241 | report real MySQL column types from `get_table_schema` |
| 06-15/17 | #264 / #293 | TINYINT/SMALLINT/MEDIUMINT/BLOB/LONGBLOB + "Did you mean …?" (bugbot caught BLOB→BOOL) |

**Why it recurs:** three hand-maintained type maps. **Permanent fix (not yet done):** derive all
three from one type registry, or a test that asserts the three key-sets are identical.

---

## Class 5 — semantic_segmentation `mask_id` cross-layer indirection  ✅ closed

**Invariant:** `mask_id` is a per-row pointer to a mask **file**, not a DB column; it must reach
`file_transfer` but never the insert — yet must be stored when the schema explicitly declares it.

| When | PR(s) | Instance |
|---|---|---|
| 06-09 | #207 (#196) | honor `_mask` suffix in file pairing |
| 06-09 | #212 | preserve `mask_id` through `process_record` (+ pop before insert) |
| 06-17 | #280 (P5) | move `mask_id` off the DB-bound record (lend-then-strip in `map_file_transfer`) |
| 06-17 | #282 | e2e semseg case + drop the #136 xfail (superseded #277/#285) |

**Why it recurred:** a value that's "data for one layer, metadata for the next" with no single
owner. #280 finally gave it an owner (the transfer lends it from the raw record). Watch for
regressions whenever the record/transfer boundary moves.

---

## Class 6 — Test stub/mock signature drift  ⚠️ LATENT

**Invariant:** when a production function gains a parameter, every test stub standing in for it
must gain it too — otherwise the stub `TypeError`s (swallowed by a `try/except`) and the test
passes **without exercising the real path** (false green).

| When | PR(s) | Instance |
|---|---|---|
| 06-17 | #283 | drop dead `max_retries` param (stubs/ctors lagged) |
| 06-18 | #305 (bugbot #304) | `map_file_transfer` stubs missing `source_record` (and earlier `cfg`) — 3 NLP-profile tests passed without running file transfer |

**Why it recurs:** `map_file_transfer` grew `cfg=` then `source_record=` across P4/P5; stubs are
hand-written lambdas with no link to the real signature. This week's audit found all current
stubs now correct — but the pattern reappears every time that signature changes. **Mitigation:**
prefer `autospec=True` / a shared fake bound to the real signature, or a single
`fake_map_file_transfer` helper.

---

## Adjacent recurring theme — Docs/examples drift from the validator contract  ✅ ongoing

Not a code bug, but the same shape: shipped YAML/README examples that the validators reject.
#131 A-series, #197/#208 (text_classification wrong schema block), #206 (#198/#199 sample
defaults), #200 (timestamp VARCHAR→TIMESTAMP), #224 (token_classification reserved `filename`),
#290/#296 (tsf README). **Mitigation:** the `test_template_equivalence` / e2e parity suites are
the right home; every shipped example should be asserted ingestable.

---

## Recommendation — convert "fix-in-a-loop" into "fix-at-the-seam"

| Class | Status | One-time fix that ends the loop |
|---|---|---|
| 1. Validator read divergence | ⚠️ live (7 sites) | one `read_for_validation(path, schema)` + parity test vs CSVIngestor |
| 2. NULL/NA semantics | ✅ | done (`coercion.NA_SENTINELS`) — finishes when Class 1 routes through it |
| 3. Silent half-ingest | ✅ | `IngestionSummary.has_failures` + deferred table creation |
| 4. Type vocabulary | ✅ fragile | derive 3 maps from 1 registry, or key-set equality test |
| 5. mask_id indirection | ✅ | single owner (transfer lends from raw record) |
| 6. Stub signature drift | ⚠️ latent | `autospec=True` / shared fake for `map_file_transfer` |

**Highest leverage:** Class 1. It is the same bug as Classes 2 and 4 viewed from the read layer,
it is the one that keeps generating bugbot rounds, and it is the only one still actively live in
production code (7 validators). Consolidating the read kills the most recurrence for the least change.
