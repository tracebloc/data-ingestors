# Masked Language Modeling Data Ingestion Template

This template demonstrates how to ingest pre-tokenized text sequences for masked language modeling (MLM) into a database using the tracebloc_ingestor framework.

## Quickstart — declarative (recommended)

Ingest with ~7 lines of YAML using the official ingestor image (`ghcr.io/tracebloc/ingestor`). No Python edits, no Dockerfile to build.

> **Prerequisite:** the chart doesn't transport data into the cluster. Stage your files on the cluster's shared PVC first — see the [data-staging recipe](https://github.com/tracebloc/client/blob/develop/ingestor/README.md#stage-your-data-on-the-shared-pvc) in the chart docs (kubectl cp pattern for small datasets, init-container sync for production).

**1. Stage the data** on the shared PVC at `/data/shared/<your-prefix>/` with:
- a `sequences/` subdirectory holding the per-record `.txt` sequence files, and
- **`tokenizer.json` placed in the same folder as your labels CSV** (the prefix root, not inside `sequences/`).

The ingestor auto-discovers `tokenizer.json` from that folder — there is no YAML field for it. See [Tokenizer Requirements](#tokenizer-requirements) for the format rules (must be a HuggingFace tokenizer with `[MASK]` and `[PAD]` in its vocab).

**2. Write `ingest.yaml`** — note there is **no** `tokenizer:` field; the file is discovered automatically:

```yaml
apiVersion: tracebloc.io/v1
kind: IngestConfig
category: masked_language_modeling
table: primekg_mlm_train
intent: train
csv: /data/shared/primekg-mlm/labels_file.csv
sequences: /data/shared/primekg-mlm/sequences/
```

**3. Install:**

```bash
helm install my-mlm-dataset tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

> **If install fails with `'masked_language_modeling' is not one of [...]` or `Additional properties are not allowed ('sequences' was unexpected)`:** this comes from the cluster's `jobs-manager` validating against its own bundled `ingest.v1.json` schema at submit time — the deployed schema is older than the ingestor image you're installing. `helm repo update` won't fix it (that only touches the local chart index). The fix is on the cluster side: upgrade the parent chart so jobs-manager redeploys with the current schema:
>
> ```bash
> helm upgrade <workspace> tracebloc/client \
>   -n <workspace> --reset-then-reuse-values
> ```
>
> (Tracked in [client-runtime#64 / #65](https://github.com/tracebloc/client-runtime/pull/65).)

MLM is unsupervised — no `label:` field; the tokenizer validator checks that sequences match the configured vocabulary. Canonical example: [`examples/yaml/masked_language_modeling.yaml`](../../examples/yaml/masked_language_modeling.yaml). Full chart docs: [`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/develop/ingestor/README.md).

## Directory Structure

```
masked_language_modeling/
├── masked_language_modeling.py   # Main ingestion script
├── README.md                     # This file
└── data/
    ├── sequences/                # Text files (.txt), one per sequence
    │   ├── seq_0000001.txt
    │   ├── seq_0000002.txt
    │   ├── seq_0000003.txt
    │   ├── seq_0000004.txt
    │   └── seq_0000005.txt
    ├── labels_file_sample.csv    # CSV manifest mapping filenames to extensions
    └── tokenizer.json            # REQUIRED — same folder as the labels CSV; HuggingFace tokenizer with [MASK] and [PAD]
```

## Data Format

### Text Files
- Supported extension: `.txt`
- One space-separated token sequence per file, placed in `data/sequences/`
- Each sequence is typically a random walk over a knowledge graph (e.g. PrimeKG)
- Example: `"Lepirudin indication Huntington phenotype_present Chorea"`

### CSV Labels File
MLM is **self-supervised** — no label column is needed. The CSV contains:
- `filename`: Base name of the text file, without extension (e.g. `seq_0000001`)
- `extension`: File extension as a quoted string (e.g. `'.txt'`)

## Tokenizer Requirements

MLM ingestion requires a `tokenizer.json` placed **in the same folder as your labels CSV** (the dataset prefix root — not inside `sequences/`). The ingestor copies it once from there ([`file_transfer.py:369`](../../tracebloc_ingestor/file_transfer.py)), and the training client loads it automatically.

Hard rules (enforced — failing any of these blocks ingestion or training):

- Must be a valid HuggingFace `tokenizer.json`, loadable via `tokenizers.Tokenizer.from_file()`.
- Must contain **both `[MASK]` and `[PAD]`** in its vocabulary (`model.vocab` or `added_tokens`).
- These tokens **cannot be added dynamically** — doing so would create token IDs beyond the model's embedding size (`vocab_size`) and crash training with an `IndexError`.

Validation runs twice:
1. **At ingestion** — [`TokenizerValidator`](../../tracebloc_ingestor/validators/tokenizer_validator.py) checks the file exists, parses as JSON, and contains the required tokens.
2. **At training load** — the MLM client re-checks `mask_token`/`pad_token` and raises a `ValueError` if either is missing.

### Troubleshooting validation failures

| Error | Cause | Fix |
|-------|-------|-----|
| `tokenizer.json not found` | File missing or in the wrong folder | Place `tokenizer.json` in the same folder as your labels CSV |
| `Invalid JSON in tokenizer.json` | Corrupt / non-JSON file | Regenerate the tokenizer; verify it loads with `Tokenizer.from_file()` |
| `Tokenizer is missing required special tokens: [MASK], [PAD]` | Tokens absent from vocab | Add `[MASK]` and `[PAD]` to the vocabulary **before** saving the tokenizer |

## Preprocessing

The text sequences are generated from a knowledge graph using the preprocessing scripts in the `tracebloc-client` repo:

```bash
# Step 1: Download and reformat PrimeKG
python prep_primekg.py --output-dir ./primekg_data

# Step 2: Generate random walk corpus
python graph_to_corpus.py \
    --nodes primekg_data/nodes.csv \
    --edges primekg_data/edges.csv \
    --output-dir ./output \
    --walk-length 5 \
    --walks-per-node 30 \
    --seed 42
```

This produces the `labels_file.csv` + `sequences/` directory consumed by this ingestor.

## Usage

1. Place your `.txt` sequence files in `data/sequences/`
2. Update `labels_file_sample.csv` with one row per sequence file
3. Configure environment variables (see below)
4. Run the ingestion script:

```bash
python masked_language_modeling.py
```

## Configuration

The script uses the following configuration:
- **Extension**: TXT - Expected text file extension
- **Chunk Size**: 100 - Batch size for CSV reading
- **Encoding**: utf-8
- **Category**: MASKED_LANGUAGE_MODELING
- **Data Format**: TEXT
- **Intent**: TRAIN (change to `Intent.TEST` for evaluation data)
- **Label column**: None (self-supervised)

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TABLE_NAME` | Target database table | Required |
| `LABEL_FILE` | Path to CSV manifest | Required |
| `SRC_PATH` | Path to sequences directory | Required |
| `BATCH_SIZE` | Ingestion batch size | 4000 |
| `BACKEND_TOKEN` | Auth token for API | Required |
| `CLIENT_ENV` | Environment (local/dev/stg/prod) | prod |

## Notes

- The `filename` column does **not** include the extension — that's supplied separately via the `extension` column.
- No `label` column is needed because MLM training is self-supervised (masking is applied on-the-fly by the training client).
- `tokenizer.json` must sit **in the same folder as your labels CSV** (not inside `sequences/`). See [Tokenizer Requirements](#tokenizer-requirements).
- Generate the tokenizer with the preprocessing scripts in the `tracebloc-client` repo. After generating, confirm the special tokens are present:

  ```python
  from tokenizers import Tokenizer
  tok = Tokenizer.from_file("tokenizer.json")
  vocab = tok.get_vocab()
  assert "[MASK]" in vocab and "[PAD]" in vocab, "Missing required special tokens"
  ```
