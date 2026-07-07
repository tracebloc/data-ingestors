# Embeddings (Self-Supervised Contrastive) Data Ingestion Template

This template demonstrates how to ingest raw text samples for self-supervised
contrastive **embedding** training into a database using the
tracebloc_ingestor framework.

embeddings is **self-supervised** — there is no `label:` field. Each sample is a
single `.txt` file holding **raw text** as a tab-separated record:

- A **pair** — `anchor<TAB>positive` — two texts that should embed close
  together (e.g. a sentence and its paraphrase, a question and a matching
  passage), **or**
- A **triplet** — `anchor<TAB>positive<TAB>negative` — additionally carrying a
  hard negative that should embed far from the anchor.

The training client builds a contrastive objective (e.g. InfoNCE / triplet loss)
from these views on-the-fly.

This is the same raw-text ingestion path as causal language modeling /
seq2seq — one `.txt` per sample under `texts/` — but the on-disk shape is
**structured**: a file that is not exactly 2 or 3 non-empty tab-separated fields
is **rejected** at ingest by `ContrastivePairsValidator` (no plain prose, no
empty fields, one record per file).

## Quickstart — declarative (recommended)

Ingest with ~8 lines of YAML using the official ingestor image
(`ghcr.io/tracebloc/ingestor`). No Python edits, no Dockerfile to build.

> **Prerequisite:** the chart doesn't transport data into the cluster. Stage
> your files on the cluster's shared PVC first — see the
> [data-staging recipe](https://github.com/tracebloc/client/blob/develop/ingestor/README.md#stage-your-data-on-the-shared-pvc)
> in the chart docs (kubectl cp pattern for small datasets, init-container sync
> for production).

**1. Stage the data** on the shared PVC at `/data/shared/<your-prefix>/` with a
`texts/` subdirectory holding the per-record `.txt` files.

**2. Write `ingest.yaml`** — note there is **no** `label:` field (self-supervised):

```yaml
apiVersion: tracebloc.io/v1
kind: IngestConfig
category: embeddings
table: my_embeddings_train
intent: train
csv: /data/shared/my-embeddings/labels_file.csv
texts: /data/shared/my-embeddings/texts/
```

**3. Install:**

```bash
helm install my-embeddings-dataset tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

> **If install fails with `'embeddings' is not one of [...]`:** your local Helm
> chart cache is stale. Refresh it and retry: `helm repo update`.

Canonical example: [`examples/yaml/embeddings.yaml`](../../examples/yaml/embeddings.yaml).
Full chart docs: [`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/develop/ingestor/README.md).

## Directory Structure

```
embeddings/
├── embeddings.py                 # Main ingestion script
├── README.md                     # This file
└── data/
    ├── texts/                    # Text files (.txt), one per sample
    │   ├── emb_0000001.txt       #   anchor<TAB>positive (pair)
    │   ├── emb_0000002.txt       #   anchor<TAB>positive (pair)
    │   ├── emb_0000003.txt       #   anchor<TAB>positive (pair)
    │   ├── emb_0000004.txt       #   anchor<TAB>positive<TAB>negative (triplet)
    │   └── emb_0000005.txt       #   anchor<TAB>positive<TAB>negative (triplet)
    └── labels_file_sample.csv    # CSV manifest mapping filenames to extensions
```

> The `texts/` subdir is the raw-text convention shared with text/token
> classification, causal language modeling and seq2seq. (Masked language
> modeling instead uses `sequences/`, which the framework reserves for
> *pre-tokenized* data.)

## Data Format

### Text Files
- Supported extension: `.txt`
- One sample per file, placed in `data/texts/`
- A single tab-separated `anchor<TAB>positive` (pair) **or**
  `anchor<TAB>positive<TAB>negative` (triplet) line. Pairs and triplets may be
  mixed within one dataset.
- Files must be decodable UTF-8 (validated at ingest by `TextContentValidator`)
  **and** structurally valid (validated by `ContrastivePairsValidator`): exactly
  2 or 3 non-empty tab fields, one record per file. Binary / non-UTF-8 files,
  plain prose without a tab, empty fields, and multi-line files are rejected.

### CSV Labels File
embeddings is **self-supervised** — no label column is needed. The CSV contains:
- `filename`: Base name of the text file, without extension (e.g. `emb_0000001`)
- `extension`: File extension as a quoted string (e.g. `'.txt'`)

## Tokenizer Alignment

embeddings does **not** ship a tokenizer at ingest. Instead, the ingestor
computes a data-derived **text profile** (Unicode-script mix + document-length
distribution — no raw text, no vocabulary, no hash; the FL guardrail) and ships
it on the global-metadata channel (#805). The backend uses it for a warn-only
contributor-tokenizer-fit check at dataset linking — the same alignment path as
the other NLP modalities (text/token classification, MLM, causal LM, seq2seq).
That check runs on the training-client side.

## Usage

1. Place your `.txt` sample files in `data/texts/`
2. Update `labels_file_sample.csv` with one row per text file
3. Configure environment variables (see below)
4. Run the ingestion script:

```bash
python embeddings.py
```

## Configuration

The script uses the following configuration:
- **Extension**: TXT - Expected text file extension
- **Chunk Size**: 100 - Batch size for CSV reading
- **Encoding**: utf-8
- **Category**: EMBEDDINGS
- **Data Format**: TEXT
- **Intent**: TRAIN (change to `Intent.TEST` for evaluation data)
- **Label column**: None (self-supervised)

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TABLE_NAME` | Target database table | Required |
| `LABEL_FILE` | Path to CSV manifest | Required |
| `SRC_PATH` | Path to the parent of the `texts/` directory | Required |
| `BATCH_SIZE` | Ingestion batch size | 4000 |
| `BACKEND_TOKEN` | Auth token for API | Required |
| `CLIENT_ENV` | Environment (local/dev/stg/prod) | prod |

## Notes

- The `filename` column does **not** include the extension — that's supplied
  separately via the `extension` column.
- No `label` column is needed because embeddings training is self-supervised
  (the pairing itself is the supervision signal, derived from the text by the
  training client).
- Put exactly one tab between each field. For a pair: everything before the tab
  is the anchor, everything after is the positive. For a triplet: anchor, then
  positive, then the hard negative.
