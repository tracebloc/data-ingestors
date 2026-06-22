# Causal Language Modeling Data Ingestion Template

This template demonstrates how to ingest raw text samples for causal (next-token)
language modeling (CLM) into a database using the tracebloc_ingestor framework.

CLM is **self-supervised** — there is no `label:` field. Each sample is a single
`.txt` file holding **raw text**, in one of two shapes:

- **Pretraining:** the whole file is plain text. The training client builds
  next-token targets from it on-the-fly.
- **SFT (instruction tuning):** the file is a single tab-separated
  `prompt<TAB>completion` pair. The client masks the prompt's loss and trains on
  the completion.

A dataset may mix both shapes freely; a file with no tab is treated as plain
text, a file with one tab as a prompt/completion pair.

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
category: causal_language_modeling
table: dolly_clm_train
intent: train
csv: /data/shared/dolly-clm/labels_file.csv
texts: /data/shared/dolly-clm/texts/
```

**3. Install:**

```bash
helm install my-clm-dataset tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

> **If install fails with `'causal_language_modeling' is not one of [...]`:** your
> local Helm chart cache is stale. Refresh it and retry: `helm repo update`.

Canonical example: [`examples/yaml/causal_language_modeling.yaml`](../../examples/yaml/causal_language_modeling.yaml).
Full chart docs: [`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/develop/ingestor/README.md).

## Directory Structure

```
causal_language_modeling/
├── causal_language_modeling.py   # Main ingestion script
├── README.md                     # This file
└── data/
    ├── texts/                    # Text files (.txt), one per sample
    │   ├── clm_0000001.txt       #   plain text (pretraining)
    │   ├── clm_0000002.txt       #   plain text (pretraining)
    │   ├── clm_0000003.txt       #   plain text (pretraining)
    │   ├── clm_0000004.txt       #   prompt<TAB>completion (SFT)
    │   └── clm_0000005.txt       #   prompt<TAB>completion (SFT)
    └── labels_file_sample.csv    # CSV manifest mapping filenames to extensions
```

> The `texts/` subdir is the raw-text convention shared with text/token
> classification. (Masked language modeling instead uses `sequences/`, which the
> framework reserves for *pre-tokenized* data.)

## Data Format

### Text Files
- Supported extension: `.txt`
- One sample per file, placed in `data/texts/`
- Either plain UTF-8 text, or a single `prompt<TAB>completion` line
- Files must be decodable UTF-8 (validated at ingest by `TextContentValidator`);
  binary / non-UTF-8 files are rejected, empty files are warned about.

### CSV Labels File
CLM is **self-supervised** — no label column is needed. The CSV contains:
- `filename`: Base name of the text file, without extension (e.g. `clm_0000001`)
- `extension`: File extension as a quoted string (e.g. `'.txt'`)

## Tokenizer Alignment

CLM does **not** ship a tokenizer at ingest. Instead, the ingestor computes a
data-derived **text profile** (Unicode-script mix + document-length
distribution — no raw text, no vocabulary, no hash; the FL guardrail) and ships
it on the global-metadata channel (#805). The backend uses it for a warn-only
contributor-tokenizer-fit check at dataset linking — the same alignment path as
the other NLP modalities (text/token classification, MLM).

Decoder-only models tie `pad` to `eos`, so — unlike masked language modeling —
**no `[MASK]` token is required**; the tokenizer-fit check looks at vocabulary
coverage and the pad token only. That check runs on the training-client side.

## Usage

1. Place your `.txt` sample files in `data/texts/`
2. Update `labels_file_sample.csv` with one row per text file
3. Configure environment variables (see below)
4. Run the ingestion script:

```bash
python causal_language_modeling.py
```

## Configuration

The script uses the following configuration:
- **Extension**: TXT - Expected text file extension
- **Chunk Size**: 100 - Batch size for CSV reading
- **Encoding**: utf-8
- **Category**: CAUSAL_LANGUAGE_MODELING
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
- No `label` column is needed because CLM training is self-supervised (the
  next-token targets are derived from the text by the training client).
- For SFT samples, put exactly one tab between prompt and completion; everything
  before the first tab is the prompt, everything after is the completion.
