# Sequence-to-Sequence (seq2seq) Data Ingestion Template

This template demonstrates how to ingest raw text samples for sequence-to-sequence
(encoder-decoder) modeling into a database using the tracebloc_ingestor framework.

seq2seq is **self-supervised** — there is no `label:` field. Each sample is a
single `.txt` file holding **raw text**:

- A single tab-separated `source<TAB>target` pair. The training client feeds the
  source to the encoder and trains the decoder to produce the target (e.g.
  translation, summarization, paraphrase).

This is the same on-disk shape as causal language modeling's
`prompt<TAB>completion` pair — one `.txt` per sample under `texts/` — so it
reuses the same raw-text ingestion path.

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
category: seq2seq
table: wmt_seq2seq_train
intent: train
csv: /data/shared/wmt-seq2seq/labels_file.csv
texts: /data/shared/wmt-seq2seq/texts/
```

**3. Install:**

```bash
helm install my-seq2seq-dataset tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

> **If install fails with `'seq2seq' is not one of [...]`:** your local Helm
> chart cache is stale. Refresh it and retry: `helm repo update`.

Canonical example: [`examples/yaml/seq2seq.yaml`](../../examples/yaml/seq2seq.yaml).
Full chart docs: [`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/develop/ingestor/README.md).

## Directory Structure

```
seq2seq/
├── seq2seq.py                    # Main ingestion script
├── README.md                     # This file
└── data/
    ├── texts/                    # Text files (.txt), one per sample
    │   ├── s2s_0000001.txt       #   source<TAB>target (translation)
    │   ├── s2s_0000002.txt       #   source<TAB>target (translation)
    │   ├── s2s_0000003.txt       #   source<TAB>target (summarization)
    │   ├── s2s_0000004.txt       #   source<TAB>target (translation)
    │   └── s2s_0000005.txt       #   source<TAB>target (paraphrase)
    └── labels_file_sample.csv    # CSV manifest mapping filenames to extensions
```

> The `texts/` subdir is the raw-text convention shared with text/token
> classification and causal language modeling. (Masked language modeling instead
> uses `sequences/`, which the framework reserves for *pre-tokenized* data.)

## Data Format

### Text Files
- Supported extension: `.txt`
- One sample per file, placed in `data/texts/`
- A single `source<TAB>target` line
- Files must be decodable UTF-8 (validated at ingest by `TextContentValidator`);
  binary / non-UTF-8 files are rejected, empty files are warned about.

### CSV Labels File
seq2seq is **self-supervised** — no label column is needed. The CSV contains:
- `filename`: Base name of the text file, without extension (e.g. `s2s_0000001`)
- `extension`: File extension as a quoted string (e.g. `'.txt'`)

## Tokenizer Alignment

seq2seq does **not** ship a tokenizer at ingest. Instead, the ingestor computes a
data-derived **text profile** (Unicode-script mix + document-length
distribution — no raw text, no vocabulary, no hash; the FL guardrail) and ships
it on the global-metadata channel (#805). The backend uses it for a warn-only
contributor-tokenizer-fit check at dataset linking — the same alignment path as
the other NLP modalities (text/token classification, MLM, causal LM). That check
runs on the training-client side.

## Usage

1. Place your `.txt` sample files in `data/texts/`
2. Update `labels_file_sample.csv` with one row per text file
3. Configure environment variables (see below)
4. Run the ingestion script:

```bash
python seq2seq.py
```

## Configuration

The script uses the following configuration:
- **Extension**: TXT - Expected text file extension
- **Chunk Size**: 100 - Batch size for CSV reading
- **Encoding**: utf-8
- **Category**: SEQ2SEQ
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
- No `label` column is needed because seq2seq training is self-supervised (the
  target side of each pair is the supervision signal, derived from the text by
  the training client).
- Put exactly one tab between source and target; everything before the first tab
  is the source, everything after is the target.
