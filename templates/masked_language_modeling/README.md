# Masked Language Modeling Data Ingestion Template

This template demonstrates how to ingest pre-tokenized text sequences for masked language modeling (MLM) into a database using the tracebloc_ingestor framework.

## Quickstart — declarative (recommended)

Ingest with ~7 lines of YAML using the official ingestor image (`ghcr.io/tracebloc/ingestor`). No Python edits, no Dockerfile to build.

**1. Stage the data** on your cluster's shared PVC at `/data/shared/<your-prefix>/` with a `sequences/` subdirectory holding the per-record `.txt` sequence files.

**2. Write `ingest.yaml`:**

```yaml
apiVersion: tracebloc.io/v1
kind: IngestConfig
category: masked_language_modeling
table: primekg_mlm_train
intent: train
csv: /data/shared/primekg/labels_file.csv
sequences: /data/shared/primekg/sequences/
```

**3. Install:**

```bash
helm install my-mlm-dataset tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

MLM is unsupervised — no `label:` field; the tokenizer validator checks that sequences match the configured vocabulary. Canonical example: [`examples/yaml/masked_language_modeling.yaml`](../../examples/yaml/masked_language_modeling.yaml). Full chart docs: [`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/main/ingestor/README.md).

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
    └── labels_file_sample.csv    # CSV manifest mapping filenames to extensions
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
- A `tokenizer.json` file should be placed alongside the data on the dataset path. The MLM client will load it automatically. See the preprocessing scripts for tokenizer generation.
