# Token Classification (NER/POS) Data Ingestion Template

This template demonstrates how to ingest token classification data — `.txt` files of whitespace-tokenized words plus a CSV whose `label` column holds the per-token BIO tags — into a database using the tracebloc_ingestor framework.

The on-disk layout is identical to text classification; the only difference is the `label` column: instead of a single class it carries a **space-separated string of BIO/IOB2 tags, one tag per word**.

## Quickstart — declarative (recommended)

Ingest with ~10 lines of YAML using the official ingestor image (`ghcr.io/tracebloc/ingestor`). No Python edits, no Dockerfile to build.

> **Prerequisite:** the chart doesn't transport data into the cluster. Stage your files on the cluster's shared PVC first — see the [data-staging recipe](https://github.com/tracebloc/client/blob/develop/ingestor/README.md#stage-your-data-on-the-shared-pvc) in the chart docs (kubectl cp pattern for small datasets, init-container sync for production).

**1. Stage the data** on the shared PVC at `/data/shared/<your-prefix>/` with a `texts/` subdirectory holding the `.txt` files.

**2. Write `ingest.yaml`:**

```yaml
apiVersion: tracebloc.io/v1
kind: IngestConfig
category: token_classification
table: ner_conll_train
intent: train
csv: /data/shared/ner/labels.csv
texts: /data/shared/ner/texts/
label: label
```

> **Note:** `filename` and the label column are framework-managed (`filename` is reserved by the DB layer) and must **not** be declared in a `schema:` block — doing so fails ingestor init with a reserved-column collision. `schema:` is only for additional feature columns.

**3. Install:**

```bash
helm install my-ner-dataset tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

Canonical example: [`examples/yaml/token_classification.yaml`](../../examples/yaml/token_classification.yaml). Full chart docs: [`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/develop/ingestor/README.md).

## Directory Structure

```
token_classification/
├── token_classification.py     # Main ingestion script
├── README.md                   # This file
└── data/
    ├── texts/                  # Source text files (whitespace-tokenized words)
    │   ├── sample1.txt
    │   ├── sample2.txt
    │   └── sample3.txt
    └── labels_file_sample.csv  # CSV mapping each file to its BIO tag sequence
```

## Data Format

### Text Files
- Supported extension: `.txt`
- One **pre-tokenized** sentence per file: words separated by whitespace (the word boundaries define the tokens that get labeled).
- Each text file must have a corresponding row in the CSV.

### CSV Labels File
- `filename`: Base name of the text file, without extension (e.g. `sample1`)
- `extension`: File extension as a quoted string (e.g. `'.txt'`)
- `label`: **Space-separated BIO/IOB2 tags, one per word** in the `.txt` (e.g. `B-PER I-PER O O B-ORG`)

The number of tags **must equal** the number of whitespace-separated words in the corresponding `.txt`. The ingestor's `BIOLabelValidator` enforces this (and that every tag is `O` or `B-<TYPE>` / `I-<TYPE>`) and rejects the dataset otherwise — catching annotation drift before training instead of failing on the client.

## Usage

1. Place your pre-tokenized `.txt` documents in `data/texts/`
2. Update `labels_file_sample.csv` with one row per document
3. Configure the ingestion parameters in `token_classification.py`
4. Run the ingestion script:

```bash
python token_classification.py
```

## Configuration

- **Extension**: TXT - Expected text file extension
- **Chunk Size**: 100
- **Encoding**: utf-8
- **Category**: TOKEN_CLASSIFICATION
- **Data Format**: TEXT
- **Intent**: TRAIN (change to `Intent.TEST` for evaluation data)
- **Label column**: `label`

## Sample Data

- 3 text files (`sample1.txt` … `sample3.txt`) of pre-tokenized sentences
- BIO tags spanning `PER`, `ORG`, `LOC`, `MISC` entity types
- Each row's tag count matches its file's word count

## Notes

- The `filename` column does **not** include the extension — that's supplied separately via the `extension` column.
- Choose the tokenizer at training time via `tokenizer_id` / `model_id` (defaults to `bert-base-uncased`); the words here define label alignment, and the model's tokenizer handles sub-word splitting.
