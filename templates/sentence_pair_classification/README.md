# Sentence-Pair Classification Data Ingestion Template

This template demonstrates how to ingest **sentence-pair classification** data
into a database using the tracebloc_ingestor framework.

sentence_pair_classification is **supervised** — every sample carries a class
`label` in the labels CSV, exactly like `text_classification`. Each sample is a
single `.txt` file under `texts/` holding **raw text** as a tab-separated pair:

- `text_a<TAB>text_b` — two sentences the model encodes together and classifies
  (e.g. a premise and a hypothesis for NLI → `entailment` / `contradiction` /
  `neutral`, or two sentences for a paraphrase / duplicate-question task →
  `paraphrase` / `not_paraphrase`).

This is the same on-disk layout as `text_classification` (one `.txt` per sample
under `texts/`, class label in the CSV) with **one difference**: the `.txt` holds
**two** sentences separated by a single tab. The on-disk shape is therefore
**structured** — a file that is not exactly 2 non-empty tab-separated fields is
**rejected** at ingest by `SentencePairValidator` (no plain prose with no tab, no
empty side, one record per file).

## Quickstart — declarative (recommended)

Ingest with ~9 lines of YAML using the official ingestor image
(`ghcr.io/tracebloc/ingestor`). No Python edits, no Dockerfile to build.

> **Prerequisite:** the chart doesn't transport data into the cluster. Stage
> your files on the cluster's shared PVC first — see the
> [data-staging recipe](https://github.com/tracebloc/client/blob/develop/ingestor/README.md#stage-your-data-on-the-shared-pvc)
> in the chart docs (kubectl cp pattern for small datasets, init-container sync
> for production).

**1. Stage the data** on the shared PVC at `/data/shared/<your-prefix>/` with a
`texts/` subdirectory holding the per-record `.txt` files.

**2. Write `ingest.yaml`** — note the `label:` field (supervised):

```yaml
apiVersion: tracebloc.io/v1
kind: IngestConfig
category: sentence_pair_classification
table: my_sentence_pairs_train
intent: train
csv: /data/shared/my-pairs/labels_file.csv
texts: /data/shared/my-pairs/texts/
label: label
```

**3. Install:**

```bash
helm install my-sentence-pairs-dataset tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

> **If install fails with `'sentence_pair_classification' is not one of [...]`:**
> your local Helm chart cache is stale. Refresh it and retry: `helm repo update`.

## Data layout

```
data/
├── labels_file_sample.csv    # filename, extension, label (one row per sample)
└── texts/
    ├── pair1.txt             # text_a<TAB>text_b
    ├── pair2.txt
    └── ...
```

The labels CSV maps each `filename` to its class `label`:

```csv
filename,extension,label
pair1,'.txt',entailment
pair2,'.txt',contradiction
pair3,'.txt',neutral
```

Each referenced `.txt` is a single tab-separated sentence pair, e.g. `pair1.txt`:

```
A man is playing a guitar.<TAB>A person is making music.
```

## Validation

At ingest the framework rejects, before any row reaches the database:

- **Non-UTF-8 / binary content** (shared `TextContentValidator`).
- **A `.txt` that isn't exactly 2 non-empty tab-separated fields** — plain prose
  with no tab, an empty side, or several records crammed into one file
  (`SentencePairValidator`).
- **A missing / absent `label` column**, or a dataset with fewer than 2 distinct
  labels (`LabelColumnValidator` + `LabelDiversityValidator`).

## Script alternative

To run from Python instead of the declarative YAML, see
[`sentence_pair_classification.py`](sentence_pair_classification.py) — it builds a
`CSVIngestor` with `category=TaskCategory.SENTENCE_PAIR_CLASSIFICATION` and
`label_column="label"`, then calls the shared `run_ingestion` helper (which exits
non-zero on any hard failure — no silent `exit 0`).
