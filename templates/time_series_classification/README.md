# Time-Series Classification Data Ingestion Template

This template demonstrates how to ingest time-series classification data (one multivariate sequence per entity → one label, e.g. per-patient ICU vitals → sepsis outcome) from a CSV file into a database using the tracebloc_ingestor framework.

## Quickstart — declarative (recommended)

Ingest with ~15 lines of YAML using the official ingestor image (`ghcr.io/tracebloc/ingestor`). No Python edits, no Dockerfile to build.

> **Prerequisite:** the chart doesn't transport data into the cluster. Stage your files on the cluster's shared PVC first — see the [data-staging recipe](https://github.com/tracebloc/client/blob/develop/ingestor/README.md#stage-your-data-on-the-shared-pvc) in the chart docs (kubectl cp pattern for small datasets, init-container sync for production).

**1. Stage the CSV** on the shared PVC at `/data/shared/<your-prefix>/<file>.csv`.

**2. Write `ingest.yaml`:**

```yaml
apiVersion: tracebloc.io/v1
kind: IngestConfig
category: time_series_classification
table: icu_vitals_sepsis_train
intent: train
csv: /data/shared/icu-sepsis/vitals.csv
schema:
  sequence_id: VARCHAR(64)
  timestamp: TIMESTAMP
  heart_rate: FLOAT
  resp_rate: FLOAT
  temperature: FLOAT
  sepsis: INT
label: sepsis
```

**3. Install:**

```bash
helm install my-sepsis-dataset tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

The column names `sequence_id` and `timestamp` are **fixed by the platform** — rename your columns before ingest. `label` takes the plain string shorthand (classification-class task, passthrough policy). Canonical example: [`examples/yaml/time_series_classification.yaml`](../../examples/yaml/time_series_classification.yaml). Full chart docs: [`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/develop/ingestor/README.md).

## Directory Structure

```
time_series_classification/
├── time_series_classification.py                              # Main ingestion script
├── time_series_classification_sample_in_csv_format.csv        # Sample data
└── README.md                                                  # This file
```

## Data Format

### CSV File

One row per **timestep**; the rows of one sequence share a `sequence_id` and are ordered by `timestamp` within that sequence. The sample CSV contains hourly ICU vitals for 6 patients (3–7 timesteps each) with the following columns:

- `sequence_id`: The entity whose rows form one sequence — patient / device / session id (VARCHAR). **Fixed name.**
- `timestamp`: Orders the rows *within* each sequence (SQL TIMESTAMP, or a numeric step index like INT hours-since-admission). Must be non-decreasing per sequence; ordering *across* sequences doesn't matter. **Fixed name.**
- `heart_rate`, `resp_rate`, `temperature`, `spo2`, `lactate`: Numeric feature columns. Nulls are allowed (e.g. a lab value not measured at every timestep — `lactate` in the sample).
- `label`: The per-sequence outcome (sepsis: 0 or 1). **Constant within each sequence** — every row of a `sequence_id` repeats the same value.

The script declares a schema covering the sequence, time and feature columns; the label column is excluded from the schema and supplied via `label_column`:

```python
schema = {
    "sequence_id": "VARCHAR(64)",
    "timestamp": "TIMESTAMP",
    "heart_rate": "FLOAT",
    # ... more numeric features
}
label_column = "label"   # the per-sequence outcome
```

## Validation

Beyond the standard checks, `time_series_classification` runs the grouped validator set:

- **Sequence Group Validator** — `sequence_id` present, no null/empty ids, and rejects `data_id: {strategy: column, column: sequence_id}` (the per-row `data_id` is UNIQUE, so mapping it from the sequence id would silently collapse every sequence to one row).
- **Label Constant Within Group Validator** — rejects a label value that changes mid-sequence.
- **Per Group Time Ordered Validator** — timestamps must be monotonic (non-decreasing) *within* each sequence; interleaved sequences are fine.
- **Numeric Columns Validator** — all schema columns except `sequence_id` / `timestamp` must be numeric (nulls allowed).

## Counting Semantics

The ingest summary counts **sequences, not rows**: `labels = {label: number_of_sequences}` and `meta_data.number_of_sequences` carry per-sequence counts (the platform's sample unit for this category is one sequence). Because of this, **one sequence must be fully contained in one ingest run** — don't split a patient's rows across two pushes, or the merged counts will double-count that sequence.

If individual rows fail to insert, the post-insert group-integrity pass removes the affected sequences entirely (a sequence is stored whole or not at all) and the run exits non-zero.

## Usage

1. Replace the sample CSV with your data, or set `LABEL_FILE` to point at your CSV
2. Update the `schema` dict in `time_series_classification.py` to match your feature columns (keep `sequence_id` and `timestamp` — the names are fixed)
3. Confirm `label_column` matches your CSV's outcome column name
4. Run the ingestion script:

```bash
python time_series_classification.py
```

## Configuration

The script uses the following configuration:
- **Chunk Size**: 1000 - Number of records to process in each batch
- **Encoding**: utf-8
- **NA values**: `""`, `"NA"`, `"NULL"`, `"None"` are treated as missing
- **Category**: TIME_SERIES_CLASSIFICATION
- **Data Format**: TABULAR
- **Intent**: TRAIN
- **Sequence column**: `sequence_id` (fixed name)
- **Time column**: `timestamp` (fixed name; TIMESTAMP or numeric step index)
- **Label column**: `label` (the per-sequence outcome)

## Sample Data

The template includes sample data with:
- 6 sequences (ICU stays) of 3–7 hourly timesteps each (30 rows)
- 5 numeric feature columns (vitals; `lactate` has legal nulls)
- 1 per-sequence label column (`label`, 0/1)

## Notes

- The `schema` dict includes `sequence_id` and `timestamp` — from the schema's perspective they are regular columns; the framework recognizes them by their fixed names. Only the label column is excluded from the schema and supplied via `label_column`.
- A composite `(sequence_id, timestamp)` index is created automatically so training-side grouped reads don't full-scan.
- Variable-length sequences are fine at ingest; the training side pads/truncates to the experiment's `sequence_length`.
