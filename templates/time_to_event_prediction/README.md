# Time-to-Event Prediction Data Ingestion Template

This template demonstrates how to ingest time-to-event (survival analysis) data from a CSV file into a database using the tracebloc_ingestor framework.

## Quickstart — declarative (recommended)

Ingest with ~13 lines of YAML using the official ingestor image (`ghcr.io/tracebloc/ingestor`). No Python edits, no Dockerfile to build.

**1. Stage the CSV** on your cluster's shared PVC at `/data/shared/<your-prefix>/<file>.csv`.

**2. Write `ingest.yaml`:**

```yaml
apiVersion: tracebloc.io/v1
kind: IngestConfig
category: time_to_event_prediction
table: customer_churn_survival_train
intent: train
csv: /data/shared/survival/survival.csv
time_column: tenure_days
schema:
  customer_id: VARCHAR(64)
  tenure_days: INT
  plan_tier: VARCHAR(32)
  monthly_spend: FLOAT
  churned: INT
label:
  column: churned
  policy: bucket
```

**3. Install:**

```bash
helm install my-survival-dataset tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

`time_column:` names the column that holds the time-to-event value. `label.policy: bucket` is required so the central backend never sees raw target values. Canonical example: [`examples/yaml/time_to_event_prediction.yaml`](../../examples/yaml/time_to_event_prediction.yaml). Full chart docs: [`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/main/ingestor/README.md).

## Directory Structure

```
time_to_event_prediction/
├── time_to_event_prediction.py                              # Main ingestion script
├── time_to_event_prediction_sample_in_csv_format.csv        # Sample data
└── README.md                                                # This file
```

## Data Format

### CSV File
The sample CSV contains a heart-failure clinical dataset with the following columns:
- `age`: Patient age (FLOAT)
- `anaemia`: Decrease of red blood cells or hemoglobin (0 or 1)
- `creatinine_phosphokinase`: Level of CPK enzyme in the blood (FLOAT)
- `diabetes`: If the patient has diabetes (0 or 1)
- `ejection_fraction`: Percentage of blood leaving the heart at each contraction (FLOAT)
- `high_blood_pressure`: If the patient has hypertension (0 or 1)
- `platelets`: Platelets in the blood (FLOAT)
- `serum_creatinine`: Level of serum creatinine in the blood (FLOAT)
- `serum_sodium`: Level of serum sodium in the blood (FLOAT)
- `sex`: Woman (0) or man (1)
- `smoking`: If the patient smokes or not (0 or 1)
- `time`: Follow-up period in days (INT) — the **time column** for survival analysis
- `DEATH_EVENT`: If the patient died during follow-up (0 or 1) — the **event column**

The script declares a schema covering the feature columns (including `time`) and configures both the time and event columns:

```python
file_options = {
    "number_of_columns": len(schema),
    "schema": schema,
    "time_column": "time",     # follow-up duration
}
label_column = "DEATH_EVENT"   # the event indicator
```

## Usage

1. Replace the sample CSV with your data, or set `LABEL_FILE` to point at your CSV
2. Update the `schema` dict in `time_to_event_prediction.py` to match your feature columns
3. Confirm `time_column` and `label_column` match your CSV's time and event column names
4. Run the ingestion script:

```bash
python time_to_event_prediction.py
```

## Configuration

The script uses the following configuration:
- **Chunk Size**: 1000 - Number of records to process in each batch
- **Encoding**: utf-8
- **NA values**: `""`, `"NA"`, `"NULL"`, `"None"` are treated as missing
- **Category**: TIME_TO_EVENT_PREDICTION
- **Data Format**: TABULAR
- **Intent**: TRAIN
- **Time column**: `time` (follow-up duration in days)
- **Label column**: `DEATH_EVENT` (the event indicator)

## Sample Data

The template includes sample data with:
- 12 feature columns (clinical measurements)
- 1 time column (`time`)
- 1 event column (`DEATH_EVENT`)
- Heart-failure clinical records

## Notes

- The `schema` dict includes the time column (`time`) — from the schema's perspective it's a regular feature column. Only the event column (`DEATH_EVENT`) is excluded from the schema and supplied via `label_column`.
- The framework uses `time_column` and `label_column` together: each row contributes a `(time, event)` pair to the survival data.
