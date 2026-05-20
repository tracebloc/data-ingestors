# Tabular Regression Data Ingestion Template

This template demonstrates how to ingest tabular regression data from a CSV file into a database using the tracebloc_ingestor framework.

## Quickstart — declarative (recommended)

Ingest with ~13 lines of YAML using the official ingestor image (`ghcr.io/tracebloc/ingestor`). No Python edits, no Dockerfile to build.

**1. Stage the CSV** on your cluster's shared PVC at `/data/shared/<your-prefix>/<file>.csv`.

**2. Write `ingest.yaml`:**

```yaml
apiVersion: tracebloc.io/v1
kind: IngestConfig
category: tabular_regression
table: house_prices_train
intent: train
csv: /data/shared/houses/houses.csv
schema:
  square_feet: FLOAT
  bedrooms: INT
  bathrooms: FLOAT
  age_years: INT
  price: FLOAT
label:
  column: price
  policy: bucket
```

**3. Install:**

```bash
helm install my-regression-dataset tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

Regression-class tasks require an explicit `label.policy` (typically `bucket`) so the central backend never sees raw target values — the schema enforces this. Canonical example: [`examples/yaml/tabular_regression.yaml`](../../examples/yaml/tabular_regression.yaml). Full chart docs: [`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/main/ingestor/README.md).

## Directory Structure

```
tabular_regression/
├── tabular_regression.py                              # Main ingestion script
├── tabular_regression_sample_in_csv_format.csv        # Sample data
└── README.md                                          # This file
```

## Data Format

### CSV File
The sample CSV contains the following columns:
- `id`: Unique row identifier
- `square_feet`: Floor area in square feet (FLOAT)
- `bedrooms`: Number of bedrooms (INT)
- `age`: Age of the property in years (INT)
- `price`: Sale price — the regression target (FLOAT)

The script defines a schema for **feature columns only**. The label column is configured separately via `label_column`:

```python
schema = {
    "square_feet": "FLOAT",
    "bedrooms": "INT",
    "age": "INT",
}
```

## Usage

1. Replace `tabular_regression_sample_in_csv_format.csv` with your data, or set `LABEL_FILE` to point at your CSV
2. Update the `schema` dict in `tabular_regression.py` to match your feature columns
3. Set `label_column` to the name of your continuous target column
4. Run the ingestion script:

```bash
python tabular_regression.py
```

## Configuration

The script uses the following configuration:
- **Chunk Size**: 1000 - Number of records to process in each batch
- **Encoding**: utf-8
- **NA values**: `""`, `"NA"`, `"NULL"`, `"None"` are treated as missing
- **Category**: TABULAR_REGRESSION
- **Data Format**: TABULAR
- **Intent**: TRAIN (change to `Intent.TEST` for evaluation data)
- **Label column**: `price`

## Sample Data

The template includes sample data with:
- 3 feature columns (`square_feet`, `bedrooms`, `age`)
- 1 continuous target (`price`)
- Synthetic housing-price rows

## Notes

- The `schema` dict defines feature columns only. The label column (`price`) is supplied via `label_column`, not via the schema.
- The framework validates the number of CSV columns against the schema length plus the label column.
