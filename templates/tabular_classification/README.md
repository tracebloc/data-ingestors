# Tabular Classification Data Ingestion Template

This template demonstrates how to ingest tabular classification data from a CSV file into a database using the tracebloc_ingestor framework.

## Directory Structure

```
tabular_classification/
├── tabular_classification.py                          # Main ingestion script
├── tabular_classification_sample_in_csv_format.csv    # Sample data
└── README.md                                          # This file
```

## Data Format

### CSV File
The sample CSV contains the following columns:
- `id`: Unique row identifier
- `feature_00`, `feature_01`, `feature_02`: Numerical feature columns (FLOAT)
- `label`: Class label (e.g. `0` or `1`)

The script defines a schema for **feature columns only**. The label column is configured separately via `label_column`:

```python
schema = {
    "feature_00": "FLOAT",
    "feature_01": "FLOAT",
    "feature_02": "FLOAT",
}
```

## Usage

1. Replace `tabular_classification_sample_in_csv_format.csv` with your data, or set `LABEL_FILE` to point at your CSV
2. Update the `schema` dict in `tabular_classification.py` to match your feature columns
3. Set `label_column` to the name of your target column in the CSV
4. Run the ingestion script:

```bash
python tabular_classification.py
```

## Configuration

The script uses the following configuration:
- **Chunk Size**: 1000 - Number of records to process in each batch
- **Encoding**: utf-8
- **NA values**: `""`, `"NA"`, `"NULL"`, `"None"` are treated as missing
- **Category**: TABULAR_CLASSIFICATION
- **Data Format**: TABULAR
- **Intent**: TRAIN (change to `Intent.TEST` for evaluation data)

## Sample Data

The template includes sample data with:
- 3 numerical features (`feature_00`, `feature_01`, `feature_02`)
- 1 binary label column (`label`)
- Multiple rows of synthetic data

## Notes

- The `schema` dict defines feature columns only — do **not** include the label column in it.
- If you replace the sample CSV with your own data, update `label_column` to match your CSV's target column.
- The framework validates the number of CSV columns against the schema length plus the label column.
