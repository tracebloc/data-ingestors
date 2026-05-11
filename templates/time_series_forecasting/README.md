# Time Series Forecasting Data Ingestion Template

This template demonstrates how to ingest time series forecasting data from a CSV file into a database using the tracebloc_ingestor framework.

## Directory Structure

```
time_series_forecasting/
├── time_series_forecasting.py                              # Main ingestion script
├── time_series_forecasting_sample_in_csv_format.csv        # Sample data
└── README.md                                               # This file
```

## Data Format

### CSV File
The sample CSV contains the following columns:
- `timestamp`: ISO date (YYYY-MM-DD) — required column for time series ingestion
- `day_of_week`, `month`, `day_of_month`, `week_of_year`, `is_weekend`: Calendar features (INT)
- `lag_1`: Previous timestep's value (FLOAT; blank for the first row)
- `moving_avg_7`: 7-day moving average (FLOAT; blank until enough history accumulates)
- `value`: The forecasting target (FLOAT)

The script defines a schema for **feature columns only**. The label column is configured separately via `label_column`.

## Usage

1. Replace `time_series_forecasting_sample_in_csv_format.csv` with your data, or set `LABEL_FILE` to point at your CSV
2. Update the `schema` dict in `time_series_forecasting.py` to match your feature columns
3. Set `label_column` to the name of your target column
4. Run the ingestion script:

```bash
python time_series_forecasting.py
```

## Configuration

The script uses the following configuration:
- **Chunk Size**: 1000 - Number of records to process in each batch
- **Encoding**: utf-8
- **NA values**: `""`, `"NA"`, `"NULL"`, `"None"` are treated as missing
- **Category**: TIME_SERIES_FORECASTING
- **Data Format**: TABULAR
- **Intent**: TRAIN

## Sample Data

The template includes sample data with:
- Daily timestamps starting from 2023-10-01
- Calendar features + lag and moving-average features
- A `value` column as the forecasting target

## Notes

- The `schema` dict defines feature columns only. The label column is supplied via `label_column`.
- A column named `timestamp` is required for time series ingestion — keep this name even if you replace the sample CSV with your own data.
- If you replace the sample CSV with your own data, update the `schema` dict and `label_column` in the script to match your CSV's columns.
- Time series ingestion treats each row as a timestep; ensure your CSV is sorted chronologically.
