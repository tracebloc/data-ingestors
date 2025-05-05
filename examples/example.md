# Tracebloc Ingestor Examples

This directory contains example scripts demonstrating how to use the Tracebloc Ingestor package
for various data ingestion scenarios.

## Available Examples

### Basic CSV Ingestion
`basic_csv_ingestion.py` - Demonstrates how to ingest data from a CSV file into a database
and optionally send it to an API.

### Basic JSON Ingestion
`basic_json_ingestion.py` - Shows how to ingest data from a JSON file, handling both
single-object and array-of-objects formats.

### Custom Processor
`custom_processor.py` - Illustrates how to create and use custom processors to transform
data during ingestion.

## Running the Examples

1. Make sure you have installed the package:
```bash
pip install tracebloc_ingestor
```

2. Navigate to the examples directory:
```bash
cd tracebloc_ingestor/examples
```

3. Run an example:
```bash
python csv_ingestor.py
```

## Example Data

The examples use sample data files located in the `data` directory:
- `sample.csv` - Sample CSV data for testing
- `users.json` - Sample JSON data for testing

## Configuration

Before running the examples, make sure to:
1. Set up your database connection in the Config class
2. Configure your API client settings
3. Adjust the schema definitions to match your data structure

## Extending the Examples

You can use these examples as templates for your own ingestion scripts by:
1. Modifying the schema to match your data structure
2. Adding custom processors for data transformation
3. Adjusting the ingestion options (batch size, retries, etc.)
4. Adding error handling and logging specific to your use case 