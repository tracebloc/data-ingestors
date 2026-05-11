# Text Classification Data Ingestion Template

This template demonstrates how to ingest text classification data with `.txt` files and a CSV labels file into a database using the tracebloc_ingestor framework.

## Directory Structure

```
text_classification/
├── text_classification.py      # Main ingestion script
├── README.md                   # This file
└── data/
    ├── texts/                  # Source text files
    │   ├── sample1.txt
    │   ├── sample2.txt
    │   ├── sample3.txt
    │   ├── sample4.txt
    │   └── sample5.txt
    └── labels_file_sample.csv  # CSV mapping each text file to its class label
```

## Data Format

### Text Files
- Supported extension: `.txt`
- One document per file, placed in `data/texts/`
- Each text file must have a corresponding row in the CSV

### CSV Labels File
The CSV contains the following columns:
- `filename`: Base name of the text file, without extension (e.g. `sample1`)
- `extension`: File extension as a quoted string (e.g. `'.txt'`)
- `label`: Class label (e.g. `positive`, `negative`, `neutral`)

## Usage

1. Place your `.txt` documents in `data/texts/`
2. Update `labels_file_sample.csv` with one row per document
3. Configure the ingestion parameters in `text_classification.py`
4. Run the ingestion script:

```bash
python text_classification.py
```

## Configuration

The script uses the following configuration:
- **Extension**: TXT - Expected text file extension
- **Chunk Size**: 100 - Smaller batches to accommodate text processing
- **Encoding**: utf-8
- **Category**: TEXT_CLASSIFICATION
- **Data Format**: TEXT
- **Intent**: TRAIN (change to `Intent.TEST` for evaluation data)
- **Label column**: `label`

## Sample Data

The template includes sample data with:
- 5 text files (`sample1.txt` through `sample5.txt`)
- 3 sentiment classes: `positive`, `negative`, `neutral`
- Product-review-style text content

## Notes

- The `filename` column does **not** include the extension — that's supplied separately via the `extension` column.
- Text files are read with utf-8 encoding by default.
