# Image Classification Data Ingestion Template

This template demonstrates how to ingest image classification data with images and a CSV labels file into a database using the tracebloc_ingestor framework.

## Quickstart — declarative (recommended)

Ingest with ~8 lines of YAML using the official ingestor image (`ghcr.io/tracebloc/ingestor`). No Python edits, no Dockerfile to build.

> **Prerequisite:** the chart doesn't transport data into the cluster. Stage your files on the cluster's shared PVC first — see the [data-staging recipe](https://github.com/tracebloc/client/blob/develop/ingestor/README.md#stage-your-data-on-the-shared-pvc) in the chart docs (kubectl cp pattern for small datasets, init-container sync for production).

**1. Stage the data** on the shared PVC at `/data/shared/<your-prefix>/` per the recipe above. The chart mounts this volume into the ingestor pod.

**2. Write `ingest.yaml`:**

```yaml
apiVersion: tracebloc.io/v1
kind: IngestConfig
category: image_classification
table: cats_dogs_train
intent: train
csv: /data/shared/cats-dogs/labels.csv
images: /data/shared/cats-dogs/images/
label: label
```

**3. Install:**

```bash
helm install my-cats-dogs tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

The ingestor validates the data, copies files into the destination directory on the PVC, inserts rows into MySQL, sends metadata to the backend, then exits.

Canonical example: [`examples/yaml/image_classification.yaml`](../../examples/yaml/image_classification.yaml). Full chart docs: [`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/develop/ingestor/README.md).

## Directory Structure

```
image_classification/
├── image_classification.py     # Main ingestion script
├── README.md                   # This file
└── data/
    ├── images/                 # Source image files
    │   ├── cat1.jpeg
    │   ├── cat2.jpeg
    │   ├── cat3.jpeg
    │   ├── dog1.jpeg
    │   ├── dog2.jpeg
    │   └── dog3.jpeg
    └── labels_file_sample.csv  # CSV mapping each image to its class label
```

## Data Format

### Images
- Each dataset uses **one** image extension across all files: `.jpg`, `.jpeg`,
  or `.png`. Mixed extensions in a single dataset are not supported by design —
  `FileTypeValidator` is strict and rejects any file whose extension doesn't
  match the declared one. Set the extension via `spec.file_options.extension`
  in your YAML (default `.jpeg` — see `cli/conventions.py`).
- Images should be placed in the `data/images/` directory
- Each image must have a corresponding row in the CSV file

### CSV Labels File
The CSV contains the following columns:
- `filename`: Image filename (with or without extension — the configured extension is appended if missing). E.g. `cat1.jpeg` or `cat1`.
- `label`: Class label for the image, e.g. `cat`, `dog`

## Advanced: custom processor script

Use the Python+Dockerfile pattern when the declarative schema can't express your processing needs (custom validators, non-standard transforms, etc.). Otherwise prefer the Quickstart above.

1. Place your images in the `data/images/` directory
2. Update `labels_file_sample.csv` with your `(filename, label)` pairs
3. Configure the ingestion parameters in `image_classification.py`
4. Run the ingestion script:

```bash
python image_classification.py
```

## Configuration

The script uses the following configuration:
- **Target Size**: (256, 256) - Images will be resized to this dimension (height = width)
- **Extension**: `.jpeg` — strictly enforced for every image in the dataset.
  Override via `spec.file_options.extension` (one of `.jpg` / `.jpeg` / `.png`).
- **Chunk Size**: 1000 - Number of records to process in each batch
- **Category**: IMAGE_CLASSIFICATION
- **Data Format**: IMAGE
- **Intent**: TEST (change to `Intent.TRAIN` for training data)
- **Label column**: `label`

## Sample Data

The template includes sample data with:
- 6 images (3 cats, 3 dogs)
- 2 classes: `cat`, `dog`
- A CSV with 6 `(filename, label)` rows

## Notes

- The framework validates each image's extension and resizes it to the target size during processing
- Image files are copied to the destination directory; raw data stays on your cluster
- The `intent` field in the script defaults to `Intent.TEST` — set it to `Intent.TRAIN` for training data
