# Keypoint Detection Data Ingestion Template

This template demonstrates how to ingest keypoint detection data with images and JSON-based keypoint annotations into a database using the tracebloc_ingestor framework.

## Quickstart — declarative (recommended)

Ingest with ~8 lines of YAML using the official ingestor image (`ghcr.io/tracebloc/ingestor`). No Python edits, no Dockerfile to build.

> **Prerequisite:** the chart doesn't transport data into the cluster. Stage your files on the cluster's shared PVC first — see the [data-staging recipe](https://github.com/tracebloc/client/blob/develop/ingestor/README.md#stage-your-data-on-the-shared-pvc) in the chart docs (kubectl cp pattern for small datasets, init-container sync for production).

**1. Stage the data** on the shared PVC at `/data/shared/<your-prefix>/` with an `images/` subdirectory; keypoint annotations are columns in the CSV.

**2. Write `ingest.yaml`:**

```yaml
apiVersion: tracebloc.io/v1
kind: IngestConfig
category: keypoint_detection
table: pose_train
intent: train
csv: /data/shared/pose/labels.csv
images: /data/shared/pose/images/
label: image_label
```

**3. Install:**

```bash
helm install my-keypoint-dataset tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

Canonical example: [`examples/yaml/keypoint_detection.yaml`](../../examples/yaml/keypoint_detection.yaml). Full chart docs: [`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/develop/ingestor/README.md).

## Directory Structure

```
keypoint_detection/
├── keypoint_detection.py        # Main ingestion script
├── README.md                    # This file
└── data/
    ├── images/                  # Source image files
    │   ├── person_001.jpg
    │   ├── person_002.jpg
    │   └── person_003.jpg
    └── labels_file_sample.csv   # CSV file with keypoint annotations
```

## Data Format

### Images
- Supported formats: PNG, JPEG, JPG
- Images should be placed in the `data/images/` directory
- Each image must have a corresponding row in the CSV file

### CSV Labels File
The CSV file contains the following columns:
- `filename`: Name of the image file (without extension)
- `Annotation`: JSON string with keypoint coordinates as `{"joint_name": [x, y]}` pairs
- `Visibility`: JSON string with per-keypoint visibility flags as `{"joint_name": 0|1}` (1=visible, 0=occluded/out-of-frame)
- `image_label`: Class label for the image

### Annotation Format

Each `Annotation` value is a JSON object mapping joint names to `[x, y]` coordinates:
```json
{
    "nose": [128, 50],
    "left_eye": [118, 42],
    "right_eye": [138, 42],
    "left_shoulder": [95, 100],
    "right_shoulder": [160, 100],
    "left_elbow": [80, 150],
    "right_elbow": [175, 150],
    "left_wrist": [70, 195],
    "right_wrist": [185, 195]
}
```

### Visibility Format

Each `Visibility` value is a JSON object mapping joint names to visibility flags:
```json
{
    "nose": 1,
    "left_eye": 1,
    "right_eye": 1,
    "left_shoulder": 1,
    "right_shoulder": 0
}
```
- `1` = keypoint is visible
- `0` = keypoint is occluded or out of frame

## Usage

1. Place your images in the `data/images/` directory
2. Update the `labels_file_sample.csv` with your keypoint annotations
3. Configure the ingestion parameters in `keypoint_detection.py`
4. Run the ingestion script:

```bash
python keypoint_detection.py
```

## Configuration

The script uses the following configuration:
- **Target Size**: (448, 448) - Images will be resized to this dimension
- **Extension**: JPG - Expected image file extension
- **Chunk Size**: 100 - Number of records to process in each batch
- **Number of Keypoints**: 9 - Must match the number of keypoints in each Annotation JSON
- **Category**: KEYPOINT_DETECTION
- **Data Format**: IMAGE
- **Intent**: TRAIN

### Keypoint Definition

The `keypoints` list in the script defines the expected keypoints for the dataset. Update this list to match your data:

```python
keypoints = [
    "nose",
    "left_eye",
    "right_eye",
    "left_shoulder",
    "right_shoulder",
    "left_elbow",
    "right_elbow",
    "left_wrist",
    "right_wrist",
]
```

The `number_of_keypoints` option is derived from this list (`len(keypoints)`) and passed to the backend as metadata for model-dataset compatibility checks.

## Sample Data

The template includes sample data with:
- 3 images (person_001.jpg, person_002.jpg, person_003.jpg)
- 9 keypoints per image: nose, left_eye, right_eye, left_shoulder, right_shoulder, left_elbow, right_elbow, left_wrist, right_wrist
- Visibility flags indicating occluded keypoints

## Notes

- Keypoint coordinates are in pixel space relative to the original image dimensions
- Coordinates are automatically rescaled when images are resized during training
- The number of keypoints must be consistent across all samples
- All keypoint names must match across samples in a dataset
