# Image Classification Data Ingestion Template

This template demonstrates how to ingest image classification data with images and a CSV labels file into a database using the tracebloc_ingestor framework.

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
- Supported formats: PNG, JPEG, JPG
- Images should be placed in the `data/images/` directory
- Each image must have a corresponding row in the CSV file

### CSV Labels File
The CSV contains the following columns:
- `filename`: Image filename with extension, e.g. `cat1.jpeg`
- `label`: Class label for the image, e.g. `cat`, `dog`

## Usage

1. Place your images in the `data/images/` directory
2. Update `labels_file_sample.csv` with your `(filename, label)` pairs
3. Configure the ingestion parameters in `image_classification.py`
4. Run the ingestion script:

```bash
python image_classification.py
```

## Configuration

The script uses the following configuration:
- **Target Size**: (512, 512) - Images will be resized to this dimension (height = width)
- **Extension**: JPG - Expected image file extension (jpeg, jpg, and png are also accepted)
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
