# Semantic Segmentation Data Ingestion Template

This template demonstrates how to ingest semantic segmentation data with images and corresponding mask annotations into a database using the tracebloc_ingestor framework.

## Directory Structure

```
semantic_segmentation/
├── semantic_segmentation.py     # Main ingestion script
├── README.md                    # This file
└── data/
    ├── images/                  # Source image files
    │   ├── image_001.png
    │   ├── image_002.png
    │   └── image_003.png
    ├── masks/                   # Mask annotation files
    │   ├── image_001_mask.png
    │   ├── image_002_mask.png
    │   └── image_003_mask.png
    └── labels_file_sample.csv   # CSV file with image-mask mappings and labels
```

## Data Format

### Images
- Supported formats: PNG, JPEG, JPG
- Images should be placed in the `data/images/` directory
- Each image must have a corresponding mask file in the `data/masks/` directory

### Mask Annotations
- Mask files should be placed in the `data/masks/` directory
- Masks must be grayscale PNG images where each pixel value represents a class index
  - Pixel value `0` = background
  - Pixel value `1` = class 1
  - Pixel value `2` = class 2
  - And so on...
- Masks must have the same spatial dimensions as their corresponding source images
- File naming convention: `{image_name}_mask.png`

### CSV Labels File
The CSV file contains the following columns:
- `data_id`: Name of the image file (without extension)
- `mask_id`: Name of the corresponding mask file (without extension)
- `image_label`: Class label present in the image (one row per class per image)

Example:
```csv
data_id,mask_id,image_label
image_001,image_001_mask,road
image_001,image_001_mask,car
image_002,image_002_mask,building
image_002,image_002_mask,sky
```

## Usage

1. Place your images in the `data/images/` directory
2. Place your corresponding mask files in the `data/masks/` directory
3. Update the `labels_file_sample.csv` with your data
4. Configure the ingestion parameters in `semantic_segmentation.py`
5. Run the ingestion script:

```bash
python semantic_segmentation.py
```

## Configuration

The script uses the following configuration:
- **Target Size**: (512, 512) - Images and masks will be resized to this dimension
- **Extension**: PNG - Expected image file extension
- **Chunk Size**: 100 - Number of records to process in each batch
- **Category**: SEMANTIC_SEGMENTATION
- **Data Format**: IMAGE
- **Intent**: TRAIN

## Sample Data

The template includes sample data with:
- 3 images (image_001.png, image_002.png, image_003.png)
- 3 mask files (image_001_mask.png, image_002_mask.png, image_003_mask.png)
- 10 total label entries across all images
- Classes: road, car, sidewalk, building, sky, tree, person, traffic_sign

## Notes

- The framework automatically validates image files during ingestion
- Masks should use nearest-neighbor interpolation when resizing to preserve class indices
- Each pixel in the mask represents a class label (integer index)
- Images are copied to the destination directory during processing
- Multiple rows per image are expected when an image contains multiple classes
