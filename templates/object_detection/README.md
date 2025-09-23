# Object Detection Data Ingestion Template

This template demonstrates how to ingest object detection data with images and XML annotations into a database using the tracebloc_ingestor framework.

## Directory Structure

```
object_detection/
├── object_detection.py          # Main ingestion script
├── README.md                    # This file
└── data/
    ├── images/                  # Sample image files
    │   ├── image1.png
    │   ├── image2.png
    │   └── image3.png
    ├── annotations/             # XML annotation files
    │   ├── image1.xml
    │   ├── image2.xml
    │   └── image3.xml
    └── labels_file_sample.csv   # CSV file with object labels
```

## Data Format

### Images
- Supported formats: PNG, JPEG, JPG
- Images should be placed in the `data/images/` directory
- Each image should have a corresponding XML annotation file

### XML Annotations
- XML files should be placed in the `data/annotations/` directory
- Each XML file should follow the Pascal VOC format
- File naming convention: `{image_name}.xml`

### CSV Labels File
The CSV file contains the following columns:
- `object_id`: Unique identifier for each object (format: `{image_name}_obj_{index}`)
- `image_id`: Name of the image file (without extension)
- `image_label`: Class label of the object
- `object_count`: Number of objects in the image (always 1 for individual objects)

## XML Annotation Format

Each XML file should contain:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<annotation>
    <folder>images</folder>
    <filename>image_name.png</filename>
    <path>/path/to/images/image_name.png</path>
    <source>
        <database>Unknown</database>
    </source>
    <size>
        <width>640</width>
        <height>480</height>
        <depth>3</depth>
    </size>
    <segmented>0</segmented>
    <object>
        <name>class_name</name>
        <pose>Unspecified</pose>
        <truncated>0</truncated>
        <difficult>0</difficult>
        <bndbox>
            <xmin>100</xmin>
            <ymin>50</ymin>
            <xmax>300</xmax>
            <ymax>400</ymax>
        </bndbox>
    </object>
    <!-- Additional objects... -->
</annotation>
```

## Usage

1. Place your images in the `data/images/` directory
2. Create corresponding XML annotation files in the `data/annotations/` directory
3. Update the `labels_file_sample.csv` with your data
4. Configure the ingestion parameters in `object_detection.py`
5. Run the ingestion script:

```bash
python object_detection.py
```

## Configuration

The script uses the following configuration:
- **Target Size**: (256, 256) - Images will be resized to this dimension
- **Extension**: PNG - Expected image file extension
- **Chunk Size**: 100 - Number of records to process in each batch
- **Category**: OBJECT_DETECTION
- **Data Format**: IMAGE
- **Intent**: TRAIN

## Sample Data

The template includes sample data with:
- 3 images (image1.png, image2.png, image3.png)
- 3 XML annotation files with multiple objects per image
- 7 total object annotations across all images
- Classes: person, car, dog, bicycle, cat

## Notes

- The framework automatically validates image files and annotations
- Images are copied to the destination directory during processing
- XML annotations are processed and stored as JSON in the database
- Each object becomes a separate record in the database
