# Object Detection Data Ingestion Template

This template demonstrates how to ingest object detection data with images and XML annotations into a database using the tracebloc_ingestor framework.

## Quickstart — declarative (recommended)

Ingest with ~9 lines of YAML using the official ingestor image (`ghcr.io/tracebloc/ingestor`). No Python edits, no Dockerfile to build.

> **Prerequisite:** the chart doesn't transport data into the cluster. Stage your files on the cluster's shared PVC first — see the [data-staging recipe](https://github.com/tracebloc/client/blob/develop/ingestor/README.md#stage-your-data-on-the-shared-pvc) in the chart docs (kubectl cp pattern for small datasets, init-container sync for production).

**1. Stage the data** on the shared PVC at `/data/shared/<your-prefix>/` with `images/` and `annotations/` (Pascal VOC XML) subdirectories.

**2. Write `ingest.yaml`:**

```yaml
apiVersion: tracebloc.io/v1
kind: IngestConfig
category: object_detection
table: visdrone_train
intent: train
images: /data/shared/visdrone/images/
annotations: /data/shared/visdrone/annotations/
```

> **No `csv:` and no `label:`** — since backend#1006 records are enumerated from
> `annotations/*.xml`, one per IMAGE, with the class read from `<object><name>`.
> There is no manifest to point at and no user-named label column, so both keys
> are **rejected** for this category rather than ignored: a stale `labels.csv`
> config fails loudly instead of being silently dropped while the XML is
> enumerated behind it.

**3. Install:**

```bash
helm install my-od-dataset tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

Object detection uses atomic image+annotation transfer — a record is committed only when both files copy successfully. Canonical example: [`examples/yaml/object_detection.yaml`](../../examples/yaml/object_detection.yaml). Full chart docs: [`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/develop/ingestor/README.md).

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
    └── annotations/             # XML annotation files — the RECORD SOURCE
        ├── image1.xml
        ├── image2.xml
        └── image3.xml
```

## Data Format

### Images
- Supported formats: PNG, JPEG, JPG
- Images should be placed in the `data/images/` directory
- Each image should have a corresponding XML annotation file

### XML Annotations
- XML files should be placed in the `data/annotations/` directory
- Each XML file should follow the Pascal VOC format
- File naming convention: `{image_name}.xml`, where `{image_name}` is the image's
  **stem** (its name with no extension). The annotation for `images/image1.jpg` is
  `annotations/image1.xml`.

### Labels

There is **no labels file**. One record is one image, and its classes come from
the `<object><name>` elements of `annotations/<stem>.xml`.

The image and its annotation are paired by **stem** — the image name with no
extension — so `images/image1.jpg` pairs with `annotations/image1.xml`.

> If a run reports 0 committed rows with "Source file not found:
> `annotations/…`", it is a missing or mis-stemmed annotation. Object detection
> is atomic: a record commits only when **both** the image and its `<stem>.xml`
> copy successfully.

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

## Advanced: custom processor script

Use the Python+Dockerfile pattern when the declarative schema can't express your processing needs. Otherwise prefer the Quickstart above.

1. Place your images in the `data/images/` directory
2. Create corresponding XML annotation files in the `data/annotations/` directory
3. Configure the ingestion parameters in `object_detection.py`
4. Run the ingestion script:

```bash
python object_detection.py
```

## Configuration

The script uses the following configuration:
- **Target Size**: (1920, 1080) - Images will be resized to this dimension (height = width)
- **Extension**: JPG - Expected image file extension (jpeg, jpg are also accepted)
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
