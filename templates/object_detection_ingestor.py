"""Object Detection Data Ingestion Example.

This example demonstrates how to ingest object detection data with images and XML annotations
into a database and optionally send it to an API. It processes both the image files and their
corresponding XML annotation files from the VisDrone dataset format.
"""

import logging
import shutil
import xml.etree.ElementTree as ET
from typing import Dict, Any
import json
import os

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
from tracebloc_ingestor.processors.base import BaseProcessor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import TaskCategory, Intent, DataFormat

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

class ObjectDetectionProcessor(BaseProcessor):
    """Processor for handling object detection data including images and annotations.
    
    This processor handles both image processing and XML annotation parsing for object
    detection datasets. It supports the VisDrone dataset format with bounding boxes
    and object classes.
    """
    
    def __init__(self, config: Config, image_path: str = None, annotations_path: str = None):
        """Initialize the object detection processor.
        
        Args:
            config: Configuration object
            target_size: Target size for resized images (width, height)
            storage_path: Optional path for storing processed images
            annotations_path: Path to the annotations directory
        """
        super().__init__(config)
        self.image_path = image_path
        self.annotations_path = annotations_path
        os.makedirs(self.config.DEST_PATH, exist_ok=True)

        self._processed_files = set()  # Only for temporary files
        self._final_files = set()      # For final output files (not deleted)
    
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process image and annotation data in the record.
        
        Args:
            record: The record containing image path
            
        Returns:
            Processed record with image data, annotations and metadata
            
        Raises:
            ValueError: If processing fails
        """
        try:
            logger.info(f"Processing record: {record}")
            
            # Get image path - if not in record, construct from filename
            image_id = record.get('image_id')
            if not image_id:
                raise ValueError(f"{image_id} not found in record")
                
            # Construct the image path based on the dataset structure
            # Assuming images are in VisDrone2019-DET-train/images/
            full_image_path = f"{self.image_path}/{image_id}"
            
            if not isinstance(full_image_path, str):
                raise ValueError("image_path must be a string")
            
            logger.info(f"Processing image: {image_id}")
            
            # Process image and get annotations
            record = self._process_image(record, image_id, self.image_path)
            logger.info(f"After image processing: {list(record.keys())}")
            
            record = self._process_annotations(record, image_id, self.annotations_path)
            logger.info(f"After annotation processing: {list(record.keys())}")
            
            logger.info(f"Processed record successfully: {list(record.keys())}")
            return record
            
        except Exception as e:
            logger.error(f"Error processing object detection data: {str(e)}")
            raise ValueError(f"Error processing object detection data: {str(e)}")
    
    def _process_image(self, record: Dict[str, Any], image_id: str, image_path: str) -> Dict[str, Any]:
        """Process the image file.
        
        Args:
            record: The record to update
            image_path: Path to the image file
            
        Returns:
            Updated record with image metadata
        """
        try:

            # Process the image
            image_src_path = os.path.join(image_path, f"{image_id}.png")
            if not os.path.exists(image_src_path):
                logger.error(f"Source image not found: {image_src_path}")
                return record

            # Save the resized image
            image_dest_path = os.path.join(config.DEST_PATH, "image", f"{image_id}.png")
            # Copy file without resizing
            shutil.copy(image_src_path, image_dest_path)

            logger.info(f"Successfully copied image: {image_id}")

            return record
                
        except Exception as e:
            raise ValueError(f"Error processing image: {str(e)}")
    
    def _process_annotations(self, record: Dict[str, Any], image_id: str, annotation_path: str) -> Dict[str, Any]:
        """Process the XML annotation file.
        
        Args:
            record: The record to update
            image_id: image_id for annotation file
            annotation_path: Path to the annotation files (used to find corresponding XML)
            
        Returns:
            Updated record with annotation data for a single object
        """
        try:
            if not self.annotations_path:
                raise ValueError("annotations_path not set")
            
            # Get corresponding XML file
            xml_path = os.path.join(annotation_path, f"{image_id}.xml")
            
            if not xml_path.exists():
                raise ValueError(f"Annotation file not found: {xml_path}")
            
            # Parse XML
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            # Extract object index from data_id (format: image_name_obj_index)
            data_id = record.get('data_id', '')
            if '_obj_' in data_id:
                object_index = int(data_id.split('_obj_')[-1])
            else:
                raise ValueError(f"Invalid data_id format: {data_id}. Expected format: image_name_obj_index")
            
            # Find the specific object by index
            objects = root.findall('object')
            if object_index >= len(objects):
                raise ValueError(f"Object index {object_index} not found in {xml_path}")
            
            obj = objects[object_index]
            bbox = obj.find('bndbox')
            
            if bbox is not None:
                # Get all required elements with null checks
                class_elem = obj.find('name')
                difficult_elem = obj.find('difficult')
                truncated_elem = obj.find('truncated')
                pose_elem = obj.find('pose')
                
                xmin_elem = bbox.find('xmin')
                ymin_elem = bbox.find('ymin')
                xmax_elem = bbox.find('xmax')
                ymax_elem = bbox.find('ymax')
                
                # Check if all required elements exist and have text
                if (class_elem is not None and class_elem.text is not None and
                    difficult_elem is not None and difficult_elem.text is not None and
                    truncated_elem is not None and truncated_elem.text is not None and
                    pose_elem is not None and pose_elem.text is not None and
                    xmin_elem is not None and xmin_elem.text is not None and
                    ymin_elem is not None and ymin_elem.text is not None and
                    xmax_elem is not None and xmax_elem.text is not None and
                    ymax_elem is not None and ymax_elem.text is not None):
                    
                    annotation = {
                        'label': class_elem.text,
                        'difficult': int(difficult_elem.text),
                        'truncated': int(truncated_elem.text),
                        'pose': pose_elem.text,
                        'bbox': {
                            'xmin': int(xmin_elem.text),
                            'ymin': int(ymin_elem.text),
                            'xmax': int(xmax_elem.text),
                            'ymax': int(ymax_elem.text)
                        }
                    }
                    
                    # Save individual XML file for this object
                    annotation_dest_path = os.path.join(config.DEST_PATH, "annotations")

                    if annotation_dest_path:
                        xml_filename = f"{image_id}.xml"
                        xml_dest_path = os.path.join(annotation_dest_path, xml_filename)

                        # Create new XML tree with only this object
                        new_root = ET.Element('annotation')

                        # Copy basic annotation info
                        for elem_name in ['folder', 'path', 'source', 'size', 'segmented']:
                            elem = root.find(elem_name)
                            if elem is not None:
                                new_root.append(elem)

                        # Add filename element with the new naming convention
                        filename_elem = ET.SubElement(new_root, 'filename')
                        filename_elem.text = f"{image_id}_obj_{object_index}.jpg"

                        # Add only this specific object
                        new_root.append(obj)

                        # Write the XML file
                        new_tree = ET.ElementTree(new_root)
                        new_tree.write(xml_dest_path, encoding='utf-8', xml_declaration=True)
                        self._final_files.add(str(xml_dest_path))  # Track as final output file
                    
                    # Store annotation as JSON for database
                    record['annotation'] = json.dumps([annotation])
                    record['label'] = annotation['label']
                    record['object_count'] = 1
                    
                    return record
                else:
                    logger.warning(f"Skipping object {object_index} in {xml_path} due to missing or empty XML elements")
                    raise ValueError(f"Invalid object {object_index} in {xml_path}")
            else:
                raise ValueError(f"No bounding box found for object {object_index} in {xml_path}")
            
        except Exception as e:
            raise ValueError(f"Error processing annotations: {str(e)}")


def main():
    """Run the object detection ingestion example."""
    try:
        # Initialize components
        database = Database(config)
        api_client = APIClient(config)

        # Schema definition for object detection data
        schema = {
            "image_id": "VARCHAR(255) NOT NULL",
            "annotation_file": "LONGBLOB",
            "image_label" : "VARCHAR(50) NOT NULL",
            "object_count": "INT NOT NULL",
        }

        # CSV specific options
        csv_options = {
            "chunk_size": 100,  # Smaller chunk size due to larger data
            "delimiter": ",",
            "quotechar": '"',
            "escapechar": "\\",
            "on_bad_lines": 'warn',
            "encoding": "utf-8"
        }

        # Get dataset paths
        images_dir = f"{config.SRC_PATH}/images"
        annotations_dir = f"{config.SRC_PATH}/annotations"

        # Create object detection processor
        object_detection_processor = ObjectDetectionProcessor(
            config=config,
            image_path=images_dir,
            annotations_path=annotations_dir
        )

        # Create ingestor
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            category=TaskCategory.IMAGE_CLASSIFICATION,
            data_format=DataFormat.IMAGE,
            csv_options=csv_options,
            processors=[object_detection_processor],
            label_column="image_label",
            intent=Intent.TRAIN,
        )

        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(config.LABEL_FILE, batch_size=config.BATCH_SIZE)
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(
                        f"Failed record: {record.get('image_id', 'Unknown')} - {record.get('mask_id', 'Unknown')}")
                    logger.warning(f"Error details: {record.get('error', 'Unknown error')}")
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 