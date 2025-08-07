"""Object Detection Data Ingestion Example.

This example demonstrates how to ingest object detection data with images and XML annotations
into a database and optionally send it to an API. It processes both the image files and their
corresponding XML annotation files from the VisDrone dataset format.
"""

import logging
import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Generator
from PIL import Image
import io
import json

from tracebloc_ingestor import Config, Database, APIClient
from tracebloc_ingestor.ingestors.base import BaseIngestor
from tracebloc_ingestor.processors.base import BaseProcessor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import DataCategory, Intent, DataFormat

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
    
    def __init__(self, config: Config, target_size: tuple = (800, 800), 
                 storage_path: Optional[str] = None, annotations_path: str = None):
        """Initialize the object detection processor.
        
        Args:
            config: Configuration object
            target_size: Target size for resized images (width, height)
            storage_path: Optional path for storing processed images
            annotations_path: Path to the annotations directory
        """
        super().__init__(config)
        self.target_size = target_size
        self.storage_path = Path(storage_path) if storage_path else None
        self.annotations_path = Path(annotations_path) if annotations_path else None
        self.dest_path = Path(f"{config.DEST_PATH}") if config.DEST_PATH else None
        
        if self.storage_path:
            self.storage_path.mkdir(parents=True, exist_ok=True)
        
        if self.dest_path:
            self.dest_path.mkdir(parents=True, exist_ok=True)
        
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
            image_path = record.get('image_path')
            if not image_path:
                # Construct image path from filename
                filename = record.get('filename')
                if not filename:
                    raise ValueError("Neither image_path nor filename found in record")
                
                # Construct the image path based on the dataset structure
                # Assuming images are in VisDrone2019-DET-train/images/
                image_path = f"{self.storage_path}/images/{filename}"
            
            if not isinstance(image_path, str):
                raise ValueError("image_path must be a string")
            
            logger.info(f"Processing image: {image_path}")
            
            # Process image and get annotations
            record = self._process_image(record, image_path)
            logger.info(f"After image processing: {list(record.keys())}")
            
            record = self._process_annotations(record, image_path)
            logger.info(f"After annotation processing: {list(record.keys())}")
            
            logger.info(f"Processed record successfully: {list(record.keys())}")
            return record
            
        except Exception as e:
            logger.error(f"Error processing object detection data: {str(e)}")
            raise ValueError(f"Error processing object detection data: {str(e)}")
    
    def _process_image(self, record: Dict[str, Any], image_path: str) -> Dict[str, Any]:
        """Process the image file.
        
        Args:
            record: The record to update
            image_path: Path to the image file
            
        Returns:
            Updated record with image metadata
        """
        try:
            image_file = Path(image_path)
            if not image_file.exists():
                raise ValueError(f"Image file not found: {image_file}")
            
            with Image.open(image_file) as image:
                # Extract original dimensions
                original_width, original_height = image.size
                
                # Resize image while maintaining aspect ratio
                image.thumbnail(self.target_size, Image.Resampling.LANCZOS)
                
                # Get new dimensions
                new_width, new_height = image.size

                # Save processed image to destination path with new image name
                if self.dest_path:
                    dest_filename = f"{record['data_id']}.jpg"
                    dest_path = self.dest_path / dest_filename
                    image.save(dest_path, format='JPEG')
                    record['processed_path'] = str(dest_path)
                    self._final_files.add(str(dest_path))  # Track as final output file
                
                # Update metadata
                record['original_width'] = original_width
                record['original_height'] = original_height
                record['width'] = new_width
                record['height'] = new_height
                record['format'] = image.format.lower()
                
                return record
                
        except Exception as e:
            raise ValueError(f"Error processing image: {str(e)}")
    
    def _process_annotations(self, record: Dict[str, Any], image_path: str) -> Dict[str, Any]:
        """Process the XML annotation file.
        
        Args:
            record: The record to update
            image_path: Path to the image file (used to find corresponding XML)
            
        Returns:
            Updated record with annotation data for a single object
        """
        try:
            if not self.annotations_path:
                raise ValueError("annotations_path not set")
            
            # Get corresponding XML file
            image_name = Path(image_path).stem
            xml_path = self.annotations_path / f"{image_name}.xml"
            
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
                    if self.dest_path:
                        xml_filename = f"{record['data_id']}.xml"
                        xml_dest_path = self.dest_path / xml_filename
                        
                        # Create new XML tree with only this object
                        new_root = ET.Element('annotation')
                        
                        # Copy basic annotation info
                        for elem_name in ['folder', 'path', 'source', 'size', 'segmented']:
                            elem = root.find(elem_name)
                            if elem is not None:
                                new_root.append(elem)
                        
                        # Add filename element with the new naming convention
                        filename_elem = ET.SubElement(new_root, 'filename')
                        filename_elem.text = f"{image_name}_obj_{object_index}.jpg"
                        
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
    
    def cleanup(self):
        """Cleanup any temporary files."""
        if self.storage_path:
            for file_path in self._processed_files:
                try:
                    Path(file_path).unlink(missing_ok=True)
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")
            self._processed_files.clear()
        
        # Final output files in dest_path are not cleaned up - they are the intended output
        logger.info(f"Final output files created: {len(self._final_files)}")
        for file_path in self._final_files:
            logger.debug(f"Final output file: {file_path}")

class ObjectDetectionIngestor(BaseIngestor):
    """Ingestor for object detection datasets with images and annotations."""
    
    def __init__(self, database: Database, api_client: APIClient,
                 table_name: str, schema: Dict[str, str],
                 category: DataCategory, intent: Intent,
                 processors: List[BaseProcessor], data_format):
        """Initialize the object detection ingestor.
        
        Args:
            database: Database instance
            api_client: API client instance
            table_name: Name of the table to store data
            schema: Database schema
            category: Data category
            intent: Processing intent
            processors: List of data processors
        """
        super().__init__(database, api_client, table_name, schema,
                        processors, unique_id_column="data_id",
                        intent=intent, category=category, data_format=data_format)
    
    def read_data(self, source: Dict[str, str]) -> Generator[Dict[str, Any], None, None]:
        """Read object detection data from images directory.
        
        This method reads image files from the specified directory and yields
        records for each object in each image file found.
        
        Args:
            source: Dictionary containing 'images_dir' and 'annotations_dir' paths
            
        Yields:
            Dict containing image file information for each object
            
        Raises:
            ValueError: If source is invalid or directory doesn't exist
        """
        if not isinstance(source, dict):
            raise ValueError("Source must be a dictionary with 'images_dir' and 'annotations_dir'")
        
        images_dir = source.get('images_dir')
        annotations_dir = source.get('annotations_dir')
        if not images_dir:
            raise ValueError("Source must contain 'images_dir' key")
        if not annotations_dir:
            raise ValueError("Source must contain 'annotations_dir' key")
        
        images_path = Path(images_dir)
        annotations_path = Path(annotations_dir)
        if not images_path.exists():
            raise ValueError(f"Images directory not found: {images_path}")
        if not annotations_path.exists():
            raise ValueError(f"Annotations directory not found: {annotations_path}")
        
        # Process all image files
        image_files = sorted(images_path.glob('*.jpg'))  # Assuming JPG format
        
        for image_file in image_files:
            # Get corresponding XML file
            image_name = image_file.stem
            xml_path = annotations_path / f"{image_name}.xml"
            
            if not xml_path.exists():
                logger.warning(f"Annotation file not found for {image_name}, skipping")
                continue
            
            # Parse XML to get object count
            try:
                tree = ET.parse(xml_path)
                root = tree.getroot()
                objects = root.findall('object')
                
                # Create a record for each object in the image
                for obj_index, obj in enumerate(objects):
                    bbox = obj.find('bndbox')
                    class_elem = obj.find('name')
                    
                    if bbox is not None and class_elem is not None and class_elem.text is not None:
                        # Create unique data_id for each object
                        object_data_id = f"{image_name}_obj_{obj_index}"
                        
                        record = {
                            'image_path': str(image_file),
                            'filename': image_file.name,
                            'data_id': object_data_id,  # Unique ID for each object
                            'label': class_elem.text,  # Individual object label
                            'object_index': obj_index  # Track object position in image
                        }
                        yield record
                    else:
                        logger.warning(f"Skipping invalid object {obj_index} in {image_name}")
                        
            except Exception as e:
                logger.error(f"Error processing annotations for {image_name}: {str(e)}")
                continue
    
    def _count_records(self, source: Dict[str, str]) -> Optional[int]:
        """Count total objects across all images in the directory.
        
        Args:
            source: Dictionary containing 'images_dir' and 'annotations_dir' paths
            
        Returns:
            Total number of objects if countable, None otherwise
        """
        try:
            images_dir = source.get('images_dir')
            annotations_dir = source.get('annotations_dir')
            if not images_dir or not annotations_dir:
                return None
            
            images_path = Path(images_dir)
            annotations_path = Path(annotations_dir)
            if not images_path.exists() or not annotations_path.exists():
                return None
            
            total_objects = 0
            # Count objects in each image
            for image_file in images_path.glob('*.jpg'):
                image_name = image_file.stem
                xml_path = annotations_path / f"{image_name}.xml"
                
                if xml_path.exists():
                    try:
                        tree = ET.parse(xml_path)
                        root = tree.getroot()
                        objects = root.findall('object')
                        total_objects += len(objects)
                    except Exception as e:
                        logger.debug(f"Error counting objects in {image_name}: {str(e)}")
                        continue
            
            return total_objects
        except Exception as e:
            logger.debug(f"Unable to count objects: {str(e)}")
            return None
    
    def ingest(self, images_dir: str, annotations_dir: str,
               batch_size: int = 10) -> List[Dict[str, Any]]:
        """Ingest object detection data from images and annotations directories.
        
        Args:
            images_dir: Directory containing image files
            annotations_dir: Directory containing XML annotation files
            batch_size: Number of records to process in each batch
            
        Returns:
            List of failed records
        """
        logger.info(f"Starting object detection ingestion from {images_dir}")
        
        try:
            # Create source dictionary for read_data method
            source = {
                'images_dir': images_dir,
                'annotations_dir': annotations_dir
            }
            
            failed_records = super().ingest(source, batch_size)
            
            logger.info(
                f"Object detection ingestion completed. "
                f"Failed records: {len(failed_records)}"
            )
            
            return failed_records
            
        except Exception as e:
            logger.error(f"Object detection ingestion failed: {str(e)}")
            raise

def main():
    """Run the object detection ingestion example."""
    try:
        # Initialize components
        database = Database(config)
        api_client = APIClient(config)

        # Schema definition for object detection data
        schema = {
            "filename": "VARCHAR(255) NOT NULL",
            "width": "INT NOT NULL",
            "height": "INT NOT NULL",
            "original_width": "INT NOT NULL",
            "original_height": "INT NOT NULL",
            "format": "VARCHAR(10) NOT NULL",
            "processed_path": "VARCHAR(512)",
            "object_count": "INT NOT NULL"
        }

        # Get dataset paths
        images_dir = f"{config.STORAGE_PATH}/images"
        annotations_dir = f"{config.STORAGE_PATH}/annotations"

        # Create object detection processor
        object_detection_processor = ObjectDetectionProcessor(
            config=config,
            target_size=(800, 800),
            storage_path=config.STORAGE_PATH,
            annotations_path=annotations_dir
        )

        # Create ingestor
        ingestor = ObjectDetectionIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            category=DataCategory.OBJECT_DETECTION,
            intent=Intent.TEST,
            processors=[object_detection_processor],
            data_format=DataFormat.IMAGE,
        )

        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(
                images_dir=str(images_dir),
                annotations_dir=str(annotations_dir),
                batch_size=config.BATCH_SIZE
            )
            
            if failed_records:
                logger.warning(f"Failed to process {len(failed_records)} records")
                for record in failed_records:
                    logger.warning(f"Failed record: {record.get('filename', 'Unknown')}")
                    logger.warning(f"Error details: {record.get('error', 'Unknown error')}")
            else:
                logger.info("All records processed successfully")

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 