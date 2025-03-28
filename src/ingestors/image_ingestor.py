import os
import csv
import logging
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import imghdr

from src.ingestors.base import BaseIngestor
from src.database import Database
from src.api.client import APIClient
from src.processors.base import BaseProcessor
from src.utils.constants import DataCategory, Intent

logger = logging.getLogger(__name__)

class ImageIngestor(BaseIngestor):
    """
    Ingestor for image data with associated metadata.
    
    This class handles the ingestion of image files from a directory,
    along with their metadata from a CSV file.
    """
    
    def __init__(
        self,
        database: Database,
        api_client: APIClient,
        table_name: str,
        schema: Dict[str, str],
        category: DataCategory,
        image_options: Dict[str, Any],
        processors: Optional[List[BaseProcessor]] = None,
        label_column: Optional[str] = None,
        intent: Intent = Intent.TRAIN,
        annotation_column: Optional[str] = None,
        unique_id_column: Optional[str] = "image_id"
    ):
        """
        Initialize the ImageIngestor.
        
        Args:
            database: Database instance for storing data
            api_client: API client for external services
            table_name: Name of the table to store data
            schema: Database schema for the table
            category: Data category (e.g., IMAGE_CLASSIFICATION)
            image_options: Options for image processing
                - target_size: Tuple of (width, height) for resizing
                - normalize: Whether to normalize pixel values
                - formats: List of supported image formats
                - recursive: Whether to search subdirectories
            processors: List of data processors to apply
            label_column: Column name containing the label
            intent: Whether data is for training or testing
            annotation_column: Column containing annotations
            unique_id_column: Column containing unique identifiers
        """
        super().__init__(
            database=database,
            api_client=api_client,
            table_name=table_name,
            schema=schema,
            category=category,
            processors=processors,
            label_column=label_column,
            intent=intent,
            annotation_column=annotation_column,
            unique_id_column=unique_id_column
        )
        self.image_options = image_options
        self.supported_formats = image_options.get("formats", ["jpg", "jpeg", "png"])
        self.target_size = image_options.get("target_size", (224, 224))
        self.normalize = image_options.get("normalize", True)
        self.recursive = image_options.get("recursive", True)
    
    def ingest(
        self, 
        image_dir: str, 
        metadata_file: Optional[str] = None, 
        batch_size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Ingest images from a directory and their metadata from a CSV file.
        
        Args:
            image_dir: Directory containing image files
            metadata_file: CSV file with image metadata
            batch_size: Number of records to process in each batch
            
        Returns:
            List of records that failed to process
        """
        logger.info(f"Starting image ingestion from {image_dir}")
        
        # Validate directory
        if not os.path.isdir(image_dir):
            raise ValueError(f"Image directory does not exist: {image_dir}")
        
        # Load metadata if provided
        metadata = {}
        if metadata_file and os.path.exists(metadata_file):
            metadata = self._load_metadata(metadata_file)
            logger.info(f"Loaded metadata for {len(metadata)} images")
        
        # Collect image files
        image_files = self._collect_image_files(image_dir)
        logger.info(f"Found {len(image_files)} image files")
        
        # Process images in batches
        failed_records = []
        records_batch = []
        
        for image_path in image_files:
            try:
                # Create record from image and metadata
                record = self._create_record_from_image(image_path, metadata)
                
                # Apply processors
                if self.processors:
                    for processor in self.processors:
                        record = processor.process(record)
                
                records_batch.append(record)
                
                # Process batch if it reaches the batch size
                if len(records_batch) >= batch_size:
                    batch_failed = self._process_batch(records_batch)
                    failed_records.extend(batch_failed)
                    records_batch = []
            
            except Exception as e:
                logger.error(f"Error processing image {image_path}: {str(e)}")
                failed_records.append({"filename": os.path.basename(image_path), "error": str(e)})
        
        # Process any remaining records
        if records_batch:
            batch_failed = self._process_batch(records_batch)
            failed_records.extend(batch_failed)
        
        logger.info(f"Image ingestion completed. Failed records: {len(failed_records)}")
        return failed_records
    
    def _load_metadata(self, metadata_file: str) -> Dict[str, Dict[str, Any]]:
        """
        Load metadata from a CSV file.
        
        Args:
            metadata_file: Path to CSV file with metadata
            
        Returns:
            Dictionary mapping filenames to their metadata
        """
        metadata = {}
        with open(metadata_file, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if 'filename' in row:
                    metadata[row['filename']] = row
        return metadata
    
    def _collect_image_files(self, image_dir: str) -> List[str]:
        """
        Collect all image files from the directory.
        
        Args:
            image_dir: Directory to search for images
            
        Returns:
            List of paths to image files
        """
        image_files = []
        
        if self.recursive:
            # Walk through all subdirectories
            for root, _, files in os.walk(image_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    if self._is_valid_image(file_path):
                        image_files.append(file_path)
        else:
            # Only look at files in the top directory
            for file in os.listdir(image_dir):
                file_path = os.path.join(image_dir, file)
                if os.path.isfile(file_path) and self._is_valid_image(file_path):
                    image_files.append(file_path)
        
        return image_files
    
    def _is_valid_image(self, file_path: str) -> bool:
        """
        Check if a file is a valid image in supported formats.
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if the file is a valid image, False otherwise
        """
        # Check file extension
        ext = os.path.splitext(file_path)[1].lower().lstrip('.')
        if ext not in self.supported_formats:
            return False
        
        # Verify it's actually an image using imghdr
        try:
            img_type = imghdr.what(file_path)
            return img_type is not None and img_type in self.supported_formats
        except Exception:
            return False
    
    def _create_record_from_image(
        self, 
        image_path: str, 
        metadata: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Create a record from an image file and its metadata.
        
        Args:
            image_path: Path to the image file
            metadata: Dictionary of metadata
            
        Returns:
            Record dictionary with image data and metadata
        """
        filename = os.path.basename(image_path)
        
        # Start with basic image information
        record = {
            "filename": filename,
            "image_id": os.path.splitext(filename)[0],  # Use filename without extension as ID
            "format": os.path.splitext(filename)[1].lower().lstrip('.'),
            "file_path": image_path,
        }
        
        # In a real implementation, you would extract image dimensions here
        # For this example, we'll use placeholder values
        record["width"] = 0
        record["height"] = 0
        
        # Add metadata if available
        if filename in metadata:
            record.update(metadata[filename])
        
        return record
    
    def _process_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a batch of records.
        
        Args:
            records: List of records to process
            
        Returns:
            List of records that failed to process
        """
        failed_records = []
        
        try:
            # Store records in database
            self.database.insert_many(self.table_name, records)
            
            # Send to API if needed
            if self.api_client:
                for record in records:
                    try:
                        self.api_client.send_data(record, self.category, self.intent)
                    except Exception as e:
                        logger.error(f"API error for record {record.get('image_id')}: {str(e)}")
                        failed_records.append(record)
        
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            failed_records.extend(records)
        
        return failed_records 