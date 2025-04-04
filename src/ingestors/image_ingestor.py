import os
import logging
import imghdr
from typing import Dict, Any, List, Optional

from src.ingestors.csv_ingestor import CSVIngestor
from src.database import Database
from src.api.client import APIClient
from src.processors.base import BaseProcessor
from src.utils.constants import DataCategory, Intent

logger = logging.getLogger(__name__)

class ImageIngestor(CSVIngestor):
    """
    Ingestor for image data with associated metadata from CSV.
    
    Inherits CSV handling capabilities from CSVIngestor while adding
    image-specific functionality.
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
        """Initialize ImageIngestor with both CSV and image-specific options."""
        # Extract CSV options from image_options or use defaults
        csv_options = image_options.get("csv_options", {
            "chunk_size": 1000,
            "delimiter": ",",
            "quotechar": '"',
            "escapechar": "\\"
        })
        
        super().__init__(
            database=database,
            api_client=api_client,
            table_name=table_name,
            schema=schema,
            category=category,
            csv_options=csv_options,
            processors=processors,
            label_column=label_column,
            intent=intent,
            annotation_column=annotation_column,
            unique_id_column=unique_id_column
        )
        
        # Store image-specific options
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
        Ingest images and their metadata.
        
        Args:
            image_dir: Directory containing image files
            metadata_file: Optional CSV file with image metadata
            batch_size: Number of records to process in each batch
        """
        logger.info(f"Starting image ingestion from {image_dir}")
        
        if not os.path.isdir(image_dir):
            raise ValueError(f"Image directory does not exist: {image_dir}")
            
        # Collect all valid image files first
        image_files = self._collect_image_files(image_dir)
        logger.info(f"Found {len(image_files)} valid image files")
        
        # If metadata file exists, use CSVIngestor's functionality to read it
        metadata = {}
        if metadata_file and os.path.exists(metadata_file):
            # Use CSVIngestor's _read_csv method (assuming it's available)
            # If not, we can still use the direct CSV reading approach
            for record in self._read_csv(metadata_file):
                if 'filename' in record:
                    metadata[record['filename']] = record
            logger.info(f"Loaded metadata for {len(metadata)} images")
            
        # Process images and merge with metadata
        failed_records = []
        records_batch = []
        
        for image_path in image_files:
            try:
                # Create base record from image
                record = self._create_record_from_image(image_path)
                
                # Merge with metadata if available
                filename = os.path.basename(image_path)
                if filename in metadata:
                    record.update(metadata[filename])
                
                # Use CSVIngestor's record processing pipeline
                processed_record = self._process_record(record)
                records_batch.append(processed_record)
                
                # Use CSVIngestor's batch processing
                if len(records_batch) >= batch_size:
                    failed = self._process_batch(records_batch)
                    failed_records.extend(failed)
                    records_batch = []
                    
            except Exception as e:
                logger.error(f"Error processing image {image_path}: {str(e)}")
                failed_records.append({
                    "filename": os.path.basename(image_path),
                    "error": str(e)
                })
        
        # Process remaining records using CSVIngestor's functionality
        if records_batch:
            failed = self._process_batch(records_batch)
            failed_records.extend(failed)
            
        return failed_records

    def _is_valid_image(self, file_path: str) -> bool:
        """Validate image file using extension and content checking."""
        try:
            ext = os.path.splitext(file_path)[1].lower().lstrip('.')
            if not ext or ext not in self.supported_formats:
                return False
                
            # Verify it's actually an image
            img_type = imghdr.what(file_path)
            return img_type is not None and img_type in self.supported_formats
            
        except Exception as e:
            logger.warning(f"Error validating image {file_path}: {e}")
            return False

    def _collect_image_files(self, image_dir: str) -> List[str]:
        """Collect all valid image files from the directory."""
        image_files = []
        
        if self.recursive:
            for root, _, files in os.walk(image_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.isfile(file_path) and self._is_valid_image(file_path):
                        image_files.append(file_path)
        else:
            for file in os.listdir(image_dir):
                file_path = os.path.join(image_dir, file)
                if os.path.isfile(file_path) and self._is_valid_image(file_path):
                    image_files.append(file_path)
                    
        return image_files

    def _create_record_from_image(self, image_path: str) -> Dict[str, Any]:
        """Create a base record from image file."""
        filename = os.path.basename(image_path)
        
        return {
            "filename": filename,
            "image_id": os.path.splitext(filename)[0],
            "format": os.path.splitext(filename)[1].lower().lstrip('.'),
            "file_path": image_path,
            "width": 0,  # Placeholder - would actually read dimensions
            "height": 0  # Placeholder - would actually read dimensions
        } 