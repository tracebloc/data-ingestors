"""Image Data Ingestion Example.

This example demonstrates how to ingest image data from a CSV file into a database
and optionally send it to an API. It includes image resizing and metadata extraction,
supporting both binary data and file-based image processing.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional
from PIL import Image
import io

from tracebloc_ingestor import Config, Database, APIClient, CSVIngestor
from tracebloc_ingestor.processors.base import BaseProcessor
from tracebloc_ingestor.utils.logging import setup_logging
from tracebloc_ingestor.utils.constants import DataCategory, Intent

# Initialize config and configure logging
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

class ImageResizeProcessor(BaseProcessor):
    """Processor for handling image data in records.
    
    This processor resizes images to a target size while maintaining aspect ratio,
    and extracts image metadata. It supports both binary data and file-based processing.
    """
    
    def __init__(self, config: Config, target_size: tuple = (800, 800), storage_path: Optional[str] = None):
        """Initialize the image processor.
        
        Args:
            config: Configuration object
            target_size: Target size for resized images (width, height)
            storage_path: Optional path for storing processed images
        """
        super().__init__(config)
        self.target_size = target_size
        self.storage_path = Path(storage_path) if storage_path else None
        if self.storage_path:
            self.storage_path.mkdir(parents=True, exist_ok=True)
        self._processed_files = set()  # Track processed files for cleanup
        
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process image data in the record.
        
        Args:
            record: The record containing image data
            
        Returns:
            Processed record with resized image and metadata
            
        Raises:
            ValueError: If image data is invalid or processing fails
        """
        try:
            # Try to get image data from binary field first
            image_data = record.get('image_data')
            if image_data:
                if not isinstance(image_data, bytes):
                    raise ValueError("image_data must be bytes")
                # Process binary image data
                return self._process_binary_image(record, image_data)
            
            # Try to get image from file path
            filename = record.get('filename')
            if filename:
                if not isinstance(filename, str):
                    raise ValueError("filename must be a string")
                # Process file-based image
                return self._process_file_image(record, filename)
            
            raise ValueError("No image data or filename found in record")
            
        except Exception as e:
            raise ValueError(f"Error processing image: {str(e)}")
    
    def _process_binary_image(self, record: Dict[str, Any], image_data: bytes) -> Dict[str, Any]:
        """Process image from binary data.
        
        Args:
            record: The record containing image data
            image_data: Binary image data
            
        Returns:
            Processed record with resized image and metadata
            
        Raises:
            ValueError: If image processing fails
        """
        try:
            # Open image from binary data
            image = Image.open(io.BytesIO(image_data))
            
            # Extract original dimensions
            original_width, original_height = image.size
            
            # Resize image while maintaining aspect ratio
            image.thumbnail(self.target_size, Image.Resampling.LANCZOS)
            
            # Get new dimensions
            new_width, new_height = image.size
            
            # Convert back to binary data
            output = io.BytesIO()
            image.save(output, format=image.format)
            record['image_data'] = output.getvalue()
            
            # Update metadata
            record['original_width'] = original_width
            record['original_height'] = original_height
            record['width'] = new_width
            record['height'] = new_height
            record['format'] = image.format.lower()
            
            return record
        except Exception as e:
            raise ValueError(f"Error processing binary image: {str(e)}")
    
    def _process_file_image(self, record: Dict[str, Any], filename: str) -> Dict[str, Any]:
        """Process image from file path.
        
        Args:
            record: The record containing image data
            filename: Path to the image file
            
        Returns:
            Processed record with resized image and metadata
            
        Raises:
            ValueError: If image processing fails
        """
        try:
            # Get the source image path
            src_path = Path(filename)
            if not src_path.exists():
                raise ValueError(f"Source image not found: {src_path}")
            
            # Open and resize the image
            with Image.open(src_path) as image:
                # Extract original dimensions
                original_width, original_height = image.size
                
                # Resize image while maintaining aspect ratio
                image.thumbnail(self.target_size, Image.Resampling.LANCZOS)
                
                # Get new dimensions
                new_width, new_height = image.size
                
                # Save the resized image if storage path is provided
                if self.storage_path:
                    dest_path = self.storage_path / src_path.name
                    image.save(dest_path, format=image.format)
                    record['processed_path'] = str(dest_path)
                    self._processed_files.add(str(dest_path))
                
                # Update metadata
                record['original_width'] = original_width
                record['original_height'] = original_height
                record['width'] = new_width
                record['height'] = new_height
                record['format'] = image.format.lower()
                
                return record
        except Exception as e:
            raise ValueError(f"Error processing file image: {str(e)}")
    
    def cleanup(self):
        """Cleanup any temporary files if needed."""
        if self.storage_path:
            for file_path in self._processed_files:
                try:
                    Path(file_path).unlink(missing_ok=True)
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")
            self._processed_files.clear()

def main():
    """Run the image ingestion example."""
    try:
        # Initialize components
        database = Database(config)
        api_client = APIClient(config)

        # Schema definition for image data with constraints
        schema = {
            "filename": "VARCHAR(255) NOT NULL",
            "image_data": "LONGBLOB",
            "width": "INT NOT NULL",
            "height": "INT NOT NULL",
            "original_width": "INT NOT NULL",
            "original_height": "INT NOT NULL",
            "format": "VARCHAR(10) NOT NULL",
            "processed_path": "VARCHAR(512)",
            "notes": "TEXT"
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

        # Create image processor with storage path
        image_processor = ImageResizeProcessor(
            config=config,
            target_size=(800, 800),
            storage_path=config.STORAGE_PATH
        )

        # Create ingestor with image processor
        ingestor = CSVIngestor(
            database=database,
            api_client=api_client,
            table_name=config.TABLE_NAME,
            schema=schema,
            category=DataCategory.TABULAR_CLASSIFICATION,
            intent=Intent.TRAIN,
            csv_options=csv_options,
            processors=[image_processor]
        )

        # Get the example data path
        data_path = Path(__file__).parent / "data" / "labels_file_sample.csv"
        
        # Ingest data
        with ingestor:
            failed_records = ingestor.ingest(
                str(data_path),
                batch_size=10  # Smaller batch size due to larger data
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