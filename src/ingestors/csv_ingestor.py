from typing import Dict, Any, Generator, Optional, List
import pandas as pd
import logging
from pathlib import Path
from .base import BaseIngestor
from ..database import Database
from ..api.client import APIClient
from ..processors.base import BaseProcessor

logger = logging.getLogger(__name__)

class CSVIngestor(BaseIngestor):
    def __init__(
        self,
        database: Database,
        api_client: APIClient,
        table_name: str,
        schema: Dict[str, str],
        processors: Optional[List[BaseProcessor]] = None,
        max_retries: int = 3,
        csv_options: Optional[Dict[str, Any]] = None,
        unique_id_column: Optional[str] = None,
        label_column: Optional[str] = None,
        intent_column: Optional[str] = None,
        annotation_column: Optional[str] = None
    ):
        """
        Initialize CSV Ingestor
        
        Args:
            database: Database instance
            api_client: API client instance
            table_name: Name of the target table
            schema: Database schema definition
            processors: List of data processors
            max_retries: Maximum number of retry attempts
            csv_options: Additional options for pandas read_csv
            unique_id_column: Name of the column to use as unique identifier
            label_column: Name of the column to use as label
            intent_column: Name of the column to use as intent
            annotation_column: Name of the column to use as annotation
        """
        super().__init__(
            database, 
            api_client, 
            table_name, 
            schema, 
            processors, 
            max_retries,
            unique_id_column,
            label_column,
            intent_column,
            annotation_column
        )
        self.csv_options = csv_options or {}
        
    def _validate_csv(self, df: pd.DataFrame) -> None:
        """
        Validate CSV data against schema
        
        Args:
            df: Pandas DataFrame
            
        Raises:
            ValueError: If validation fails
        """
        # Check for required columns
        schema_columns = set(self.schema.keys())
        df_columns = set(df.columns)
        
        missing_columns = schema_columns - df_columns
        if missing_columns:
            raise ValueError(
                f"Missing required columns in CSV: {', '.join(missing_columns)}"
            )
            
        # Basic data type validation
        for column, dtype in self.schema.items():
            try:
                if 'INT' in dtype.upper():
                    pd.to_numeric(df[column], errors='raise')
                elif 'FLOAT' in dtype.upper():
                    pd.to_numeric(df[column], errors='raise')
                elif 'BOOL' in dtype.upper():
                    df[column].astype(bool)
                # Add more type validations as needed
            except Exception as e:
                raise ValueError(
                    f"Data type validation failed for column {column}: {str(e)}"
                )

    def read_data(self, file_path: str) -> Generator[Dict[str, Any], None, None]:
        """
        Read and validate CSV file
        
        Args:
            file_path: Path to the CSV file
            
        Yields:
            Dict containing record data
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
            
        try:
            # Read CSV in chunks for memory efficiency
            chunk_size = self.csv_options.pop('chunk_size', 1000)
            
            default_options = {
                'dtype': 'object',  # Prevent automatic type inference
                'keep_default_na': False,  # Prevent automatic NA handling
                'na_values': [''],  # Only treat empty strings as NA
                'encoding': 'utf-8',
                'on_bad_lines': 'warn'  # Log warning for bad lines
            }
            
            # Merge default options with user-provided options
            csv_options = {**default_options, **self.csv_options}
            
            # Read CSV in chunks
            for chunk in pd.read_csv(
                file_path, 
                chunksize=chunk_size,
                **csv_options
            ):
                if chunk.index[0] == 0:
                    self._validate_csv(chunk)
                    # Validate unique_id_column exists if specified
                    if self.unique_id_column and self.unique_id_column not in chunk.columns:
                        raise ValueError(f"Specified unique_id_column '{self.unique_id_column}' not found in CSV")
                
                chunk.columns = chunk.columns.str.strip()
                
                for _, row in chunk.iterrows():
                    record = row.to_dict()
                    yield record  # Let base class handle the cleaning and unique ID mapping
                    
        except pd.errors.EmptyDataError:
            logger.warning(f"Empty CSV file: {file_path}")
            return
            
        except pd.errors.ParserError as e:
            logger.error(f"Error parsing CSV file: {str(e)}")
            raise
            
        except Exception as e:
            logger.error(f"Unexpected error reading CSV: {str(e)}")
            raise

    def ingest(self, file_path: str, batch_size: int = 50) -> List[Dict[str, Any]]:
        """
        Ingest CSV file with progress tracking
        
        Args:
            file_path: Path to the CSV file
            batch_size: Size of each batch
            
        Returns:
            List of failed records
        """
        logger.info(f"Starting CSV ingestion from {file_path}")
        
        try:
            failed_records = super().ingest(file_path, batch_size)
            
            logger.info(
                f"CSV ingestion completed. "
                f"Failed records: {len(failed_records)}"
            )
            
            return failed_records
            
        except Exception as e:
            logger.error(f"CSV ingestion failed: {str(e)}")
            raise 