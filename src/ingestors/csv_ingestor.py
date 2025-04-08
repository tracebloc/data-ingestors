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
        intent: Optional[str] = None,
        annotation_column: Optional[str] = None,
        category: Optional[str] = None
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
            intent: Is the data for training or testing
            annotation_column: Name of the column to use as annotation
            category: Category of the data
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
            intent,
            annotation_column,
            category
        )
        self.csv_options = csv_options or {}
        
    def _validate_csv(self, df: pd.DataFrame) -> None:
        """
        Validate CSV data against schema using pandas functionality
        
        Args:
            df: Pandas DataFrame
            
        Raises:
            ValueError: If validation fails
        """
        # Only validate columns that exist in both schema and CSV
        common_columns = set(self.schema.keys()) & set(df.columns)
        
        # Log which schema columns are not in the CSV (for information only)
        missing_columns = set(self.schema.keys()) - set(df.columns)
        if missing_columns:
            logger.warning(f"Schema columns not present in CSV: {', '.join(missing_columns)}")
            
        # Type validation using pandas dtypes - only for columns that exist in the CSV
        for column in common_columns:
            dtype = self.schema[column]
            try:
                if 'INT' in dtype.upper():
                    df[column] = pd.to_numeric(df[column], downcast='integer')
                elif 'FLOAT' in dtype.upper():
                    df[column] = pd.to_numeric(df[column], downcast='float')
                elif 'BOOL' in dtype.upper():
                    df[column] = df[column].astype('boolean')
                elif 'DATE' in dtype.upper():
                    df[column] = pd.to_datetime(df[column])
                elif 'STRING' in dtype.upper() or 'TEXT' in dtype.upper():
                    df[column] = df[column].astype('string')
            except Exception as e:
                raise ValueError(f"Data type validation failed for column {column}: {str(e)}")

    def read_data(self, file_path: str) -> Generator[Dict[str, Any], None, None]:
        """
        Read and validate CSV file using pandas optimizations
        
        Args:
            file_path: Path to the CSV file
            
        Yields:
            Dict containing record data
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
            
        try:
            chunk_size = self.csv_options.pop('chunk_size', 1000)
            
            # Enhanced default options for pandas
            default_options = {
                'dtype': None,  # Let pandas infer types initially
                'keep_default_na': False,
                'na_values': [''],
                'encoding': 'utf-8',
                'on_bad_lines': 'warn',
                'low_memory': False,  # Prevent mixed type inference warnings
                'engine': 'c'  # Use faster C engine
            }
            
            csv_options = {**default_options, **self.csv_options}
            
            first_chunk = True
            for chunk in pd.read_csv(file_path, chunksize=chunk_size, **csv_options):
                if first_chunk:
                    chunk.columns = chunk.columns.str.strip()
                    self._validate_csv(chunk)
                    if self.unique_id_column and self.unique_id_column not in chunk.columns:
                        raise ValueError(f"Specified unique_id_column '{self.unique_id_column}' not found in CSV")
                    first_chunk = False
                
                # Process each row efficiently using itertuples instead of iterrows
                for row in chunk.itertuples(index=False, name=None):
                    record = dict(zip(chunk.columns, row))
                    yield record
                    
        except pd.errors.EmptyDataError:
            logger.warning(f"Empty CSV file: {file_path}")
            return
            
        except (pd.errors.ParserError, Exception) as e:
            logger.error(f"Error reading CSV file: {str(e)}")
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

    def _count_records(self, file_path: str) -> Optional[int]:
        """
        Count total records in CSV file efficiently using pandas
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Total number of records if countable, None otherwise
        """
        try:
            # Use pandas to count lines efficiently
            return pd.read_csv(file_path).shape[0]
        except Exception as e:
            logger.debug(f"Unable to count CSV records using pandas: {str(e)}")
            return None 