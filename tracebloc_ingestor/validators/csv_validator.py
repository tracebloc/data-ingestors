"""CSV Structure Validator Module.

This module provides validation for CSV file structure, format, and encoding
to ensure proper CSV format before ingestion.
"""

import csv
import logging
from pathlib import Path
from typing import Any, List, Dict, Optional

try:
    import chardet
    CHARDET_AVAILABLE = True
except ImportError:
    CHARDET_AVAILABLE = False
    chardet = None

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class CSVStructureValidator(BaseValidator):
    """Validator for ensuring CSV file format and readability.
    
    This validator checks if the CSV file is in correct format and readable.
    It validates file structure, encoding, and basic CSV format requirements.
    
    Attributes:
        expected_encoding: Expected file encoding (auto-detect if None)
        expected_delimiter: Expected CSV delimiter (auto-detect if None)
    """
    
    def __init__(self, 
                 expected_encoding: Optional[str] = None,
                 expected_delimiter: Optional[str] = None,
                 name: str = "CSV Structure Validator"):
        """Initialize the CSV structure validator.
        
        Args:
            expected_encoding: Expected file encoding (e.g., 'utf-8', 'latin-1')
            expected_delimiter: Expected CSV delimiter (e.g., ',', ';', '\t')
            name: Human-readable name of the validator
        """
        super().__init__(name)
        self.expected_encoding = expected_encoding
        self.expected_delimiter = expected_delimiter
        self.supported_encodings = ['utf-8', 'utf-16', 'latin-1', 'cp1252', 'ascii']
        self.common_delimiters = [',', ';', '\t', '|', ' ']
    
    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate CSV file format and readability.
        
        This method checks if the CSV file is in correct format and readable.
        
        Args:
            data: CSV file path to validate
            **kwargs: Additional validation parameters
                - sample_size: Number of rows to sample for validation (default: 100)
                
        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            sample_size = kwargs.get('sample_size', 100)
            
            # Get CSV file path
            csv_path = self._get_csv_path(data)
            if not csv_path:
                return self._create_result(
                    is_valid=False,
                    errors=["No valid CSV file found to validate"],
                    metadata={'files_checked': 0}
                )
            
            # Validate CSV format and readability
            return self._validate_csv_format(csv_path, sample_size)
            
        except Exception as e:
            logger.error(f"Error during CSV format validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Validation error: {str(e)}"],
                metadata={'error_type': 'validation_exception'}
            )
    
    def _get_csv_path(self, data: Any) -> Optional[Path]:
        """Get CSV file path from input data.
        
        Args:
            data: Input data (file path, directory, or list of paths)
            
        Returns:
            Path to CSV file if found, None otherwise
        """
        if isinstance(data, (str, Path)):
            path = Path(data)
            if path.is_file() and path.suffix.lower() == '.csv':
                return path
            elif path.is_dir():
                # Look for CSV files in directory
                csv_files = list(path.glob('*.csv'))
                if csv_files:
                    return csv_files[0]  # Return first CSV file found
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (str, Path)):
                    path = Path(item)
                    if path.is_file() and path.suffix.lower() == '.csv':
                        return path
        
        return None
    
    def _validate_csv_format(self, csv_path: Path, sample_size: int) -> ValidationResult:
        """Validate CSV file format and readability.
        
        This method checks if the CSV file is in correct format and readable.
        
        Args:
            csv_path: Path to the CSV file
            sample_size: Number of rows to sample for validation
            
        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            # 1. Check if file is readable
            if not csv_path.exists():
                return self._create_result(
                    is_valid=False,
                    errors=[f"CSV file does not exist: {csv_path}"],
                    metadata={'file_path': str(csv_path)}
                )
            
            if not csv_path.is_file():
                return self._create_result(
                    is_valid=False,
                    errors=[f"Path is not a file: {csv_path}"],
                    metadata={'file_path': str(csv_path)}
                )
            
            # 2. Check file extension
            if csv_path.suffix.lower() != '.csv':
                return self._create_result(
                    is_valid=False,
                    errors=[f"File does not have .csv extension: {csv_path}"],
                    metadata={'file_path': str(csv_path), 'extension': csv_path.suffix}
                )
            
            # 3. Detect and validate encoding
            encoding_result = self._detect_encoding(csv_path)
            if not encoding_result['is_valid']:
                return self._create_result(
                    is_valid=False,
                    errors=encoding_result['errors'],
                    metadata={'file_path': str(csv_path)}
                )
            
            detected_encoding = encoding_result['encoding']
            
            # 4. Detect and validate delimiter
            delimiter_result = self._detect_delimiter(csv_path, detected_encoding)
            if not delimiter_result['is_valid']:
                return self._create_result(
                    is_valid=False,
                    errors=delimiter_result['errors'],
                    metadata={'file_path': str(csv_path), 'encoding': detected_encoding}
                )
            
            detected_delimiter = delimiter_result['delimiter']
            
            # 5. Validate CSV content structure
            content_result = self._validate_csv_content(csv_path, detected_encoding, detected_delimiter, sample_size)
            if not content_result['is_valid']:
                return self._create_result(
                    is_valid=False,
                    errors=content_result['errors'],
                    metadata={
                        'file_path': str(csv_path),
                        'encoding': detected_encoding,
                        'delimiter': detected_delimiter
                    }
                )
            
            # Success case - file is in correct format and readable
            return self._create_result(
                is_valid=True,
                warnings=content_result.get('warnings', []),
                metadata={
                    'file_path': str(csv_path),
                    'encoding': detected_encoding,
                    'delimiter': detected_delimiter,
                    'rows_checked': content_result['rows_checked'],
                    'columns_detected': content_result['columns_detected'],
                    'file_readable': True,
                    'format_valid': True
                }
            )
            
        except Exception as e:
            logger.error(f"Error validating CSV format for {csv_path}: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"CSV format validation failed: {str(e)}"],
                metadata={'file_path': str(csv_path), 'error_type': 'validation_exception'}
            )
    
    def _detect_encoding(self, csv_path: Path) -> Dict[str, Any]:
        """Detect file encoding.
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Dictionary with encoding detection results
        """
        try:
            if not CHARDET_AVAILABLE:
                # Fallback to utf-8 if chardet is not available
                return {
                    'is_valid': True,
                    'encoding': 'utf-8',
                    'confidence': 1.0
                }
            
            # Read a sample of the file to detect encoding
            with open(csv_path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB
            
            # Use chardet to detect encoding
            detected = chardet.detect(raw_data)
            detected_encoding = detected.get('encoding', 'utf-8').lower()
            confidence = detected.get('confidence', 0)
            
            # Validate detected encoding
            if confidence < 0.7:
                logger.warning(f"Low confidence ({confidence:.2f}) in encoding detection: {detected_encoding}")
            
            # Check if encoding is supported
            if detected_encoding not in self.supported_encodings:
                return {
                    'is_valid': False,
                    'errors': [f"Unsupported encoding detected: {detected_encoding}. Supported encodings: {self.supported_encodings}"],
                    'encoding': detected_encoding
                }
            
            # Check against expected encoding if specified
            if self.expected_encoding and detected_encoding != self.expected_encoding.lower():
                return {
                    'is_valid': False,
                    'errors': [f"Encoding mismatch. Expected: {self.expected_encoding}, Detected: {detected_encoding}"],
                    'encoding': detected_encoding
                }
            
            return {
                'is_valid': True,
                'encoding': detected_encoding,
                'confidence': confidence
            }
            
        except Exception as e:
            return {
                'is_valid': False,
                'errors': [f"Error detecting encoding: {str(e)}"],
                'encoding': 'unknown'
            }
    
    def _detect_delimiter(self, csv_path: Path, encoding: str) -> Dict[str, Any]:
        """Detect CSV delimiter.
        
        Args:
            csv_path: Path to the CSV file
            encoding: File encoding
            
        Returns:
            Dictionary with delimiter detection results
        """
        try:
            # Read first few lines to detect delimiter
            with open(csv_path, 'r', encoding=encoding) as f:
                sample = f.read(1024)  # Read first 1KB
            
            # Use csv.Sniffer to detect delimiter
            sniffer = csv.Sniffer()
            try:
                detected_delimiter = sniffer.sniff(sample).delimiter
            except csv.Error:
                # Fallback to comma if sniffer fails
                detected_delimiter = ','
            
            # Check against expected delimiter if specified
            if self.expected_delimiter and detected_delimiter != self.expected_delimiter:
                return {
                    'is_valid': False,
                    'errors': [f"Delimiter mismatch. Expected: '{self.expected_delimiter}', Detected: '{detected_delimiter}'"],
                    'delimiter': detected_delimiter
                }
            
            return {
                'is_valid': True,
                'delimiter': detected_delimiter
            }
            
        except Exception as e:
            return {
                'is_valid': False,
                'errors': [f"Error detecting delimiter: {str(e)}"],
                'delimiter': 'unknown'
            }
    
    def _validate_csv_content(self, csv_path: Path, encoding: str, delimiter: str, sample_size: int) -> Dict[str, Any]:
        """Validate CSV content structure for readability.
        
        Args:
            csv_path: Path to the CSV file
            encoding: File encoding
            delimiter: CSV delimiter
            sample_size: Number of rows to sample
            
        Returns:
            Dictionary with content validation results
        """
        try:
            rows_checked = 0
            columns_detected = 0
            warnings = []
            
            with open(csv_path, 'r', encoding=encoding) as f:
                reader = csv.reader(f, delimiter=delimiter)
                
                # Read header
                try:
                    header = next(reader)
                    columns_detected = len(header)
                    rows_checked += 1
                    
                    if not header or all(not col.strip() for col in header):
                        return {
                            'is_valid': False,
                            'errors': ["Empty or invalid header row found"],
                            'rows_checked': rows_checked,
                            'columns_detected': columns_detected
                        }
                    
                except StopIteration:
                    return {
                        'is_valid': False,
                        'errors': ["Empty CSV file"],
                        'rows_checked': 0,
                        'columns_detected': 0
                    }
                
                # Check data rows for basic structure
                for row_num, row in enumerate(reader, start=2):  # Start from row 2 (after header)
                    rows_checked += 1
                    
                    # Check for consistent column count
                    if len(row) != columns_detected:
                        return {
                            'is_valid': False,
                            'errors': [f"Inconsistent column count at row {row_num}. Expected: {columns_detected}, Found: {len(row)}"],
                            'rows_checked': rows_checked,
                            'columns_detected': columns_detected
                        }
                    
                    # Stop after sample_size rows
                    if rows_checked >= sample_size:
                        break
                
                # Check if file has at least header + 1 data row
                if rows_checked < 2:
                    return {
                        'is_valid': False,
                        'errors': ["CSV file must have at least a header row and one data row"],
                        'rows_checked': rows_checked,
                        'columns_detected': columns_detected
                    }
            
            return {
                'is_valid': True,
                'warnings': warnings,
                'rows_checked': rows_checked,
                'columns_detected': columns_detected
            }
            
        except Exception as e:
            return {
                'is_valid': False,
                'errors': [f"Error reading CSV content: {str(e)}"],
                'rows_checked': rows_checked,
                'columns_detected': columns_detected
            }
