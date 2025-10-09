"""Text File Validator Module.

This module provides validation for text files to ensure they are readable,
have proper encoding, and meet size requirements for text classification tasks.
"""

from pathlib import Path
from typing import Any, List, Optional
import logging
import os

from .base import BaseValidator, ValidationResult
from ..utils.constants import FileExtension, RED, RESET
from ..config import Config
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class TextFileValidator(BaseValidator):
    """Validator for text files in text classification tasks.
    
    This validator checks that text files are readable, have proper encoding,
    meet size requirements, and contain valid text content.
    
    Attributes:
        allowed_extensions: Set of allowed text file extensions
        max_file_size: Maximum file size in bytes
        encoding: Expected text file encoding
    """
    
    def __init__(self, 
                 allowed_extensions: List[str] = None,
                 max_file_size: int = 10 * 1024 * 1024,  # 10MB default
                 encoding: str = "utf-8",
                 name: str = "Text File Validator",
                 path: str = "text_files"
                 ):
        """Initialize the text file validator.
        
        Args:
            allowed_extensions: List of allowed text file extensions
            max_file_size: Maximum file size in bytes
            encoding: Expected text file encoding
            name: Human-readable name of the validator
            path: Subdirectory path for text files
        """
        super().__init__(name)
        self.allowed_extensions = allowed_extensions or [FileExtension.TXT, FileExtension.TEXT]
        self.max_file_size = max_file_size
        self.encoding = encoding
        self.path = path
        
        # Validate extensions
        for ext in self.allowed_extensions:
            if not FileExtension.is_valid_extension(ext):
                raise ValueError(f"{RED}Invalid allowed extension: {ext}{RESET}")
    
    def validate(self, path: Any, **kwargs) -> ValidationResult:
        """Validate text files.
        
        Args:
            path: File path, directory path, or list of file paths to validate
            **kwargs: Additional validation parameters
                - recursive: Whether to search directories recursively (default: True)
                - ignore_hidden: Whether to ignore hidden files (default: True)
                
        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            data = f"{path}/{self.path}"
            recursive = kwargs.get('recursive', True)
            ignore_hidden = kwargs.get('ignore_hidden', True)
            
            # Get list of text files to validate
            files_to_validate = self._get_text_files(data, recursive, ignore_hidden)
            
            if not files_to_validate:
                return self._create_result(
                    is_valid=False,
                    errors=["No text files found to validate"],
                    metadata={'files_checked': 0}
                )
            
            # Validate text files
            return self._validate_text_files(files_to_validate)
            
        except Exception as e:
            logger.error(f"Error during text file validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Validation error: {str(e)}"],
                metadata={'error_type': 'validation_exception'}
            )
    
    def _get_text_files(self, data: Any, recursive: bool, ignore_hidden: bool) -> List[Path]:
        """Get list of text files to validate from the input data.
        
        Args:
            data: Input data (file path, directory, or list of paths)
            recursive: Whether to search directories recursively
            ignore_hidden: Whether to ignore hidden files
            
        Returns:
            List of text file paths to validate
        """
        text_files = []
        
        if isinstance(data, (str, Path)):
            path = Path(data)
            if path.is_file() and self._is_text_file(path):
                text_files.append(path)
            elif path.is_dir():
                pattern = "**/*" if recursive else "*"
                for file_path in path.glob(pattern):
                    if file_path.is_file() and self._is_text_file(file_path):
                        if ignore_hidden and file_path.name.startswith('.'):
                            continue
                        text_files.append(file_path)
            else:
                raise ValueError(f"Path does not exist: {path}")
                
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (str, Path)):
                    path = Path(item)
                    if path.is_file() and self._is_text_file(path):
                        text_files.append(path)
                    else:
                        logger.warning(f"Text file not found: {path}")
                else:
                    logger.warning(f"Invalid file path type: {type(item)}")
        else:
            raise ValueError(f"Unsupported data type for validation: {type(data)}")
        
        return text_files
    
    def _is_text_file(self, file_path: Path) -> bool:
        """Check if a file is a supported text format.
        
        Args:
            file_path: Path to the file to check
            
        Returns:
            True if the file is a supported text format, False otherwise
        """
        return file_path.suffix.lower() in [ext.lower() for ext in self.allowed_extensions]
    
    def _validate_text_files(self, files: List[Path]) -> ValidationResult:
        """Validate text files for content, encoding, and size.
        
        Args:
            files: List of text file paths to validate
            
        Returns:
            ValidationResult containing validation status and messages
        """
        if not files:
            return self._create_result(
                is_valid=False,
                errors=["No text files to validate"],
                metadata={'files_checked': 0}
            )
        
        errors = []
        warnings = []
        valid_files = []
        invalid_files = []
        
        # Create progress bar
        progress_bar = self._create_progress_bar(len(files), "Validating text files")
        
        try:
            for file_path in files:
                file_validation = self._validate_single_text_file(file_path)
                
                if file_validation['is_valid']:
                    valid_files.append(str(file_path))
                    if file_validation.get('warnings'):
                        warnings.extend([f"{file_path}: {w}" for w in file_validation['warnings']])
                else:
                    invalid_files.append(str(file_path))
                    errors.extend([f"{file_path}: {e}" for e in file_validation['errors']])
                
                # Update progress bar
                if progress_bar:
                    progress_bar.update(1)
        finally:
            # Close progress bar
            if progress_bar:
                progress_bar.close()
        
        # Determine overall validation result
        is_valid = len(invalid_files) == 0
        
        return self._create_result(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            metadata={
                'files_checked': len(files),
                'valid_files': len(valid_files),
                'invalid_files': len(invalid_files),
                'valid_file_list': valid_files,
                'invalid_file_list': invalid_files
            }
        )
    
    def _validate_single_text_file(self, file_path: Path) -> dict:
        """Validate a single text file.
        
        Args:
            file_path: Path to the text file to validate
            
        Returns:
            Dictionary with validation results for the single file
        """
        errors = []
        warnings = []
        
        try:
            # Check file size
            file_size = file_path.stat().st_size
            if file_size > self.max_file_size:
                errors.append(f"File size ({file_size} bytes) exceeds maximum allowed size ({self.max_file_size} bytes)")
            
            # Check if file is empty
            if file_size == 0:
                errors.append("File is empty")
                return {'is_valid': False, 'errors': errors, 'warnings': warnings}
            
            # Try to read file with specified encoding
            try:
                with open(file_path, 'r', encoding=self.encoding) as f:
                    content = f.read()
                
                # Check if content is empty after reading
                if not content.strip():
                    warnings.append("File contains only whitespace")
                
                # Check for minimum content length (optional)
                if len(content.strip()) < 10:
                    warnings.append("File content is very short (less than 10 characters)")
                
            except UnicodeDecodeError as e:
                errors.append(f"File encoding error: {str(e)}")
            except Exception as e:
                errors.append(f"Error reading file: {str(e)}")
                
        except OSError as e:
            errors.append(f"File system error: {str(e)}")
        except Exception as e:
            errors.append(f"Unexpected error: {str(e)}")
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
