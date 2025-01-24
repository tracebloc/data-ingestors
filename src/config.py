from typing import Dict, Any, Optional
import os
from dataclasses import dataclass
import logging

@dataclass
class Config:
    DB_HOST: str = os.getenv("MYSQL_HOST", "localhost")
    DB_PORT: int = int(os.getenv("MYSQL_PORT", "3306"))
    DB_USER: str = os.getenv("MYSQL_USER", "root")
    DB_PASSWORD: str = os.getenv("MYSQL_PASSWORD", "")
    DB_NAME: str = os.getenv("MYSQL_DATABASE", "ingestor_db")
    
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "10"))
    
    # Define API endpoints for different environments
    API_ENDPOINTS = {
        "dev": "https://dev-api.tracebloc.io",
        "stg": "https://stg-api.tracebloc.io",
        "prod": "https://api.tracebloc.io"
    }
    
    # Get environment and set appropriate API endpoint, default to dev
    EDGE_ENV: str = os.getenv("EDGE_ENV", "dev")
    API_ENDPOINT: str = API_ENDPOINTS.get(EDGE_ENV, API_ENDPOINTS["dev"])
    
    CLIENT_USERNAME: str = os.getenv("EDGE_USERNAME", "")
    CLIENT_PASSWORD: str = os.getenv("EDGE_PASSWORD", "")
    
    STORAGE_PATH: str = os.getenv("STORAGE_PATH", "/data/shared")
    SRC_PATH: str = os.getenv("SRC_PATH", "/data/shared")
    DEST_PATH: str = os.getenv("DEST_PATH", "/data/shared/txt/")
    LABEL_FILE: str = os.getenv("LABEL_FILE", "src/examples/data/sample.csv")
    COMPANY: str = os.getenv("COMPANY", "TB_INGESTOR")
    TABLE_NAME: str = os.getenv("TABLE_NAME", "two_dataset")
    
    # Logging configuration
    LOG_LEVEL: int = int(os.getenv("LOG_LEVEL", str(logging.WARNING)))
    LOG_FORMAT: Optional[str] = os.getenv("LOG_FORMAT", None)
    LOG_DATE_FORMAT: Optional[str] = os.getenv("LOG_DATE_FORMAT", None)