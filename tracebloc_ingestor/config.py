from typing import Dict, Any, Optional
import os
from dataclasses import dataclass
import logging

@dataclass
class Config:
    DB_HOST: str = os.getenv("MYSQL_HOST", "localhost")
    DB_PORT: int = 3306
    DB_USER: str = "edgeuser"
    DB_PASSWORD: str = "Edg9@Tr@ce"
    DB_NAME: str = "xraymetadata"
    
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))
    
    # Define API endpoints for different environments
    API_ENDPOINTS = {
        "dev": "https://dev-api.tracebloc.io",
        "stg": "https://stg-api.tracebloc.io",
        "prod": "https://api.tracebloc.io",
        "local": "http://localhost:8000"  # Add local endpoint
    }
    
    # Get environment and set appropriate API endpoint, default to dev
    EDGE_ENV: str = os.getenv("CLIENT_ENV", "dev")
    API_ENDPOINT: str = API_ENDPOINTS.get(EDGE_ENV, API_ENDPOINTS["dev"])
    
    CLIENT_USERNAME: str = os.getenv("CLIENT_ID", "testedge")
    CLIENT_PASSWORD: str = os.getenv("CLIENT_PASSWORD", "&6edg*D9e16 ")
    
    STORAGE_PATH: str = os.getenv("STORAGE_PATH", "/data/shared")
    SRC_PATH: str = os.getenv("SRC_PATH", "templates/image_classification/data") # path to the source data
    DEST_PATH: str = os.path.join(STORAGE_PATH, os.getenv("TABLE_NAME", "")) # path to the destination data with table name
    LABEL_FILE: str = os.getenv("LABEL_FILE", "templates/image_classification/data/labels_file_sample.csv")
    COMPANY: str = os.getenv("COMPANY", "TB_INGESTOR")
    TABLE_NAME: str = os.getenv("TABLE_NAME", "image_ingestor_train")
    TITLE: str = os.getenv("TITLE", "Image training data")
    
    # Logging configuration
    LOG_LEVEL: int = int(os.getenv("LOG_LEVEL", str(logging.WARNING)))
    LOG_FORMAT: Optional[str] = os.getenv("LOG_FORMAT", None)
    LOG_DATE_FORMAT: Optional[str] = os.getenv("LOG_DATE_FORMAT", None)