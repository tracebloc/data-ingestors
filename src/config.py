from typing import Dict, Any
import os
from dataclasses import dataclass

@dataclass
class Config:
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "3307"))
    DB_USER: str = os.getenv("DB_USER", "root")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
    DB_NAME: str = os.getenv("DB_NAME", "ingestor_db")
    
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "50"))
    API_ENDPOINT: str = os.getenv("API_ENDPOINT", "https://dev-api.tracebloc.io")
    CLIENT_USERNAME: str = os.getenv("CLIENT_USERNAME", "testedge")
    CLIENT_PASSWORD: str = os.getenv("CLIENT_PASSWORD", "&6edg*D9e")
    
    STORAGE_PATH: str = os.getenv("STORAGE_PATH", "./storage")
    TEXT_FILES_PATH: str = os.path.join(STORAGE_PATH, "text_files")
    IMAGE_FILES_PATH: str = os.path.join(STORAGE_PATH, "images") 