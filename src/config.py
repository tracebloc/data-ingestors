from typing import Dict, Any
import os
from dataclasses import dataclass

@dataclass
class Config:
    DB_HOST: str = os.getenv("MYSQL_HOST", "localhost")
    DB_PORT: int = int(os.getenv("MYSQL_PORT", "3306"))
    DB_USER: str = os.getenv("MYSQL_USER", "root")
    DB_PASSWORD: str = os.getenv("MYSQL_PASSWORD", "")
    DB_NAME: str = os.getenv("MYSQL_DATABASE", "ingestor_db")
    
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "50"))
    API_ENDPOINT: str = os.getenv("API_ENDPOINT", "https://dev-api.tracebloc.io")
    CLIENT_USERNAME: str = os.getenv("EDGE_USERNAME", "testedge")
    CLIENT_PASSWORD: str = os.getenv("EDGE_PASSWORD", "&6edg*D9e")
    
    STORAGE_PATH: str = os.getenv("STORAGE_PATH", "/data/shared")
    SRC_PATH: str = os.getenv("SRC_PATH", "/data/shared")
    DEST_PATH: str = os.getenv("DEST_PATH", "/data/shared/txt/")
    LABEL_FILE: str = os.getenv("LABEL_FILE", "src/examples/data/sample.csv")
    COMPANY: str = os.getenv("COMPANY", "TB_INGESTOR")
    TABLE_NAME: str = os.getenv("TABLE_NAME", "tb_dataset")