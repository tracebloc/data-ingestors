from typing import Dict, Any, Optional
import os
from dataclasses import dataclass
from .utils.constants import LogLevel


@dataclass
class Config:
    DB_HOST: str = os.getenv("MYSQL_HOST", "localhost")
    DB_PORT: int = 3306
    DB_USER: str = "edgeuser"
    DB_PASSWORD: str = "Edg9@Tr@ce"
    DB_NAME: str = "training_test_datasets"

    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "4000"))

    # Define API endpoints for different environments
    API_ENDPOINTS = {
        "dev": "https://dev-api.tracebloc.io",
        "stg": "https://stg-api.tracebloc.io",
        "prod": "https://api.tracebloc.io",
        "local": "http://localhost:8000",  # Add local endpoint
    }
    STORAGE_PATH = "/data/shared"

    # Get environment and set appropriate API endpoint, default to dev
    EDGE_ENV: str = os.getenv("CLIENT_ENV", "prod")
    API_ENDPOINT: str = API_ENDPOINTS.get(EDGE_ENV, API_ENDPOINTS["dev"])

    CLIENT_USERNAME: str = os.getenv("CLIENT_ID", "testedge")
    CLIENT_PASSWORD: str = os.getenv("CLIENT_PASSWORD", "&6edg*D9e16")

    SRC_PATH: str = os.getenv(
        "SRC_PATH",
        "~/Downloads/data-ingestors/data/crowd_monitoring/dataset_voc_512_mini/train",
    )  # path to the source data
    DEST_PATH: str = os.path.join(
        STORAGE_PATH, os.getenv("TABLE_NAME", "image_ingestor_train")
    )  # path to the destination data with table name
    LABEL_FILE: str = os.getenv(
        "LABEL_FILE",
        "~/Downloads/data-ingestors/data/crowd_monitoring/dataset_voc_512_mini/train/labels_file.csv",
    )
    TABLE_NAME: str = os.getenv("TABLE_NAME", "image_classification_ingestor_train2")
    TITLE: str = os.getenv("TITLE", "DELETE-Object detection training data")

    # Logging configuration
    LOG_LEVEL: int = LogLevel.get_level_code(os.getenv("LOG_LEVEL", "WARNING"))
