from typing import Dict, Any, Optional
import os
from dataclasses import dataclass
from .utils.constants import LogLevel


@dataclass
class Config:
    # ===== Database =====
    # The cluster-internal MySQL is bundled by the tracebloc client and ships
    # with these credentials baked into its image. They never vary per customer,
    # are not exposed outside the cluster, and are connection conventions rather
    # than secrets. Override via env only if you've replaced the bundled MySQL.
    DB_HOST: str = os.getenv("MYSQL_HOST", "localhost")
    DB_PORT: int = int(os.getenv("MYSQL_PORT", "3306"))
    DB_USER: str = os.getenv("DB_USER", "edgeuser")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "Edg9@Tr@ce")
    DB_NAME: str = os.getenv("DB_NAME", "training_test_datasets")

    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "4000"))

    # Define API endpoints for different environments
    API_ENDPOINTS = {
        "dev": "https://dev-api.tracebloc.io",
        "stg": "https://stg-api.tracebloc.io",
        "prod": "https://api.tracebloc.io",
        "local": "http://localhost:8000",  # Add local endpoint
    }
    STORAGE_PATH = "/data/shared"

    # Get environment and set appropriate API endpoint, default to prod.
    # REQUESTS_PROXY_URL — when set, takes precedence over the CLIENT_ENV → API_ENDPOINTS
    # mapping. Set inside the cluster by the Helm subchart (tracebloc/client#86) so the
    # ingestor routes backend traffic through requests-proxy-service:8888 the same way
    # jobs-manager-deployment.yaml does (per tracebloc/client#118-120 and
    # tracebloc/client-runtime#33). Saves the chart from remapping a name the rest of
    # the cluster uses with a different one. An empty value falls back to CLIENT_ENV.
    EDGE_ENV: str = os.getenv("CLIENT_ENV", "prod")
    API_ENDPOINT: str = (
        os.getenv("REQUESTS_PROXY_URL")
        or API_ENDPOINTS.get(EDGE_ENV, API_ENDPOINTS["dev"])
    )

    # ===== Auth =====
    # Preferred: pre-minted token from upstream (e.g. jobs-manager), passed via env.
    # Mirrors the training-pod pattern; no long-lived credentials in the pod.
    BACKEND_TOKEN: Optional[str] = os.getenv("BACKEND_TOKEN")

    # Fallback: username/password. Deprecated — kept for one minor version while
    # callers migrate to BACKEND_TOKEN, then removed in a follow-up.
    CLIENT_USERNAME: Optional[str] = os.getenv("CLIENT_ID")
    CLIENT_PASSWORD: Optional[str] = os.getenv("CLIENT_PASSWORD")

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

    def validate(self) -> None:
        """Fail fast on missing backend authentication.

        Called explicitly by ``APIClient.__init__`` (the boot moment for a
        real run) rather than from ``__post_init__`` so that incidental
        module-level ``Config()`` instantiations elsewhere in the package
        don't blow up at import time.

        In any non-local environment, the pod must boot with either:
          - ``BACKEND_TOKEN`` (preferred), or
          - ``CLIENT_ID`` + ``CLIENT_PASSWORD`` (deprecated fallback).

        Database credentials are intentionally **not** validated here: the
        bundled MySQL container ships with fixed credentials that the ingestor
        defaults match. They're a connection convention, not a secret, and
        forcing customers to set them in env vars adds friction with no
        security benefit.

        Set ``CLIENT_ENV=local`` to bypass for development against a mock backend.

        Raises:
            ValueError: with a single, comma-joined list of missing vars,
                including a hint about ``CLIENT_ENV=local``.
        """
        if self.EDGE_ENV == "local":
            return

        missing = []

        # Backend auth: either pre-minted token, or the deprecated cred pair.
        has_token = bool(self.BACKEND_TOKEN)
        has_creds = bool(self.CLIENT_USERNAME and self.CLIENT_PASSWORD)
        if not has_token and not has_creds:
            missing.append(
                "BACKEND_TOKEN (preferred) or CLIENT_ID + CLIENT_PASSWORD "
                "(deprecated fallback)"
            )

        if missing:
            raise ValueError(
                "Missing required environment variables: "
                + ", ".join(missing)
                + ". Set CLIENT_ENV=local to bypass for development."
            )
