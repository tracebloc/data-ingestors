from typing import List, Tuple, Dict, Any
import requests, json
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from ..config import Config
from ..utils.logging import setup_logging
from ..utils.constants import (
    TaskCategory,
    API_TIMEOUT,
    RESET,
    BOLD,
    GREEN,
    RED,
    YELLOW,
    BLUE,
    CYAN,
)

# Configure unified logging with config
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class LoggingRetry(Retry):
    def increment(self, *args, **kwargs):
        new_retry = super().increment(*args, **kwargs)
        # Print or log the retry number
        print(
            f"{BOLD}{YELLOW}Retrying {kwargs.get('url', '')} (attempt {self.total - new_retry.total}){RESET}"
        )
        return new_retry


class APIClient:
    def __init__(self, config: Config):
        self.config = config
        self.session = self._create_session()
        # Only authenticate if not in local mode
        if config.EDGE_ENV != "local":
            self.token = self.authenticate()
        else:
            self.token = "mock_token"
            logger.info("Skipping API authentication for local mode")

    def _create_session(self) -> requests.Session:
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = LoggingRetry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def authenticate(self) -> str:
        """Authenticate and return the token."""
        try:
            response = self.session.post(
                f"{self.config.API_ENDPOINT}/api-token-auth/",
                json={
                    "username": self.config.CLIENT_USERNAME,
                    "password": self.config.CLIENT_PASSWORD,
                },
                timeout=API_TIMEOUT,
            )
            # Check status after retries are exhausted
            if response.status_code >= 400:
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}: {response.text}"
                )
            print(f"{BOLD}{GREEN}Authentication successful{RESET}")
            return response.json().get("token")

        except requests.exceptions.RequestException as e:
            if hasattr(e.response, "text"):
                raise ValueError(
                    f"{RED}Authentication failed: {e.response.text}{RESET}"
                )
            else:
                raise ValueError(f"{RED}Error response: {e}{RESET}")

    def send_batch(
        self,
        records: List[Tuple[int, Dict[str, Any]]],
        table_name: str,
        ingestor_id: str,
    ) -> bool:
        """
        Send a batch of records to the remote API.

        Args:
            records: List of tuples containing (id, record) pairs
            table_name: Name of the table to send data to
            ingestor_id: Unique ID for the ingestor
        Returns:
            bool: True if successful, False otherwise
        """
        # Skip API calls in local mode
        if self.config.EDGE_ENV == "local":
            logger.info(f"Mock: Would send {len(records)} records to API")
            return True

        try:
            payload = json.dumps(
                [
                    {
                        "data_id": record_data.get("data_id"),
                        "data_intent": record_data.get("data_intent", "train"),
                        "label": record_data.get("label", ""),
                        "is_sample": False,
                        "injestor_id": ingestor_id,
                    }
                    for _, record_data in records
                ]
            )

            logger.info(f"Data to send: {payload}")

            headers = {
                "Authorization": f"TOKEN {self.token}",
                "Content-Type": "application/json",
            }

            response = self.session.post(
                f"{self.config.API_ENDPOINT}/global_meta/{table_name}/",
                data=payload,
                headers=headers,
                timeout=API_TIMEOUT,
            )

            # Check status after retries are exhausted
            if response.status_code >= 400:
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}: {response.text}"
                )
            return True

        except requests.exceptions.RequestException as e:
            if hasattr(e.response, "text"):
                logger.error(f"{RED}Error response: {e.response.text}{RESET}")
            else:
                logger.error(f"{RED}Error sending batch to API: {str(e)[:100]}{RESET}")
            return False

    def send_global_meta_meta(
        self, table_name: str, schema: Dict[str, str], add_info
    ) -> bool:
        """
        Sends global metadata, including the schema, to the remote server.

        Args:
            table_name: The type of the dataset
            schema: A dictionary representing the schema

        Returns:
            bool: True if successful, False otherwise
        """
        # Skip API calls in local mode
        if self.config.EDGE_ENV == "local":
            logger.info(f"Mock: Would send schema for {table_name}")
            return True

        try:
            payload = json.dumps(
                {
                    "table_name": table_name,
                    "schema": schema,
                    "meta_data": add_info,
                }
            )

            logger.info(f"Global metadata to send: {(payload)}")

            headers = {
                "Authorization": f"TOKEN {self.token}",
                "Content-Type": "application/json",
            }

            response = self.session.post(
                f"{self.config.API_ENDPOINT}/global_meta/global_metadata/",
                data=payload,
                headers=headers,
                timeout=API_TIMEOUT,
            )

            # Check status after retries are exhausted
            if response.status_code >= 400:
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}: {response.text}"
                )
            logger.info(
                f"{GREEN}Successfully sent global metadata. Response: {response.json()}{RESET}"
            )
            return True

        except requests.exceptions.RequestException as e:
            logger.error(
                f"{RED}Error sending global metadata to API: {str(e)[:100]}{RESET}"
            )
            if hasattr(e.response, "text"):
                logger.error(f"{RED}Error response: {e.response.text}{RESET}")
            return False

    def send_generate_edge_label_meta(
        self, table_name: str, ingestor_id: str, intent: str
    ) -> bool:
        """
        Send a request to generate edge label metadata for the specified dataset type.

        Args:
            table_name: The type of the dataset

        Returns:
            bool: True if successful, False otherwise
        """
        # Skip API calls in local mode
        if self.config.EDGE_ENV == "local":
            logger.info(f"Mock: Would generate edge labels for {table_name}")
            return True

        try:
            url = f"{self.config.API_ENDPOINT}/global_meta/generate-edge-labels-meta/?table_name={table_name}&injestor_id={ingestor_id}&data_intent={intent}"
            headers = {"Authorization": f"TOKEN {self.token}"}

            logger.info(
                f"Sending request to generate edge label metadata for dataset type: {table_name}"
            )
            response = self.session.get(url, headers=headers, timeout=API_TIMEOUT)

            # Check status after retries are exhausted
            if response.status_code >= 400:
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}: {response.text}"
                )
            logger.info(
                f"{GREEN}Successfully generated edge label metadata. Response{RESET}"
            )
            return True

        except requests.exceptions.RequestException as e:
            logger.error(
                f"{RED}Error generating edge label metadata: {str(e)[:100]}{RESET}"
            )
            if hasattr(e.response, "text"):
                logger.error(f"{RED}Error response: {e.response.text}{RESET}")
            return False

    def prepare_dataset(
        self, category: str, ingestor_id: str, data_format: str, intent: str
    ) -> bool:
        """
        Prepare data for a specific category and ingestor.

        Args:
            category: The category of data (must be one of TaskCategory values)
            injester_id: The unique identifier for the injester
            data_format: The format of the data

        Returns:
            bool: True if successful, False otherwise
        """
        # Skip API calls in local mode
        if self.config.EDGE_ENV == "local":
            logger.info(f"Mock: Would prepare dataset {category}")
            return True

        if not TaskCategory.is_valid_category(category):
            print(
                f"return {TaskCategory.is_valid_category(category)} for input : {category}"
            )
            logger.error(f"Invalid category: {category}")
            return False

        try:
            url = f"{self.config.API_ENDPOINT}/global_meta/prepare/?category={category}&injestor_id={ingestor_id}&data_format={data_format}&data_intent={intent}"
            headers = {"Authorization": f"TOKEN {self.token}"}

            logger.info(
                f"Sending prepare request for category: {category}, injester_id: {ingestor_id}, data_format: {data_format} , data_intent: {intent}"
            )
            response = self.session.get(url, headers=headers, timeout=API_TIMEOUT)

            # Check status after retries are exhausted
            if response.status_code >= 400:
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}: {response.text}"
                )
            logger.info(
                f"{GREEN}Successfully prepared data. Response: {response.json()}{RESET}"
            )
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"{RED}Error preparing data: {str(e)[:100]}{RESET}")
            if hasattr(e.response, "text"):
                logger.error(f"{RED}Error response: {e.response.text}{RESET}")
            return False

    def create_dataset(
        self,
        allow_feature_modification: bool = False,
        ingestor_id: str = None,
        category: str = None,
    ) -> Dict[str, Any]:
        """
        Create a new dataset with the specified parameters.

        Args:
            title: The title of the dataset (if None, will be generated from category and ingestor_id)
            allow_feature_modification: Whether feature modification is allowed
            ingestor_id: The unique identifier for the ingestor

        Returns:
            Dict[str, Any]: The created dataset information if successful

        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        # Skip API calls in local mode
        if self.config.EDGE_ENV == "local":
            logger.info(f"Mock: Would create dataset {category}")
            return {"id": "mock_dataset_id", "title": "Mock Dataset"}

        try:
            # Generate title from category and ingestor_id if not provided
            if config.TITLE is None:
                title = f"{category}_{ingestor_id}"
            else:
                title = config.TITLE  # Fallback to config title if no ingestor_id

            if category == TaskCategory.TABULAR_CLASSIFICATION:
                allow_feature_modification = True
            else:
                allow_feature_modification = False

            payload = json.dumps(
                {
                    "title": title,
                    "allow_feature_modification": allow_feature_modification,
                }
            )

            logger.info(f"{GREEN}Creating dataset with payload: {payload}{RESET}")

            headers = {
                "Authorization": f"TOKEN {self.token}",
                "Content-Type": "application/json",
            }

            response = self.session.post(
                f"{self.config.API_ENDPOINT}/dataset/",
                data=payload,
                headers=headers,
                timeout=API_TIMEOUT,
            )

            # Check status after retries are exhausted
            if response.status_code >= 400:
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}: {response.text}"
                )
            logger.info(
                f"{GREEN}Successfully created dataset. Response: {response.json()}{RESET}"
            )
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"{RED}Error creating dataset: {str(e)[:100]}{RESET}")
            if hasattr(e.response, "text"):
                logger.error(f"{RED}Error response: {e.response.text}{RESET}")
            raise

    def __del__(self):
        """Cleanup when the client is destroyed"""
        if hasattr(self, "session"):
            self.session.close()
