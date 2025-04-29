from typing import List, Tuple, Dict, Any
import requests, json
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from ..config import Config
from ..utils.logging import setup_logging
from ..utils.constants import DataCategory, API_TIMEOUT

# Configure unified logging with config
config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)

class APIClient:
    def __init__(self, config: Config):
        self.config = config
        self.session = self._create_session()
        self.token = self.authenticate()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
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
                json={"username": self.config.CLIENT_USERNAME, "password": self.config.CLIENT_PASSWORD},
                timeout=API_TIMEOUT
            )
            response.raise_for_status()
            logger.info(f"Authentication response: {response.json()}")
            return response.json().get("token")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error during authentication: {str(e)}")
            raise

    def send_batch(self, records: List[Tuple[int, Dict[str, Any]]], table_name: str, ingestor_id: str) -> bool:
        """
        Send a batch of records to the remote API.
        
        Args:
            records: List of tuples containing (id, record) pairs
            table_name: Name of the table to send data to
            ingestor_id: Unique ID for the ingestor
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            payload = json.dumps([
                {
                    "data_id": record_data.get("data_id"),
                    "company": self.config.COMPANY,
                    "data_intent": record_data.get("data_intent", "train"),
                    "label": record_data.get("label", ""),
                    "is_sample": False,
                    "is_active": True,
                    "injestor_id": ingestor_id,
                    # "data": record_data
                }
                for _, record_data in records
            ])

            logger.info(f"Data to send: {payload}")
            
            headers = {
                "Authorization": f"TOKEN {self.token}",
                "Content-Type": "application/json"
            }
            
            response = self.session.post(
                f"{self.config.API_ENDPOINT}/global_meta/{table_name}/",
                data=payload,
                headers=headers,
                timeout=API_TIMEOUT
            )
            
            response.raise_for_status()
            logger.info(f"Successfully sent batch. Response: {response.json()}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending batch to API: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Error response: {e.response.text}")
            return False

    def send_global_meta_meta(self, table_name: str, schema: Dict[str, str]) -> bool:
        """
        Sends global metadata, including the schema, to the remote server.
        
        Args:
            table_name: The type of the dataset
            schema: A dictionary representing the schema
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            payload = json.dumps({
                "table_name": table_name,
                "schema": schema
            })

            logger.info(f"Global metadata to send: {(payload)}")
            
            headers = {
                "Authorization": f"TOKEN {self.token}",
                "Content-Type": "application/json"
            }

            response = self.session.post(
                f"{self.config.API_ENDPOINT}/global_meta/global_metadata/",
                data=payload,
                headers=headers,
                timeout=API_TIMEOUT
            )
            
            response.raise_for_status()
            logger.info(f"Successfully sent global metadata. Response: {response.json()}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending global metadata to API: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Error response: {e.response.text}")
            return False

    def send_generate_edge_label_meta(self, table_name: str, ingestor_id: str) -> bool:
        """
        Send a request to generate edge label metadata for the specified dataset type.
        
        Args:
            table_name: The type of the dataset
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            url = f"{self.config.API_ENDPOINT}/global_meta/generate-edge-labels-meta/?table_name={table_name}&injestor_id={ingestor_id}"
            headers = {
                "Authorization": f"TOKEN {self.token}"
            }
            
            logger.info(f"Sending request to generate edge label metadata for dataset type: {table_name}")
            response = self.session.get(url, headers=headers, timeout=API_TIMEOUT)
            
            response.raise_for_status()
            logger.info(f"Successfully generated edge label metadata. Response")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error generating edge label metadata: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Error response: {e.response.text}")
            return False

    def prepare_dataset(self, category: str, ingestor_id: str) -> bool:
        """
        Prepare data for a specific category and ingestor.
        
        Args:
            category: The category of data (must be one of DataCategory values)
            injester_id: The unique identifier for the injester
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not DataCategory.is_valid_category(category):
            logger.error(f"Invalid category: {category}")
            return False
            
        try:
            url = f"{self.config.API_ENDPOINT}/global_meta/prepare/?category={category}&injestor_id={ingestor_id}"
            headers = {
                "Authorization": f"TOKEN {self.token}"
            }
            
            logger.info(f"Sending prepare request for category: {category}, injester_id: {ingestor_id}")
            response = self.session.get(url, headers=headers, timeout=API_TIMEOUT)
            
            response.raise_for_status()
            logger.info(f"Successfully prepared data. Response: {response.json()}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error preparing data: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Error response: {e.response.text}")
            return False

    def create_dataset(self, requires_gpu: bool = False, allow_feature_modification: bool = False, ingestor_id: str = None, category: str = None) -> Dict[str, Any]:
        """
        Create a new dataset with the specified parameters.
        
        Args:
            title: The title of the dataset (if None, will be generated from category and ingestor_id)
            requires_gpu: Whether the dataset requires GPU processing
            allow_feature_modification: Whether feature modification is allowed
            ingestor_id: The unique identifier for the ingestor
            
        Returns:
            Dict[str, Any]: The created dataset information if successful
            
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        try:
            # Generate title from category and ingestor_id if not provided
            if config.TITLE is None:
                title = f"{category}_{ingestor_id}"
            else:
                title = config.TITLE  # Fallback to config title if no ingestor_id

            if category == DataCategory.TABULAR_CLASSIFICATION:
                allow_feature_modification = True
            else:
                allow_feature_modification = False
            
            payload = json.dumps({
                "title": title,
                "requires_gpu": requires_gpu,
                "allow_feature_modification": allow_feature_modification
            })

            logger.info(f"Creating dataset with payload: {payload}")
            
            headers = {
                "Authorization": f"TOKEN {self.token}",
                "Content-Type": "application/json"
            }
            
            response = self.session.post(
                f"{self.config.API_ENDPOINT}/dataset/",
                data=payload,
                headers=headers,
                timeout=API_TIMEOUT
            )
            
            response.raise_for_status()
            logger.info(f"Successfully created dataset. Response: {response.json()}")
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating dataset: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Error response: {e.response.text}")
            raise

    def __del__(self):
        """Cleanup when the client is destroyed"""
        if hasattr(self, 'session'):
            self.session.close() 