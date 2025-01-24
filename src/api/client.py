from typing import List, Tuple, Dict, Any
import requests, json
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from ..config import Config
from ..utils.logging import setup_logging

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
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"Authentication response: {response.json()}")
            return response.json().get("token")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error during authentication: {str(e)}")
            raise

    def send_batch(self, records: List[Tuple[int, Dict[str, Any]]], table_name: str) -> bool:
        """
        Send a batch of records to the remote API.
        
        Args:
            records: List of tuples containing (id, record) pairs
            table_name: Name of the table to send data to
            
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
                timeout=30
            )
            
            response.raise_for_status()
            logger.info(f"Successfully sent batch. Response: {response.json()}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending batch to API: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Error response: {e.response.text}")
            return False

    def send_global_meta_meta(self, dataset_type: str, schema: Dict[str, str]) -> bool:
        """
        Sends global metadata, including the schema, to the remote server.
        
        Args:
            dataset_type: The type of the dataset
            schema: A dictionary representing the schema
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            payload = json.dumps({
                "dataset_type": dataset_type,
                "schema": schema
            })

            logger.info(f"Global metadata to send: {json.dumps(payload)}")
            
            headers = {
                "Authorization": f"TOKEN {self.token}",
                "Content-Type": "application/json"
            }
            
            response = self.session.post(
                f"{self.config.API_ENDPOINT}/global_metadata/",
                data=payload,
                headers=headers,
                timeout=30
            )
            
            response.raise_for_status()
            logger.info(f"Successfully sent global metadata. Response: {response.json()}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending global metadata to API: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Error response: {e.response.text}")
            return False

    def send_generate_edge_label_meta(self, dataset_type: str) -> bool:
        """
        Send a request to generate edge label metadata for the specified dataset type.
        
        Args:
            dataset_type: The type of the dataset
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            url = f"{self.config.API_ENDPOINT}/global_meta/generate-edge-labels-meta/?dataset_type={dataset_type}"
            headers = {
                "Authorization": f"TOKEN {self.token}",
                "Content-Type": "application/json"
            }
            
            logger.info(f"Sending request to generate edge label metadata for dataset type: {dataset_type}")
            response = self.session.get(url, headers=headers, timeout=30)
            
            response.raise_for_status()
            logger.info(f"Successfully generated edge label metadata. Response: {response.json()}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error generating edge label metadata: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Error response: {e.response.text}")
            return False

    def __del__(self):
        """Cleanup when the client is destroyed"""
        if hasattr(self, 'session'):
            self.session.close() 