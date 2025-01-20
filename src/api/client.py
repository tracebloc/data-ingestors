from typing import List, Tuple, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from ..config import Config

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
            return response.json().get("token")
        except requests.exceptions.RequestException as e:
            print(f"Error during authentication: {str(e)}")
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
            payload = [
                {
                    "id": record_id,
                    "data": record_data
                }
                for record_id, record_data in records
            ]

            print(f"Sending token: {self.token}")
            
            headers = {"Authorization": f"TOKEN {self.token}"}
            
            response = self.session.post(
                f"{self.config.API_ENDPOINT}/global_meta/{table_name}/",
                json=payload,
                headers=headers,
                timeout=30
            )
            
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            # In a production environment, we might use the azure SB to send the error message
            print(f"Error sending batch to API: {str(e)}")
            return False

    def __del__(self):
        """Cleanup when the client is destroyed"""
        if hasattr(self, 'session'):
            self.session.close() 