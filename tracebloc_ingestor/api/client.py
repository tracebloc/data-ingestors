from typing import List, Tuple, Dict, Any, Optional
import os
import requests, json
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from ..config import Config
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

# Logger for this module. Level is set by `setup_logging()` on the root
# logger when the user script calls it; child loggers inherit that level.
logger = logging.getLogger(__name__)


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
        # Fail fast on missing creds before any network or session setup.
        # `validate()` is a no-op when EDGE_ENV == "local".
        config.validate()

        self.config = config
        self.session = self._create_session()

        # Auth resolution order:
        #   1. local mode  → mock token, no network call
        #   2. BACKEND_TOKEN set → use it directly (preferred; mirrors the
        #      training-pod pattern via jobs-manager)
        #   3. CLIENT_ID + CLIENT_PASSWORD → fall back to /api-token-auth/
        #      (deprecated; kept for one minor version while callers migrate)
        if config.EDGE_ENV == "local":
            self.token = "mock_token"
            logger.info("Skipping API authentication for local mode")
        elif config.BACKEND_TOKEN:
            self.token = config.BACKEND_TOKEN
            logger.info(
                f"{GREEN}Using pre-minted BACKEND_TOKEN; skipping /api-token-auth/{RESET}"
            )
        else:
            logger.warning(
                f"{YELLOW}CLIENT_ID/CLIENT_PASSWORD auth is deprecated and will be "
                f"removed in a future release. Inject BACKEND_TOKEN via env instead.{RESET}"
            )
            self.token = self.authenticate()

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

    @staticmethod
    def _parse_json(response, *, required: bool):
        """Parse a JSON response body, turning a non-JSON 200 (an HTML error
        page, an empty body, a proxy interstitial) into a *handled* outcome
        instead of an opaque JSONDecodeError mid-ingest.

        ``required=True`` (the body IS the result — an auth token or a created
        dataset) raises a clear ValueError. ``required=False`` (we only log the
        body) warns and returns ``{}`` so a successful call isn't flipped to a
        false failure just because its response wasn't JSON.
        """
        try:
            return response.json()
        except ValueError:  # json + requests JSONDecodeError both subclass ValueError
            snippet = (response.text or "")[:200]
            msg = (
                f"Backend returned a non-JSON response "
                f"(HTTP {response.status_code}): {snippet!r}"
            )
            if required:
                raise ValueError(f"{RED}{msg}{RESET}")
            logger.warning(f"{YELLOW}{msg}{RESET}")
            return {}

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
            return self._parse_json(response, required=True).get("token")

        except requests.exceptions.RequestException as e:
            if hasattr(e.response, "text"):
                raise ValueError(
                    f"{RED}Authentication failed: {e.response.text}{RESET}"
                )
            else:
                raise ValueError(f"{RED}Error response: {e}{RESET}")

    def _refresh_token(self) -> bool:
        """Re-mint or re-read the auth token (#772 P2 — token captured
        once, expires on multi-hour runs).

        Three resolution paths matching ``__init__``:
          - local mode: keep the mock token (no real auth in the loop)
          - BACKEND_TOKEN: re-read ``os.environ`` in case jobs-manager /
            secret-rotator wrote a fresh value into the env. (Re-reading
            ``self.config.BACKEND_TOKEN`` first picks up Config layer
            overrides; the env fallback covers rotation.)
          - CLIENT_ID/PASSWORD (deprecated): re-call ``authenticate()``
            to mint a new short-lived token.

        Returns True iff the token actually changed; False signals
        "tried to refresh but got the same value, nothing more we can
        do" — caller should treat the next 401 as terminal.
        """
        if self.config.EDGE_ENV == "local":
            return False
        old = self.token
        if self.config.BACKEND_TOKEN:
            # Re-resolve via Config (which reads the env each time) so a
            # rotated BACKEND_TOKEN is picked up between attempts.
            new = self.config.BACKEND_TOKEN or os.environ.get("BACKEND_TOKEN")
            if new and new != old:
                self.token = new
                logger.info(
                    f"{GREEN}Re-read rotated BACKEND_TOKEN after 401.{RESET}"
                )
                return True
            return False
        # CLIENT_ID/PASSWORD path: mint a new token.
        try:
            self.token = self.authenticate()
            return self.token != old
        except Exception as exc:
            logger.error(
                f"{RED}Failed to re-authenticate after 401: {exc}{RESET}"
            )
            return False

    def _authed_request(
        self,
        method: str,
        url: str,
        *,
        extra_headers: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Issue an authenticated HTTP request with a single 401-refresh
        retry (#772 P2).

        Auth tokens captured at ``__init__`` used to expire silently on
        multi-hour runs — the terminal ``create_dataset`` then failed
        4xx, the ingest exited non-zero, and the rows were left
        committed-but-unregistered. Now: on a 401, attempt one token
        refresh and retry the request once. If the second attempt still
        401s, the caller's existing error path runs as before.

        Caller passes whatever ``session.request`` kwargs are needed
        (json, data, params, timeout, …). The Authorization header is
        injected here and overrides anything in ``extra_headers``.
        """
        headers = dict(extra_headers or {})
        headers["Authorization"] = f"TOKEN {self.token}"
        # Dispatch by method name (not session.request) so existing tests
        # that monkeypatch ``session.post`` / ``session.get`` directly
        # continue to work without rewrites.
        send = getattr(self.session, method.lower())
        response = send(url, headers=headers, **kwargs)
        if response.status_code != 401:
            return response
        logger.warning(
            f"{YELLOW}Backend returned 401 for {method} {url} — attempting "
            f"token refresh and one retry.{RESET}"
        )
        if not self._refresh_token():
            # Refresh did nothing; the second attempt would 401 again.
            # Surface the original 401 so the caller's existing error
            # path runs (it already logs the response body).
            return response
        headers["Authorization"] = f"TOKEN {self.token}"
        return send(url, headers=headers, **kwargs)

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

            response = self._authed_request(
                "POST",
                f"{self.config.API_ENDPOINT}/global_meta/{table_name}/",
                data=payload,
                extra_headers={"Content-Type": "application/json"},
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

            response = self._authed_request(
                "POST",
                f"{self.config.API_ENDPOINT}/global_meta/global_metadata/",
                data=payload,
                extra_headers={"Content-Type": "application/json"},
                timeout=API_TIMEOUT,
            )

            # Check status after retries are exhausted
            if response.status_code >= 400:
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}: {response.text}"
                )
            logger.info(
                f"{GREEN}Successfully sent global metadata. "
                f"Response: {self._parse_json(response, required=False)}{RESET}"
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

            logger.info(
                f"Sending request to generate edge label metadata for dataset type: {table_name}"
            )
            response = self._authed_request("GET", url, timeout=API_TIMEOUT)

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

            logger.info(
                f"Sending prepare request for category: {category}, injester_id: {ingestor_id}, data_format: {data_format} , data_intent: {intent}"
            )
            response = self._authed_request("GET", url, timeout=API_TIMEOUT)

            # Check status after retries are exhausted
            if response.status_code >= 400:
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}: {response.text}"
                )
            logger.info(
                f"{GREEN}Successfully prepared data. "
                f"Response: {self._parse_json(response, required=False)}{RESET}"
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
            if self.config.TITLE is None:
                title = f"{category}_{ingestor_id}"
            else:
                title = self.config.TITLE  # Fallback to config title if no ingestor_id

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

            response = self._authed_request(
                "POST",
                f"{self.config.API_ENDPOINT}/dataset/",
                data=payload,
                extra_headers={"Content-Type": "application/json"},
                timeout=API_TIMEOUT,
            )

            # Check status after retries are exhausted
            if response.status_code >= 400:
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}: {response.text}"
                )
            dataset = self._parse_json(response, required=True)
            logger.info(
                f"{GREEN}Successfully created dataset. Response: {dataset}{RESET}"
            )
            return dataset

        except requests.exceptions.RequestException as e:
            logger.error(f"{RED}Error creating dataset: {str(e)[:100]}{RESET}")
            if hasattr(e.response, "text"):
                logger.error(f"{RED}Error response: {e.response.text}{RESET}")
            raise

    def __del__(self):
        """Cleanup when the client is destroyed"""
        if hasattr(self, "session"):
            self.session.close()
