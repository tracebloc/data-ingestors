from typing import List, Dict, Any, Optional
import os
import requests, json
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from ..config import Config
from ..utils.constants import (
    API_TIMEOUT,
    RESET,
    BOLD,
    GREEN,
    RED,
    YELLOW,
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

    def send_ingest_summary(
        self,
        table_name: str,
        ingestor_id: str,
        labels: Dict[str, int],
        dataset_title: str,
        data_format: str,
        data_intent: str,
        category: str,
        schema: Dict[str, Any],
        samples: List[Dict[str, Any]],
        meta_data: Optional[Dict[str, Any]] = None,
        physical_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send a single ingest summary to the backend, creating the UserDataSet in one
        call. Replaces the legacy per-row POST → generate_edge_labels_meta →
        send_global_meta_meta → prepare_dataset → create_dataset flow.

        Args:
            table_name: Dataset table name (used as the URL path segment)
            ingestor_id: UUID identifying this ingest run
            labels: ``{label: row_count}`` — computed locally after DB insert
            dataset_title: Human-readable name for the new dataset
            data_format: One of "image", "tabular", "text", "audio", "video"
            data_intent: "train" or "test"
            category: TaskCategory value, e.g. "image_classification"
            schema: Column schema written to GlobalMetaData
            samples: Small list of representative records shown in the UI
            physical_table: RFC-0003 D16 (tracebloc/backend#1205) — the
                per-ingestion physical table (``ds_<uuid4().hex>``, derived
                from ingestor_id) this run wrote into, reported so the
                backend persists the handle
                (tracebloc/backend#1206). ``None`` (legacy shared-table
                ingests) omits the field entirely, keeping the payload
                byte-identical to today's.

        Returns:
            ``{"dataset_id": ..., "dataset_key": ...}``

        Raises:
            requests.exceptions.RequestException: If the API call fails after retries
        """
        if self.config.EDGE_ENV == "local":
            logger.info(
                f"Mock: Would send ingest summary for {table_name} "
                f"({sum(labels.values())} rows across {len(labels)} label(s))"
            )
            return {"dataset_id": "mock_dataset_id", "dataset_key": "mock_dataset_key"}

        try:
            payload_fields = {
                "ingestor_id": ingestor_id,
                "labels": labels,
                "dataset_title": dataset_title,
                "data_format": data_format,
                "data_intent": data_intent,
                "category": category,
                "schema": schema,
                "samples": samples,
                "meta_data": meta_data or {},
            }
            if physical_table:
                payload_fields["physical_table"] = physical_table
            payload = json.dumps(payload_fields)
            logger.info(
                f"Sending ingest summary for {table_name}: "
                f"{len(labels)} label(s), {sum(labels.values())} total rows"
            )
            response = self._authed_request(
                "POST",
                f"{self.config.API_ENDPOINT}/global_meta/summary/{table_name}/",
                extra_headers={"Content-Type": "application/json"},
                data=payload,
                timeout=API_TIMEOUT,
            )

            # 409 = backend idempotency guard (backend #900): the
            # (owner, ingestor_id) dataset already exists — e.g. urllib3
            # re-sent this POST after a dropped/timed-out 201 on a long
            # ingest (POST is in the retry adapter's allowed_methods). The
            # body carries the existing {dataset_id, dataset_key}. This is a
            # success signal for the retry, not a failure — parse and return
            # it rather than crashing an ingest whose dataset was created.
            if response.status_code == 409:
                result = self._parse_json(response, required=True)
                logger.info(
                    f"{GREEN}Dataset already registered (idempotent retry): "
                    f"key={result.get('dataset_key')} "
                    f"id={result.get('dataset_id')}{RESET}"
                )
                return result

            # Check status after retries are exhausted. Attach the response
            # so the handler below can log the status and body — a bare
            # HTTPError has e.response = None, which would hide the field
            # error that explains why the call was rejected.
            if response.status_code >= 400:
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}: {response.text}",
                    response=response,
                )
            result = self._parse_json(response, required=True)
            logger.info(
                f"{GREEN}Dataset created: key={result.get('dataset_key')} "
                f"id={result.get('dataset_id')}{RESET}"
            )
            return result

        except requests.exceptions.RequestException as e:
            if e.response is not None:
                body = (e.response.text or "")[:2000]
                logger.error(
                    f"{RED}Error sending ingest summary: "
                    f"HTTP {e.response.status_code}: {body}{RESET}"
                )
            else:
                logger.error(f"{RED}Error sending ingest summary: {str(e)[:500]}{RESET}")
            raise

    def get_dataset_metadata(self, ingestor_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a dataset's backend record by ``ingestor_id`` (backend#1198).

        Returns the core fields — ``table_name`` / ``category`` / ``data_format``
        / ``intent`` / ``is_competition`` / ``source_dataset_ids`` — plus the
        currently-stored ``schema`` / ``meta_data``. Backs the pre-cutover
        backfill runner: ``category`` and ``data_format`` drive the recompute and
        are not recoverable from the table alone, so the runner reads them here
        first.

        Returns ``None`` when the backend has no dataset for this ``ingestor_id``
        owned by this edge (404) so a sweep can skip it rather than crash.

        Raises:
            requests.exceptions.HTTPError: on any non-404 error status.
        """
        if self.config.EDGE_ENV == "local":
            logger.info(
                f"Mock: would fetch dataset metadata for ingestor_id={ingestor_id}"
            )
            return None

        url = (
            f"{self.config.API_ENDPOINT}"
            f"/global_meta/metadata_backfill/by-ingestor/{ingestor_id}/"
        )
        response = self._authed_request("GET", url, timeout=API_TIMEOUT)
        if response.status_code == 404:
            logger.warning(
                f"{YELLOW}No backend dataset for ingestor_id={ingestor_id} "
                f"owned by this edge (404); skipping.{RESET}"
            )
            return None
        if response.status_code >= 400:
            # Status only in the message — NOT response.text. A backend error
            # body can echo the dataset's metadata (categorical vocab = customer
            # cell values), and this exception surfaces in the backfill runner's
            # install-log output. The response object is attached for a caller
            # that needs to inspect it deliberately. (bugbot)
            raise requests.exceptions.HTTPError(
                f"HTTP {response.status_code}", response=response
            )
        return self._parse_json(response, required=True)

    def send_metadata_backfill(
        self,
        table_name: str,
        schema: Dict[str, Any],
        meta_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Upsert recomputed ``{schema, meta_data}`` for a PRE-EXISTING table via
        the metadata-backfill endpoint (backend#1166/#1198).

        Metadata-only: never creates a dataset (the table must already exist
        backend-side — 404 otherwise). The POST also propagates the refresh into
        any competition built from the table (re-fold). Idempotent, so a re-run
        is a safe overwrite.

        Returns:
            The backend body — ``{"table_name", "created", "competitions_refolded"}``.

        Raises:
            requests.exceptions.HTTPError: If the API call fails after retries.
        """
        if self.config.EDGE_ENV == "local":
            logger.info(
                f"Mock: would backfill metadata for {table_name} "
                f"({len(schema)} schema column(s))"
            )
            return {
                "table_name": table_name,
                "created": False,
                "competitions_refolded": 0,
            }

        payload = json.dumps({"schema": schema, "meta_data": meta_data or {}})
        response = self._authed_request(
            "POST",
            f"{self.config.API_ENDPOINT}/global_meta/metadata_backfill/{table_name}/",
            extra_headers={"Content-Type": "application/json"},
            data=payload,
            timeout=API_TIMEOUT,
        )
        if response.status_code >= 400:
            # Status only — NOT response.text (see get_dataset_metadata): the
            # POST body carries this table's categorical vocab, so a backend
            # error echoing it back would embed customer cell values in the
            # runner's install-log output. (bugbot)
            raise requests.exceptions.HTTPError(
                f"HTTP {response.status_code}", response=response
            )
        result = self._parse_json(response, required=True)
        logger.info(
            f"{GREEN}Backfilled metadata for {table_name}: "
            f"created={result.get('created')}, "
            f"competitions_refolded={result.get('competitions_refolded')}{RESET}"
        )
        return result

    def __del__(self):
        """Cleanup when the client is destroyed"""
        if hasattr(self, "session"):
            self.session.close()
