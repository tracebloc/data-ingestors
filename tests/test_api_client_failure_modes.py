"""APIClient failure-mode tests against a REAL local HTTP server.

The happy-path tests (``test_api_client_methods.py``) patch ``session.post`` to
return a ``MagicMock``, so the real adapter retry, request timeout, and JSON
decoding never execute despite 100% line coverage. These drive the real
``requests`` session against a programmable in-process server so those paths
actually run — including the urllib3 ``Retry`` on the ``HTTPAdapter``, which a
``responses``/``MagicMock`` mock bypasses entirely.
"""

from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from tracebloc_ingestor.api import client as client_mod
from tracebloc_ingestor.api.client import APIClient
from tracebloc_ingestor.config import Config
from tracebloc_ingestor.utils.constants import TaskCategory


class _Program:
    """Controls the mock server. ``queue`` is consumed one entry per request;
    once empty, ``default`` is used. Each entry: dict with optional
    status / body / delay / content_type."""

    def __init__(self):
        self.queue = []
        self.default = {"status": 200, "body": "{}"}
        self.requests = 0


def _handler_for(program):
    class _Handler(BaseHTTPRequestHandler):
        def _serve(self):
            program.requests += 1
            spec = program.queue.pop(0) if program.queue else program.default
            try:
                if spec.get("delay"):
                    time.sleep(spec["delay"])
                body = spec.get("body", "").encode()
                self.send_response(spec.get("status", 200))
                self.send_header(
                    "Content-Type", spec.get("content_type", "application/json")
                )
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionError, OSError):
                pass  # client gave up (e.g. the timeout test) — nothing to write

        do_GET = do_POST = _serve

        def log_message(self, *_a):
            pass

    return _Handler


@pytest.fixture
def mock_api(monkeypatch):
    program = _Program()
    server = HTTPServer(("127.0.0.1", 0), _handler_for(program))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    # API_ENDPOINT is derived from EDGE_ENV via this map; point "prod" at us.
    monkeypatch.setitem(Config.API_ENDPOINTS, "prod", base)
    try:
        yield program
    finally:
        server.shutdown()
        server.server_close()


def _client(**overrides):
    defaults = dict(
        BACKEND_TOKEN="tok", CLIENT_USERNAME=None, CLIENT_PASSWORD=None,
        EDGE_ENV="prod", TITLE=None,
    )
    defaults.update(overrides)
    client = APIClient(Config(**defaults))
    # No real backoff sleeps in tests.
    for adapter in client.session.adapters.values():
        adapter.max_retries.backoff_factor = 0
    return client


# --- retry on transient 5xx (proves the adapter retry actually fires) -------

def test_send_batch_retries_transient_5xx_then_succeeds(mock_api):
    mock_api.queue = [{"status": 503}, {"status": 503}, {"status": 200, "body": "{}"}]
    client = _client()
    assert client.send_batch([(1, {"data_id": "a"})], "tbl", "ing") is True
    assert mock_api.requests == 3  # 2 retries + the success


def test_send_batch_persistent_5xx_returns_false(mock_api):
    mock_api.default = {"status": 503, "body": "down"}
    client = _client()
    assert client.send_batch([(1, {"data_id": "a"})], "tbl", "ing") is False
    assert mock_api.requests > 1  # it retried before giving up


# --- non-retryable status ---------------------------------------------------

def test_send_batch_401_returns_false_without_retry(mock_api):
    mock_api.default = {"status": 401, "body": "unauthorized"}
    client = _client()
    assert client.send_batch([(1, {"data_id": "a"})], "tbl", "ing") is False
    assert mock_api.requests == 1  # 401 isn't in the 5xx retry list


# --- timeout (a hanging backend must not hang the ingest) -------------------

def test_send_batch_timeout_is_handled(mock_api, monkeypatch):
    monkeypatch.setattr(client_mod, "API_TIMEOUT", 0.5)
    mock_api.default = {"status": 200, "body": "{}", "delay": 2.0}
    client = _client()
    for adapter in client.session.adapters.values():
        adapter.max_retries.total = 0  # 1 attempt (~0.5s), no retry-on-timeout
    assert client.send_batch([(1, {"data_id": "a"})], "tbl", "ing") is False


# --- non-JSON 200 body (regression for the JSONDecodeError fix) -------------

def test_send_global_meta_non_json_200_still_succeeds(mock_api):
    # A 200 whose body isn't JSON used to flip a successful send to False (the
    # .json() in the log line raised). It must now warn + still report success.
    mock_api.default = {"status": 200, "body": "OK", "content_type": "text/plain"}
    client = _client()
    assert client.send_global_meta_meta("tbl", {"a": "INT"}, {}) is True


def test_create_dataset_non_json_200_raises_clear_error(mock_api):
    mock_api.default = {"status": 200, "body": "<html>oops</html>", "content_type": "text/html"}
    client = _client()
    with pytest.raises(ValueError, match="non-JSON"):
        client.create_dataset(ingestor_id="ing", category=TaskCategory.IMAGE_CLASSIFICATION)


def test_authenticate_non_json_200_raises_clear_error(mock_api):
    mock_api.default = {"status": 200, "body": "not-json"}
    with pytest.raises(ValueError, match="non-JSON"):
        APIClient(Config(BACKEND_TOKEN=None, CLIENT_USERNAME="u",
                         CLIENT_PASSWORD="p", EDGE_ENV="prod"))
