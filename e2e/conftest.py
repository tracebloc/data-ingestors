"""Pytest config for the end-to-end ingestion suite.

These tests run the REAL ``tracebloc-ingest`` engine against the bundled
``templates/`` datasets, into a REAL MySQL, with an in-process mock backend
(``CLIENT_ENV=local`` pins the APIClient at ``http://localhost:8000``). They
are skipped unless a MySQL is reachable (``MYSQL_HOST`` / ``MYSQL_PORT``), so
the default unit ``pytest`` run is unaffected.

Local run::

    docker compose -f e2e/docker-compose.yml up -d
    MYSQL_HOST=127.0.0.1 DB_USER=root DB_PASSWORD=root pytest e2e/ -v

CI: ``.github/workflows/e2e.yml`` (MySQL service).
"""
import json
import os
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest


def _mysql_reachable() -> bool:
    host = os.environ.get("MYSQL_HOST", "127.0.0.1")
    port = int(os.environ.get("MYSQL_PORT", "3306"))
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


# Don't even collect the e2e tests unless a MySQL is reachable — keeps the
# default `pytest` (unit) run green when run without a database.
collect_ignore_glob = [] if _mysql_reachable() else ["test_*.py"]


class _MockBackend(BaseHTTPRequestHandler):
    """200 + permissive JSON for every endpoint the APIClient calls (auth token,
    batch send, metadata, prepare/create dataset)."""

    def _ok(self):
        body = json.dumps({
            "token": "mock", "key": "mock", "id": 1, "status": "ok",
            "success": True, "data_id": "mock", "dataset_key": "mock",
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = do_PUT = do_PATCH = lambda self: self._ok()

    def log_message(self, *_a):  # silence the default access log
        pass


@pytest.fixture(scope="session", autouse=True)
def mock_backend():
    srv = HTTPServer(("127.0.0.1", 8000), _MockBackend)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield
    srv.shutdown()


@pytest.fixture(autouse=True)
def engine_env(tmp_path, monkeypatch):
    # /data/shared (the hardcoded sidecar destination) isn't writable in CI;
    # redirect it to a per-test tmp dir. STORAGE_PATH is a class constant.
    from tracebloc_ingestor.config import Config
    shared = tmp_path / "shared"
    shared.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(Config, "STORAGE_PATH", str(shared))

    monkeypatch.setenv("CLIENT_ENV", "local")  # bypass backend auth; point at the mock
    monkeypatch.setenv("MYSQL_HOST", os.environ.get("MYSQL_HOST", "127.0.0.1"))
    monkeypatch.setenv("MYSQL_PORT", os.environ.get("MYSQL_PORT", "3306"))
    monkeypatch.setenv("DB_USER", os.environ.get("DB_USER", "root"))
    monkeypatch.setenv("DB_PASSWORD", os.environ.get("DB_PASSWORD", "root"))
    monkeypatch.setenv("DB_NAME", os.environ.get("DB_NAME", "training_test_datasets"))
