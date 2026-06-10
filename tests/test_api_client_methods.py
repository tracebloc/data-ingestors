"""Tests for APIClient request methods (send_batch, metadata, prepare, create).

Auth boot paths are covered in test_api_client_auth.py. Here we build a client
with BACKEND_TOKEN set (so __init__ does no network call) and patch the
session's get/post to exercise each method's success / error / local-mode path.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
import requests

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.api.client import APIClient
from tracebloc_ingestor.utils.constants import TaskCategory


def _client(**overrides):
    # TITLE=None by default so create_dataset's title-generation path is
    # deterministic regardless of any TITLE exported in the host/CI env.
    defaults = dict(BACKEND_TOKEN="tok", CLIENT_USERNAME=None,
                    CLIENT_PASSWORD=None, EDGE_ENV="prod", TITLE=None)
    defaults.update(overrides)
    return APIClient(Config(**defaults))


def _resp(status=200, json_body=None, text=""):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_body if json_body is not None else {}
    r.text = text
    return r


# ---------------------------------------------------------------------------
# authenticate() error path
# ---------------------------------------------------------------------------

def test_authenticate_http_error_raises():
    cfg = Config(BACKEND_TOKEN=None, CLIENT_USERNAME="u",
                 CLIENT_PASSWORD="p", EDGE_ENV="prod")
    with patch("requests.Session.post", return_value=_resp(403, text="forbidden")):
        with pytest.raises(ValueError) as exc:
            APIClient(cfg)
    # The manually-raised HTTPError carries no .response, so the client
    # falls through to the generic "Error response" branch.
    assert "403" in str(exc.value)


# ---------------------------------------------------------------------------
# send_batch
# ---------------------------------------------------------------------------

def test_send_batch_success():
    client = _client()
    records = [(1, {"data_id": "a", "label": "cat"}), (2, {"data_id": "b"})]
    with patch.object(client.session, "post", return_value=_resp(200)) as post:
        assert client.send_batch(records, "tbl", ingestor_id="ing") is True
    post.assert_called_once()
    assert "/global_meta/tbl/" in post.call_args[0][0]


def test_send_batch_http_error_returns_false():
    client = _client()
    with patch.object(client.session, "post", return_value=_resp(500, text="boom")):
        assert client.send_batch([(1, {"data_id": "a"})], "tbl", "ing") is False


def test_send_batch_local_mode_skips_network():
    client = _client(EDGE_ENV="local")
    with patch.object(client.session, "post") as post:
        assert client.send_batch([(1, {})], "tbl", "ing") is True
    post.assert_not_called()


# ---------------------------------------------------------------------------
# send_global_meta_meta
# ---------------------------------------------------------------------------

def test_send_global_meta_success():
    client = _client()
    with patch.object(client.session, "post", return_value=_resp(200, {"ok": 1})):
        assert client.send_global_meta_meta("tbl", {"a": "INT"}, {"k": "v"}) is True


def test_send_global_meta_error_returns_false():
    client = _client()
    with patch.object(client.session, "post", return_value=_resp(400, text="bad")):
        assert client.send_global_meta_meta("tbl", {}, {}) is False


def test_send_global_meta_local_mode():
    client = _client(EDGE_ENV="local")
    with patch.object(client.session, "post") as post:
        assert client.send_global_meta_meta("tbl", {}, {}) is True
    post.assert_not_called()


# ---------------------------------------------------------------------------
# send_generate_edge_label_meta
# ---------------------------------------------------------------------------

def test_generate_edge_label_success():
    client = _client()
    with patch.object(client.session, "get", return_value=_resp(200)) as get:
        assert client.send_generate_edge_label_meta("tbl", "ing", "train") is True
    assert "generate-edge-labels-meta" in get.call_args[0][0]


def test_generate_edge_label_error_returns_false():
    client = _client()
    with patch.object(client.session, "get", return_value=_resp(503, text="down")):
        assert client.send_generate_edge_label_meta("tbl", "ing", "train") is False


def test_generate_edge_label_local_mode():
    client = _client(EDGE_ENV="local")
    with patch.object(client.session, "get") as get:
        assert client.send_generate_edge_label_meta("tbl", "ing", "train") is True
    get.assert_not_called()


# ---------------------------------------------------------------------------
# prepare_dataset
# ---------------------------------------------------------------------------

def test_prepare_dataset_success():
    client = _client()
    with patch.object(client.session, "get", return_value=_resp(200, {"ok": 1})):
        assert client.prepare_dataset(
            TaskCategory.IMAGE_CLASSIFICATION, "ing", "image", "train"
        ) is True


def test_prepare_dataset_invalid_category_returns_false():
    client = _client()
    with patch.object(client.session, "get") as get:
        assert client.prepare_dataset("nonsense", "ing", "image", "train") is False
    get.assert_not_called()


def test_prepare_dataset_error_returns_false():
    client = _client()
    with patch.object(client.session, "get", return_value=_resp(500, text="x")):
        assert client.prepare_dataset(
            TaskCategory.IMAGE_CLASSIFICATION, "ing", "image", "train"
        ) is False


def test_prepare_dataset_local_mode():
    client = _client(EDGE_ENV="local")
    with patch.object(client.session, "get") as get:
        assert client.prepare_dataset("anything", "ing", "image", "train") is True
    get.assert_not_called()


# ---------------------------------------------------------------------------
# create_dataset
# ---------------------------------------------------------------------------

def test_create_dataset_success_generates_title():
    client = _client()
    captured = {}

    def fake_post(url, data=None, headers=None, timeout=None):
        captured["data"] = data
        return _resp(200, {"id": 7})

    with patch.object(client.session, "post", side_effect=fake_post):
        result = client.create_dataset(
            ingestor_id="ing", category=TaskCategory.IMAGE_CLASSIFICATION
        )
    assert result == {"id": 7}
    assert "image_classification_ing" in captured["data"]


def test_create_dataset_tabular_allows_feature_modification():
    client = _client()
    captured = {}
    with patch.object(client.session, "post",
                      side_effect=lambda url, data=None, **k: captured.update(data=data) or _resp(200, {"id": 1})):
        client.create_dataset(ingestor_id="ing", category=TaskCategory.TABULAR_CLASSIFICATION)
    assert '"allow_feature_modification": true' in captured["data"]


def test_create_dataset_uses_config_title():
    client = _client(TITLE="My Title")
    captured = {}
    with patch.object(client.session, "post",
                      side_effect=lambda url, data=None, **k: captured.update(data=data) or _resp(200, {"id": 1})):
        client.create_dataset(ingestor_id="ing", category=TaskCategory.IMAGE_CLASSIFICATION)
    assert "My Title" in captured["data"]


def test_create_dataset_error_raises():
    client = _client()
    with patch.object(client.session, "post", return_value=_resp(500, text="err")):
        with pytest.raises(requests.exceptions.RequestException):
            client.create_dataset(ingestor_id="ing", category=TaskCategory.IMAGE_CLASSIFICATION)


def test_create_dataset_local_mode():
    client = _client(EDGE_ENV="local")
    with patch.object(client.session, "post") as post:
        result = client.create_dataset(ingestor_id="ing", category="x")
    assert result["id"] == "mock_dataset_id"
    post.assert_not_called()


# ---------------------------------------------------------------------------
# 401 auto-refresh (backend/#772 P2)
# ---------------------------------------------------------------------------

def test_authed_request_refreshes_token_on_401():
    """A 401 on an authenticated call triggers ONE refresh + retry. The
    refresh path rotates the token to a fresh value; the second call
    succeeds with the new token. The terminal create_dataset / metadata
    callback used to fail outright on multi-hour runs when the token
    aged out — now it transparently re-mints."""
    client = _client(BACKEND_TOKEN="old_token")
    assert client.token == "old_token"
    calls = []

    def fake_post(url, headers=None, **kwargs):
        calls.append(headers["Authorization"])
        if len(calls) == 1:
            return _resp(401, text='{"detail":"Invalid token."}')
        return _resp(200, {"id": 7})

    # Stub _refresh_token to simulate a successful rotation.
    def fake_refresh():
        client.token = "new_token"
        return True

    with patch.object(client.session, "post", side_effect=fake_post), \
         patch.object(client, "_refresh_token", side_effect=fake_refresh):
        client.create_dataset(ingestor_id="ing", category=TaskCategory.IMAGE_CLASSIFICATION)

    assert len(calls) == 2, "expected one 401 + one retry"
    assert calls[0] == "TOKEN old_token"
    assert calls[1] == "TOKEN new_token"


def test_authed_request_gives_up_after_one_retry(monkeypatch):
    """If refresh doesn't change anything (no rotation, or re-auth itself
    fails), the second attempt is NOT made — the original 401 is surfaced
    so the caller's existing error path runs unchanged."""
    monkeypatch.setenv("BACKEND_TOKEN", "stuck_token")
    client = _client()
    # No env update -> refresh returns False.
    calls = []

    def fake_post(url, headers=None, **kwargs):
        calls.append(1)
        return _resp(401, text='{"detail":"Invalid token."}')

    with patch.object(client.session, "post", side_effect=fake_post):
        with pytest.raises(requests.exceptions.RequestException):
            client.create_dataset(
                ingestor_id="ing", category=TaskCategory.IMAGE_CLASSIFICATION
            )

    # One attempt only — refresh saw no change so no retry.
    assert len(calls) == 1


def test_authed_request_passes_through_non_401_unchanged():
    """Non-401 statuses (200, 4xx other than 401, 5xx) take the no-retry
    path. Refresh logic must NOT engage on success or on a non-auth
    failure — the per-call error handling already covers those."""
    client = _client()
    with patch.object(client.session, "post", return_value=_resp(200, {"id": 1})) as post:
        client.create_dataset(ingestor_id="ing", category=TaskCategory.IMAGE_CLASSIFICATION)
    # Exactly one call — no refresh, no retry on the happy path.
    assert post.call_count == 1


def test_refresh_token_noop_in_local_mode():
    """Local mode uses a mock token and no auth network calls — refresh
    is a no-op so test runs / dev loops don't hit the env-read path."""
    client = _client(EDGE_ENV="local")
    assert client._refresh_token() is False
    assert client.token == "mock_token"
