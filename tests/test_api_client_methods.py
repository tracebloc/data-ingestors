"""Tests for APIClient request methods (send_ingest_summary).

Auth boot paths are covered in test_api_client_auth.py. Here we build a client
with BACKEND_TOKEN set (so __init__ does no network call) and patch the
session's post to exercise each method's success / error / local-mode path.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.api.client import APIClient


def _client(**overrides):
    defaults = dict(
        BACKEND_TOKEN="tok",
        CLIENT_USERNAME=None,
        CLIENT_PASSWORD=None,
        EDGE_ENV="prod",
        TITLE=None,
    )
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
    cfg = Config(
        BACKEND_TOKEN=None, CLIENT_USERNAME="u", CLIENT_PASSWORD="p", EDGE_ENV="prod"
    )
    with patch("requests.Session.post", return_value=_resp(403, text="forbidden")):
        with pytest.raises(ValueError) as exc:
            APIClient(cfg)
    # The manually-raised HTTPError carries no .response, so the client
    # falls through to the generic "Error response" branch.
    assert "403" in str(exc.value)


# ---------------------------------------------------------------------------
# send_ingest_summary
# ---------------------------------------------------------------------------


def _summary_call(client):
    return client.send_ingest_summary(
        table_name="tbl", ingestor_id="ing", labels={"cat": 1},
        dataset_title="T", data_format="image", data_intent="train",
        category="image_classification", schema={}, samples=[],
    )


def test_send_ingest_summary_success():
    client = _client()
    with patch.object(
        client.session, "post",
        return_value=_resp(200, {"dataset_id": 1, "dataset_key": "key"}),
    ):
        result = _summary_call(client)
    assert result == {"dataset_id": 1, "dataset_key": "key"}


def test_send_ingest_summary_http_error_raises():
    client = _client()
    with patch.object(client.session, "post", return_value=_resp(500, text="boom")):
        with pytest.raises(requests.exceptions.RequestException):
            _summary_call(client)


def test_send_ingest_summary_409_returns_existing_dataset():
    """Backend #900 returns 409 + {dataset_id, dataset_key} when the
    (owner, ingestor_id) dataset already exists (e.g. a transport-level POST
    retry after a dropped 201). That 409 is a success signal for the retry —
    the client must return the existing dataset, not raise (issue #332)."""
    client = _client()
    with patch.object(
        client.session, "post",
        return_value=_resp(409, {"dataset_id": 7, "dataset_key": "existing"}),
    ):
        result = _summary_call(client)
    assert result == {"dataset_id": 7, "dataset_key": "existing"}


def test_send_ingest_summary_local_mode():
    client = _client(EDGE_ENV="local")
    with patch.object(client.session, "post") as post:
        result = _summary_call(client)
    post.assert_not_called()
    assert result["dataset_id"] == "mock_dataset_id"


def test_send_ingest_summary_payload_shape():
    import json as _json

    client = _client()
    with patch.object(
        client.session, "post",
        return_value=_resp(200, {"dataset_id": 42, "dataset_key": "k"}),
    ) as post:
        client.send_ingest_summary(
            table_name="tbl",
            ingestor_id="ing-42",
            labels={"cat": 5, "dog": 3},
            dataset_title="My Dataset",
            data_format="image",
            data_intent="train",
            category="image_classification",
            schema={"col": "INT"},
            samples=[{"data_id": "a"}],
            meta_data={"source": "test"},
        )

    sent = _json.loads(post.call_args.kwargs["data"])
    for key in ("ingestor_id", "labels", "dataset_title", "data_format",
                "data_intent", "category", "schema", "samples", "meta_data"):
        assert key in sent, f"missing key: {key}"
    assert sent["meta_data"] == {"source": "test"}


# ---------------------------------------------------------------------------
# 401 auto-refresh (backend/#772 P2)
# ---------------------------------------------------------------------------


def test_authed_request_refreshes_token_on_401():
    """A 401 on an authenticated call triggers ONE refresh + retry. The
    refresh path rotates the token to a fresh value; the second call
    succeeds with the new token. The terminal send_ingest_summary
    callback used to fail outright on multi-hour runs when the token
    aged out — now it transparently re-mints."""
    client = _client(BACKEND_TOKEN="old_token")
    assert client.token == "old_token"
    calls = []

    def fake_post(url, headers=None, **kwargs):
        calls.append(headers["Authorization"])
        if len(calls) == 1:
            return _resp(401, text='{"detail":"Invalid token."}')
        return _resp(200, {"dataset_id": 7, "dataset_key": "k"})

    # Stub _refresh_token to simulate a successful rotation.
    def fake_refresh():
        client.token = "new_token"
        return True

    with patch.object(client.session, "post", side_effect=fake_post), patch.object(
        client, "_refresh_token", side_effect=fake_refresh
    ):
        _summary_call(client)

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
            _summary_call(client)

    # One attempt only — refresh saw no change so no retry.
    assert len(calls) == 1


def test_authed_request_passes_through_non_401_unchanged():
    """Non-401 statuses (200, 4xx other than 401, 5xx) take the no-retry
    path. Refresh logic must NOT engage on success or on a non-auth
    failure — the per-call error handling already covers those."""
    client = _client()
    with patch.object(
        client.session, "post",
        return_value=_resp(200, {"dataset_id": 1, "dataset_key": "k"}),
    ) as post:
        _summary_call(client)
    # Exactly one call — no refresh, no retry on the happy path.
    assert post.call_count == 1


def test_refresh_token_noop_in_local_mode():
    """Local mode uses a mock token and no auth network calls — refresh
    is a no-op so test runs / dev loops don't hit the env-read path."""
    client = _client(EDGE_ENV="local")
    assert client._refresh_token() is False
    assert client.token == "mock_token"


# ---------------------------------------------------------------------------
# _refresh_token body (#772 mid-run token rotation). The 401-retry WRAPPER is
# tested above with _refresh_token STUBBED, so the rotation body itself was
# never executed (audit gap H1 — it's the code preventing the "rows committed,
# never registered" incident on long runs). These exercise the real body.
# ---------------------------------------------------------------------------


def test_refresh_token_picks_up_rotated_backend_token(monkeypatch):
    """When BACKEND_TOKEN is rotated in env mid-run, the next _refresh_token()
    re-reads it from Config (which reads env on each access) and updates the
    in-memory token, returning True."""
    monkeypatch.setenv("CLIENT_ENV", "prod")  # EDGE_ENV != local
    monkeypatch.setenv("BACKEND_TOKEN", "old_token")
    client = APIClient(Config())  # token from env; no network (BACKEND_TOKEN branch)
    assert client.token == "old_token"

    monkeypatch.setenv("BACKEND_TOKEN", "new_token")  # rotation
    assert client._refresh_token() is True
    assert client.token == "new_token"


def test_refresh_token_false_when_backend_token_unchanged(monkeypatch):
    """No rotation -> refresh is a no-op returning False, so the caller treats
    the next 401 as terminal instead of retrying forever."""
    monkeypatch.setenv("CLIENT_ENV", "prod")
    monkeypatch.setenv("BACKEND_TOKEN", "same_token")
    client = APIClient(Config())
    assert client._refresh_token() is False
    assert client.token == "same_token"


def test_refresh_token_reauthenticates_on_creds_path():
    """CLIENT_ID/PASSWORD path: _refresh_token re-mints via authenticate() and
    returns True iff the token changed."""
    with patch.object(APIClient, "authenticate", return_value="init_token"):
        client = _client(BACKEND_TOKEN=None, CLIENT_USERNAME="u", CLIENT_PASSWORD="p")
    client.token = "old_token"
    with patch.object(client, "authenticate", return_value="fresh_token"):
        assert client._refresh_token() is True
    assert client.token == "fresh_token"


def test_refresh_token_false_when_reauth_raises():
    """If re-auth itself raises, _refresh_token swallows it and returns False
    (terminal) — the original 401 then surfaces through the caller unchanged."""
    with patch.object(APIClient, "authenticate", return_value="init_token"):
        client = _client(BACKEND_TOKEN=None, CLIENT_USERNAME="u", CLIENT_PASSWORD="p")
    client.token = "old_token"
    with patch.object(client, "authenticate", side_effect=RuntimeError("auth down")):
        assert client._refresh_token() is False
