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
        table_name="tbl",
        ingestor_id="ing",
        labels={"cat": 1},
        dataset_title="T",
        data_format="image",
        data_intent="train",
        category="image_classification",
        schema={},
        samples=[],
    )


def test_send_ingest_summary_success():
    client = _client()
    with patch.object(
        client.session,
        "post",
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
        client.session,
        "post",
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
        client.session,
        "post",
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
    for key in (
        "ingestor_id",
        "labels",
        "dataset_title",
        "data_format",
        "data_intent",
        "category",
        "schema",
        "samples",
        "meta_data",
    ):
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
        client.session,
        "post",
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


# ---------------------------------------------------------------------------
# get_dataset_metadata() — read-by-ingestor_id (backend#1198)
# ---------------------------------------------------------------------------


def test_get_dataset_metadata_success():
    client = _client()
    body = {"table_name": "tb", "category": "tabular_classification", "meta_data": {}}
    with patch.object(client.session, "get", return_value=_resp(200, body)):
        result = client.get_dataset_metadata("ing-1")
    assert result == body


def test_get_dataset_metadata_404_returns_none():
    """A 404 (no dataset for this ingestor_id owned by this edge) is a skip
    signal for the sweep, not an error — return None rather than raise."""
    client = _client()
    with patch.object(client.session, "get", return_value=_resp(404, text="nope")):
        assert client.get_dataset_metadata("missing") is None


def test_get_dataset_metadata_http_error_raises():
    client = _client()
    with patch.object(client.session, "get", return_value=_resp(500, text="boom")):
        with pytest.raises(requests.exceptions.RequestException):
            client.get_dataset_metadata("ing-1")


def test_get_dataset_metadata_local_mode():
    client = _client(EDGE_ENV="local")
    with patch.object(client.session, "get") as get:
        assert client.get_dataset_metadata("ing-1") is None
    get.assert_not_called()


# ---------------------------------------------------------------------------
# send_metadata_backfill() — upsert recomputed metadata (backend#1166/#1198)
# ---------------------------------------------------------------------------


def test_send_metadata_backfill_success():
    client = _client()
    body = {"table_name": "tb", "created": True, "competitions_refolded": 2}
    with patch.object(client.session, "post", return_value=_resp(201, body)):
        result = client.send_metadata_backfill("tb", {"x": {"dtype": "int"}}, {})
    assert result == body


def test_send_metadata_backfill_http_error_raises():
    client = _client()
    with patch.object(client.session, "post", return_value=_resp(404, text="no table")):
        with pytest.raises(requests.exceptions.RequestException):
            client.send_metadata_backfill("tb", {"x": {"dtype": "int"}}, {})


def test_send_metadata_backfill_local_mode():
    client = _client(EDGE_ENV="local")
    with patch.object(client.session, "post") as post:
        result = client.send_metadata_backfill("tb", {"x": {"dtype": "int"}}, {})
    post.assert_not_called()
    assert result["table_name"] == "tb"


def test_send_metadata_backfill_payload_shape():
    import json as _json

    client = _client()
    captured = {}

    def _capture(url, **kwargs):
        captured["url"] = url
        captured["data"] = kwargs.get("data")
        return _resp(
            201, {"table_name": "tb", "created": False, "competitions_refolded": 0}
        )

    with patch.object(client.session, "post", side_effect=_capture):
        client.send_metadata_backfill("tb", {"x": {"dtype": "int"}}, {"attributes": {}})

    assert captured["url"].endswith("/global_meta/metadata_backfill/tb/")
    sent = _json.loads(captured["data"])
    assert sent == {"schema": {"x": {"dtype": "int"}}, "meta_data": {"attributes": {}}}


# ---------------------------------------------------------------------------
# send_ingest_summary: the label-policy boundary (#486)
# ---------------------------------------------------------------------------
#
# The policy is applied HERE and nowhere earlier — the row stored in the
# cluster's MySQL keeps the raw target, which is what the training client reads.
# These pin that both label-bearing payload fields go through it.


def _captured_summary_payload(client, **overrides):
    """POST the summary with a mocked session and return the decoded body."""
    import json as _json

    kwargs = dict(
        table_name="tbl",
        ingestor_id="ing",
        labels={"1": 19, "0": 11},
        dataset_title="T",
        data_format="tabular",
        data_intent="train",
        category="time_to_event_prediction",
        schema={},
        samples=[{"data_id": "d1", "label": "1"}, {"data_id": "d2", "label": "0"}],
    )
    kwargs.update(overrides)
    captured = {}

    def _capture(url, **request_kwargs):
        captured["data"] = request_kwargs.get("data")
        return _resp(200, {"dataset_id": 1, "dataset_key": "key"})

    with patch.object(client.session, "post", side_effect=_capture):
        client.send_ingest_summary(**kwargs)
    return _json.loads(captured["data"])


def test_summary_default_policy_sends_raw_labels():
    """Default is passthrough — classification payloads are unchanged."""
    sent = _captured_summary_payload(_client())
    assert sent["labels"] == {"1": 19, "0": 11}
    assert [s["label"] for s in sent["samples"]] == ["1", "0"]


def test_summary_bucket_policy_buckets_labels_and_samples():
    from tracebloc_ingestor.utils.label_policy import BUCKET, apply

    sent = _captured_summary_payload(_client(), label_policy=BUCKET)
    # JSON object keys are strings, so compare against stringified bucket ids.
    assert sent["labels"] == {str(apply("1", BUCKET)): 19, str(apply("0", BUCKET)): 11}
    # Strings, not JSON numbers: data_samples[].label has always been a string
    # (pre-#486 the bucket was read back out of the VARCHAR label column).
    assert [s["label"] for s in sent["samples"]] == [
        str(apply("1", BUCKET)),
        str(apply("0", BUCKET)),
    ]
    assert all(isinstance(s["label"], str) for s in sent["samples"])
    # ...and no raw target value survives anywhere in the body.
    assert sent["labels"].keys() != {"1", "0"}


def test_summary_bucket_policy_preserves_total_row_count():
    from tracebloc_ingestor.utils.label_policy import BUCKET

    sent = _captured_summary_payload(
        _client(), labels={str(i): 1 for i in range(200)}, label_policy=BUCKET
    )
    assert sum(sent["labels"].values()) == 200


def test_summary_does_not_mutate_the_callers_labels_or_samples():
    """base.py reuses these for its own logging/stats after the call."""
    from tracebloc_ingestor.utils.label_policy import BUCKET

    labels = {"1": 19, "0": 11}
    samples = [{"data_id": "d1", "label": "1"}]
    _captured_summary_payload(
        _client(), labels=labels, samples=samples, label_policy=BUCKET
    )
    assert labels == {"1": 19, "0": 11}
    assert samples == [{"data_id": "d1", "label": "1"}]


def test_summary_unknown_policy_raises_before_sending():
    client = _client()
    with patch.object(client.session, "post") as post:
        with pytest.raises(ValueError, match="Unknown label policy"):
            client.send_ingest_summary(
                table_name="tbl",
                ingestor_id="ing",
                labels={"1": 1},
                dataset_title="T",
                data_format="tabular",
                data_intent="train",
                category="tabular_regression",
                schema={},
                samples=[],
                label_policy="ohno",
            )
    post.assert_not_called()


def test_summary_local_mode_still_applies_policy_to_its_log(caplog):
    """The mock branch reports what WOULD ship: bucket collisions merge keys,
    so the label count it logs is the post-policy one."""
    import logging as _logging

    from tracebloc_ingestor.utils.label_policy import BUCKET, apply

    client = _client(EDGE_ENV="local")
    labels = {str(i): 1 for i in range(200)}
    expected_labels = len({apply(k, BUCKET) for k in labels})
    with caplog.at_level(_logging.INFO, logger="tracebloc_ingestor.api.client"):
        with patch.object(client.session, "post") as post:
            client.send_ingest_summary(
                table_name="tbl",
                ingestor_id="ing",
                labels=labels,
                dataset_title="T",
                data_format="tabular",
                data_intent="train",
                category="tabular_regression",
                schema={},
                samples=[],
                label_policy=BUCKET,
            )
    post.assert_not_called()
    assert f"200 rows across {expected_labels} label(s)" in caplog.text
