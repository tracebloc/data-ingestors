"""Authentication boot-path tests for ``APIClient``.

Covers all three boot paths required by the #43 acceptance criteria:

1. ``BACKEND_TOKEN`` set: token used directly, ``authenticate()`` never called.
2. ``CLIENT_ID`` + ``CLIENT_PASSWORD`` set (no token): falls back to
   ``/api-token-auth/`` and emits a deprecation warning.
3. Neither set: ``Config.validate()`` fails fast with a clear error before
   any network call.

A fourth path — ``CLIENT_ENV=local`` — is also exercised because it bypasses
both validation and the network entirely.

Database credentials are intentionally **not** in the validation surface:
they're connection conventions for the bundled MySQL container, not
customer-facing secrets. See `Config.validate()` for the rationale.
"""

from __future__ import annotations

import logging
from unittest.mock import patch, MagicMock

import pytest

from tracebloc_ingestor.config import Config
from tracebloc_ingestor.api.client import APIClient


def _make_config(**overrides) -> Config:
    """Build a Config with sensible test defaults, overridden per-test.

    Default: a valid prod-like config with BACKEND_TOKEN set so ``validate()``
    passes; tests override only the auth fields they care about. DB creds are
    not part of the validation surface (they're bundled-MySQL conventions),
    so they're left to their env/property defaults here.
    """
    defaults = dict(
        BACKEND_TOKEN="test-token-abc",
        CLIENT_USERNAME=None,
        CLIENT_PASSWORD=None,
        EDGE_ENV="prod",
    )
    defaults.update(overrides)
    return Config(**defaults)


class TestBackendTokenPath:
    """BACKEND_TOKEN set → use it directly, skip /api-token-auth/."""

    def test_uses_token_directly(self):
        config = _make_config(BACKEND_TOKEN="pre-minted-xyz")

        with patch.object(APIClient, "authenticate") as mock_auth:
            client = APIClient(config)

        mock_auth.assert_not_called()
        assert client.token == "pre-minted-xyz"

    def test_does_not_read_client_password_when_token_present(self):
        # Even if the deprecated cred pair is also set, BACKEND_TOKEN wins
        # and authenticate() must not be called.
        config = _make_config(
            BACKEND_TOKEN="pre-minted-xyz",
            CLIENT_USERNAME="should-not-be-used",
            CLIENT_PASSWORD="should-not-be-used",
        )

        with patch.object(APIClient, "authenticate") as mock_auth:
            client = APIClient(config)

        mock_auth.assert_not_called()
        assert client.token == "pre-minted-xyz"


class TestCredsFallbackPath:
    """No token, but CLIENT_ID + CLIENT_PASSWORD set → /api-token-auth/."""

    def test_falls_back_to_authenticate(self):
        config = _make_config(
            BACKEND_TOKEN=None,
            CLIENT_USERNAME="alice",
            CLIENT_PASSWORD="hunter2",
        )

        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {"token": "minted-by-server"}

        with patch("requests.Session.post", return_value=fake_response) as mock_post:
            client = APIClient(config)

        # /api-token-auth/ was hit exactly once with the cred pair as the body.
        assert mock_post.call_count == 1
        called_args, called_kwargs = mock_post.call_args
        assert "/api-token-auth/" in called_args[0]
        assert called_kwargs["json"] == {
            "username": "alice",
            "password": "hunter2",
        }
        assert client.token == "minted-by-server"

    def test_emits_deprecation_warning(self, caplog):
        config = _make_config(
            BACKEND_TOKEN=None,
            CLIENT_USERNAME="alice",
            CLIENT_PASSWORD="hunter2",
        )

        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {"token": "minted"}

        with caplog.at_level(logging.WARNING, logger="tracebloc_ingestor.api.client"):
            with patch("requests.Session.post", return_value=fake_response):
                APIClient(config)

        assert any(
            "deprecated" in r.message.lower() and "BACKEND_TOKEN" in r.message
            for r in caplog.records
        ), "expected a deprecation warning naming BACKEND_TOKEN"


class TestNoCredsFailsFast:
    """No token, no creds → fail fast at validate(), before any network."""

    def test_validate_raises_value_error(self):
        # Construct a Config without any auth.
        config = _make_config(
            BACKEND_TOKEN=None,
            CLIENT_USERNAME=None,
            CLIENT_PASSWORD=None,
        )

        with pytest.raises(ValueError) as exc_info:
            config.validate()

        msg = str(exc_info.value)
        assert "BACKEND_TOKEN" in msg
        assert "CLIENT_ID" in msg
        assert "CLIENT_PASSWORD" in msg
        # The error message points users at the local-mode escape hatch.
        assert "CLIENT_ENV=local" in msg

    def test_apiclient_init_raises_before_network(self):
        config = _make_config(
            BACKEND_TOKEN=None,
            CLIENT_USERNAME=None,
            CLIENT_PASSWORD=None,
        )

        # If validate() didn't fail first, this patch would catch any
        # accidental network call and let the test fail loudly.
        with patch("requests.Session.post") as mock_post:
            with pytest.raises(ValueError):
                APIClient(config)

        mock_post.assert_not_called()


class TestLocalModeBypassesValidation:
    """CLIENT_ENV=local → no network, no validation, mock token."""

    def test_local_mode_skips_validation(self):
        # No backend auth — validate() must still pass under local mode.
        config = _make_config(
            EDGE_ENV="local",
            BACKEND_TOKEN=None,
            CLIENT_USERNAME=None,
            CLIENT_PASSWORD=None,
        )

        # Should not raise.
        config.validate()

    def test_local_mode_uses_mock_token(self):
        config = _make_config(
            EDGE_ENV="local",
            BACKEND_TOKEN=None,
            CLIENT_USERNAME=None,
            CLIENT_PASSWORD=None,
        )

        with patch.object(APIClient, "authenticate") as mock_auth, \
             patch("requests.Session.post") as mock_post:
            client = APIClient(config)

        mock_auth.assert_not_called()
        mock_post.assert_not_called()
        assert client.token == "mock_token"
