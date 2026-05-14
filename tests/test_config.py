"""Tests for ``Config.API_ENDPOINT`` resolution.

The cluster routes all backend traffic through ``requests-proxy-service:8888``
(per tracebloc/client#118-120 and tracebloc/client-runtime#33). When
``REQUESTS_PROXY_URL`` is set in the pod's env, the ingestor picks it up
directly rather than relying on the Helm chart to remap it onto a
``CLIENT_ENV`` value. These tests pin both the new precedence and the
preserved fallback behavior.
"""

from __future__ import annotations

import importlib


def _reload_config():
    """Reload the config module so class-body env reads pick up monkeypatched
    values. ``Config`` dataclass defaults are evaluated once at class
    definition time; changing env after import has no effect without a reload.
    """
    from tracebloc_ingestor import config as cfg_module
    return importlib.reload(cfg_module)


def test_api_endpoint_uses_requests_proxy_url_when_set(monkeypatch):
    """``REQUESTS_PROXY_URL`` wins over the ``CLIENT_ENV → API_ENDPOINTS`` map."""
    monkeypatch.setenv("REQUESTS_PROXY_URL", "http://requests-proxy-service:8888")
    monkeypatch.setenv("CLIENT_ENV", "prod")  # would otherwise pick api.tracebloc.io

    cfg_module = _reload_config()
    assert cfg_module.Config().API_ENDPOINT == "http://requests-proxy-service:8888"


def test_api_endpoint_falls_back_to_client_env_when_proxy_unset(monkeypatch):
    """Existing ``CLIENT_ENV`` behavior is preserved when ``REQUESTS_PROXY_URL`` is unset."""
    monkeypatch.delenv("REQUESTS_PROXY_URL", raising=False)
    monkeypatch.setenv("CLIENT_ENV", "stg")

    cfg_module = _reload_config()
    assert cfg_module.Config().API_ENDPOINT == "https://stg-api.tracebloc.io"


def test_api_endpoint_treats_empty_proxy_url_as_unset(monkeypatch):
    """An empty ``REQUESTS_PROXY_URL`` falls back rather than producing an empty endpoint.

    Guards against the Helm chart accidentally injecting ``REQUESTS_PROXY_URL=""``
    (e.g. from a defaulted value template) and silently breaking outbound calls.
    """
    monkeypatch.setenv("REQUESTS_PROXY_URL", "")
    monkeypatch.setenv("CLIENT_ENV", "prod")

    cfg_module = _reload_config()
    assert cfg_module.Config().API_ENDPOINT == "https://api.tracebloc.io"
