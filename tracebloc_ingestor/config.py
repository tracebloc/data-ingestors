"""Lazy, env-driven configuration.

Every env-driven field is a ``@property`` that reads ``os.environ`` on
access. Module-level ``config = Config()`` snapshots in validators and
ingestors stay valid even when env vars are set after those modules
import — e.g. the declarative entrypoint (``cli/run.py:main``) resolves
``ingest.yaml`` into env vars *after* the validator modules have already
imported.

Tests inject pinpoint values via ``Config(FIELD=value)``; instance
overrides win over env. Production callers pass no kwargs.
"""

from typing import Any, Dict, Optional
import os

from .utils.constants import LogLevel


# Sentinel signalling \"caller did not pass this field as an override\".
# Distinguishes the absent case from an explicit ``Field=None``, which
# tests use to *suppress* a value (e.g. ``BACKEND_TOKEN=None``).
_MISSING = object()


class Config:
    # ===== Cluster-safe class-level constants (not env-driven) =====
    API_ENDPOINTS: Dict[str, str] = {
        "dev": "https://dev-api.tracebloc.io",
        "stg": "https://stg-api.tracebloc.io",
        "prod": "https://api.tracebloc.io",
        "local": "http://localhost:8000",
    }
    STORAGE_PATH: str = "/data/shared"

    # Whitelist of valid override keys. A typo at the call site raises
    # immediately rather than silently no-op'ing.
    _ENV_FIELDS = frozenset({
        "DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME",
        "BATCH_SIZE",
        "EDGE_ENV",
        "BACKEND_TOKEN", "CLIENT_USERNAME", "CLIENT_PASSWORD",
        "SRC_PATH", "LABEL_FILE", "TABLE_NAME", "TITLE",
        "LOG_LEVEL",
    })

    # Numeric fields whose properties unconditionally coerce via ``int(...)``.
    # ``Config(FIELD=None)`` works for nullable fields (BACKEND_TOKEN etc.)
    # but is nonsensical here — reject at construction with a clear message
    # rather than letting ``int(None)`` blow up later at property access.
    _NUMERIC_FIELDS = frozenset({"DB_PORT", "BATCH_SIZE"})

    def __init__(self, **overrides: Any) -> None:
        unknown = set(overrides) - self._ENV_FIELDS
        if unknown:
            raise TypeError(
                f"Config got unexpected keyword arguments: {sorted(unknown)}"
            )
        for field in self._NUMERIC_FIELDS & set(overrides):
            if overrides[field] is None:
                raise TypeError(
                    f"Config({field}=None) is invalid: {field} is numeric "
                    "and cannot be suppressed via None. Omit the kwarg to "
                    "fall back to env / default."
                )
        self._overrides: Dict[str, Any] = dict(overrides)

    def _override(self, name: str, default: Any = _MISSING) -> Any:
        """Return the per-instance override for ``name`` if one was passed,
        otherwise ``default`` (sentinel by default — callers branch on it)."""
        if name in self._overrides:
            return self._overrides[name]
        return default

    @staticmethod
    def _as_int(field: str, env_name: str, raw: Any) -> int:
        """``int(raw)`` with a clear config error (#238).

        A non-numeric ``MYSQL_PORT`` / ``BATCH_SIZE`` (e.g. a typo'd
        ``MYSQL_PORT=abc``) otherwise surfaced as a raw
        ``ValueError: invalid literal for int() with base 10: 'abc'`` at the
        point of property access — opaque about which setting is wrong. Name
        the field and the env var the user should fix instead.
        """
        try:
            return int(raw)
        except (TypeError, ValueError):
            raise ValueError(
                f"{field} must be an integer, got {raw!r}. Set the "
                f"{env_name} environment variable to a valid integer."
            )

    # ===== Database =====
    # Cluster-internal MySQL ships with these credentials baked into its
    # image; they're connection conventions, not secrets. Override via env
    # only if you've replaced the bundled MySQL.
    @property
    def DB_HOST(self) -> str:
        ov = self._override("DB_HOST")
        return ov if ov is not _MISSING else os.environ.get("MYSQL_HOST", "localhost")

    @property
    def DB_PORT(self) -> int:
        ov = self._override("DB_PORT")
        raw = ov if ov is not _MISSING else os.environ.get("MYSQL_PORT", "3306")
        return self._as_int("DB_PORT", "MYSQL_PORT", raw)

    @property
    def DB_USER(self) -> str:
        ov = self._override("DB_USER")
        return ov if ov is not _MISSING else os.environ.get("DB_USER", "edgeuser")

    @property
    def DB_PASSWORD(self) -> str:
        ov = self._override("DB_PASSWORD")
        return ov if ov is not _MISSING else os.environ.get("DB_PASSWORD", "Edg9@Tr@ce")

    @property
    def DB_NAME(self) -> str:
        ov = self._override("DB_NAME")
        return ov if ov is not _MISSING else os.environ.get("DB_NAME", "training_test_datasets")

    @property
    def BATCH_SIZE(self) -> int:
        ov = self._override("BATCH_SIZE")
        raw = ov if ov is not _MISSING else os.environ.get("BATCH_SIZE", "4000")
        return self._as_int("BATCH_SIZE", "BATCH_SIZE", raw)

    # ===== API =====
    @property
    def EDGE_ENV(self) -> str:
        ov = self._override("EDGE_ENV")
        return ov if ov is not _MISSING else os.environ.get("CLIENT_ENV", "prod")

    @property
    def API_ENDPOINT(self) -> str:
        return self.API_ENDPOINTS.get(self.EDGE_ENV, self.API_ENDPOINTS["dev"])

    # ===== Auth =====
    # Preferred: pre-minted token from upstream (e.g. jobs-manager) via env.
    @property
    def BACKEND_TOKEN(self) -> Optional[str]:
        ov = self._override("BACKEND_TOKEN")
        return ov if ov is not _MISSING else os.environ.get("BACKEND_TOKEN")

    # Fallback: username/password. Deprecated — kept for one minor version
    # while callers migrate to BACKEND_TOKEN, then removed in a follow-up.
    @property
    def CLIENT_USERNAME(self) -> Optional[str]:
        ov = self._override("CLIENT_USERNAME")
        return ov if ov is not _MISSING else os.environ.get("CLIENT_ID")

    @property
    def CLIENT_PASSWORD(self) -> Optional[str]:
        ov = self._override("CLIENT_PASSWORD")
        return ov if ov is not _MISSING else os.environ.get("CLIENT_PASSWORD")

    # ===== Paths =====
    # No laptop-path defaults: in production, the declarative entrypoint
    # (cli/run.py:main) sets these from the resolved ingest.yaml. Empty
    # string fails loudly in path operations rather than silently scanning
    # a developer-laptop directory.
    @property
    def SRC_PATH(self) -> str:
        ov = self._override("SRC_PATH")
        return ov if ov is not _MISSING else os.environ.get("SRC_PATH", "")

    @property
    def LABEL_FILE(self) -> str:
        ov = self._override("LABEL_FILE")
        return ov if ov is not _MISSING else os.environ.get("LABEL_FILE", "")

    @property
    def TABLE_NAME(self) -> str:
        ov = self._override("TABLE_NAME")
        return ov if ov is not _MISSING else os.environ.get("TABLE_NAME", "")

    @property
    def DEST_PATH(self) -> str:
        return os.path.join(self.STORAGE_PATH, self.TABLE_NAME)

    @property
    def TITLE(self) -> Optional[str]:
        ov = self._override("TITLE")
        return ov if ov is not _MISSING else os.environ.get("TITLE")

    # ===== Logging =====
    @property
    def LOG_LEVEL(self) -> int:
        ov = self._override("LOG_LEVEL")
        if ov is not _MISSING:
            return ov if isinstance(ov, int) else LogLevel.get_level_code(ov)
        return LogLevel.get_level_code(os.environ.get("LOG_LEVEL", "WARNING"))

    def validate(self) -> None:
        """Fail fast on missing backend authentication.

        Called explicitly by ``APIClient.__init__`` (the boot moment for a
        real run) rather than from ``__init__`` so that incidental
        module-level ``Config()`` instantiations elsewhere in the package
        don't blow up at import time.

        In any non-local environment, the pod must boot with either:
          - ``BACKEND_TOKEN`` (preferred), or
          - ``CLIENT_ID`` + ``CLIENT_PASSWORD`` (deprecated fallback).

        Database credentials are intentionally **not** validated here: the
        bundled MySQL container ships with fixed credentials that the
        ingestor defaults match. They're a connection convention, not a
        secret, and forcing customers to set them in env vars adds friction
        with no security benefit.

        Set ``CLIENT_ENV=local`` to bypass for development against a mock
        backend.

        Raises:
            ValueError: with a single, comma-joined list of missing vars,
                including a hint about ``CLIENT_ENV=local``.
        """
        if self.EDGE_ENV == "local":
            return

        missing = []

        has_token = bool(self.BACKEND_TOKEN)
        has_creds = bool(self.CLIENT_USERNAME and self.CLIENT_PASSWORD)
        if not has_token and not has_creds:
            missing.append(
                "BACKEND_TOKEN (preferred) or CLIENT_ID + CLIENT_PASSWORD "
                "(deprecated fallback)"
            )

        if missing:
            raise ValueError(
                "Missing required environment variables: "
                + ", ".join(missing)
                + ". Set CLIENT_ENV=local to bypass for development."
            )
