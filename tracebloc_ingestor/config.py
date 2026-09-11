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
    _ENV_FIELDS = frozenset(
        {
            "DB_HOST",
            "DB_PORT",
            "DB_USER",
            "DB_PASSWORD",
            "DB_NAME",
            "BATCH_SIZE",
            "EDGE_ENV",
            "BACKEND_TOKEN",
            "CLIENT_USERNAME",
            "CLIENT_PASSWORD",
            "SRC_PATH",
            "LABEL_FILE",
            "TABLE_NAME",
            "TITLE",
            "LOG_LEVEL",
            "CATEGORICAL_MIN_COUNT",
            "EMIT_ENRICHED_SCHEMA",
            "PER_INGESTION_TABLES",
        }
    )

    # Env values (case-insensitive) that read as boolean true; anything else
    # (including unset) is false.
    _TRUE_STRINGS = frozenset({"1", "true", "yes", "on"})

    # Numeric fields whose properties unconditionally coerce via ``int(...)``.
    # ``Config(FIELD=None)`` works for nullable fields (BACKEND_TOKEN etc.)
    # but is nonsensical here — reject at construction with a clear message
    # rather than letting ``int(None)`` blow up later at property access.
    _NUMERIC_FIELDS = frozenset({"DB_PORT", "BATCH_SIZE", "CATEGORICAL_MIN_COUNT"})

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

    @staticmethod
    def _read_env_alias(
        new_name: str, old_name: str, default: Optional[str] = None
    ) -> Optional[str]:
        """Read ``new_name``, falling back to the deprecated ``old_name``.

        RFC-0076 settings-naming (data-ingestors#585): alias-first, both
        names work, ``remove_by: 2026-12-31``. ``new_name`` wins if a caller
        (wrongly) sets both — the point of the alias is to keep the old name
        working during the migration, not to make it authoritative. One
        helper so each aliased field states its old/new pair once instead of
        restating the ``os.environ`` fallback chain at every property.

        A *present-but-empty* value counts as set (``is not None``, not
        truthiness): ``TRACEBLOC_DB_NAME=""`` resolves to ``""`` and wins over
        a real legacy ``DB_NAME`` rather than falling through to it or the
        default. That restores the pre-alias ``os.environ.get`` behaviour and
        preserves an explicit blank for the caller's own guard — an optional
        field returns it as-is; a required field's ``_require_env`` still
        rejects it (backend/client-runtime settled on the same rule).
        """
        for name in (new_name, old_name):
            val = os.environ.get(name)
            if val is not None:
                return val
        return default

    @classmethod
    def _require_env(cls, required_name: str, canonical: Optional[str] = None) -> str:
        """Read a required DB-credential env var, failing fast if unset
        (backend#1528; alias per RFC-0076, remove_by 2026-12-31).

        ``required_name`` is the name the spawner (jobs-manager) injects and
        the ``runtime_env`` contract pins — it stays the *required* name until
        a coordinated phase-2 flip, so the reader can't declare the canonical
        required before the writer emits it (reader-before-writer). It is
        arg 0 so the contract test reads the injected name from the AST.
        ``canonical`` is the RFC-0076 ``TRACEBLOC_``-prefixed spelling the
        ingestor reads alias-first (data-ingestors#585): an operator who has
        moved to it wins, but either spelling satisfies the requirement.

        The edgeuser fallback is gone: jobs-manager now injects the per-Job
        tb_ingest credentials, so an unset — or explicitly blank — value means
        a misconfigured Job, not a cue to use edgeuser. A present-but-blank
        canonical is a *present* value that wins over the legacy name (see
        ``_read_env_alias``) and then fails here: it must never fall through to
        a stale legacy secret. Raise a message naming the variable(s) rather
        than letting SQLAlchemy surface an opaque access-denied (or, worse,
        silently connect as the legacy identity).
        """
        if canonical:
            val = cls._read_env_alias(canonical, required_name)
            primary, secondary = canonical, required_name
        else:
            val = os.environ.get(required_name)
            primary, secondary = required_name, None
        if not val:
            alias_hint = f" (or the deprecated {secondary})" if secondary else ""
            raise ValueError(
                f"{primary} is not set{alias_hint}. The ingestor authenticates "
                f"to MySQL as the identity jobs-manager injects per-Job "
                f"(backend#1528: tb_ingest); set the {primary} environment "
                f"variable{alias_hint}. The legacy edgeuser fallback was "
                f"removed once per-Job credentials shipped."
            )
        return val

    # ===== Database =====
    # DB_HOST/DB_PORT read the stock MySQL-image conventions (MYSQL_HOST /
    # MYSQL_PORT) and keep those names — that contract is the mysql client /
    # docker-entrypoint's, not ours, and is out of scope for the RFC-0076
    # rename below. DB_USER/DB_PASSWORD/DB_NAME ARE ours: read alias-first as
    # of data-ingestors#585 (canonical TRACEBLOC_DB_USER / TRACEBLOC_DB_PASSWORD
    # / TRACEBLOC_DB_NAME, falling back to the deprecated DB_USER / DB_PASSWORD
    # / DB_NAME; remove_by 2026-12-31). The runtime_env contract still pins the
    # *legacy* DB_USER / DB_PASSWORD as the required names the spawner injects —
    # the required-name flip to the canonical is a coordinated phase-2 step, so
    # _require_env takes the injected name first (arg 0, what the contract test
    # reads) and the canonical as the alias it reads ahead of it.
    # DB_USER/DB_PASSWORD do NOT default:
    # the ingestor authenticates as the identity jobs-manager injects per-Job
    # (backend#1528 — tb_ingest, scoped to training_test_datasets). The legacy
    # root-equivalent edgeuser fallback was removed here once those per-Job
    # credentials shipped; both are required from env (either spelling) and
    # fail fast if unset rather than silently connecting as the old identity.
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
        return (
            ov
            if ov is not _MISSING
            else self._require_env("DB_USER", "TRACEBLOC_DB_USER")
        )

    @property
    def DB_PASSWORD(self) -> str:
        ov = self._override("DB_PASSWORD")
        return (
            ov
            if ov is not _MISSING
            else self._require_env("DB_PASSWORD", "TRACEBLOC_DB_PASSWORD")
        )

    @property
    def DB_NAME(self) -> str:
        ov = self._override("DB_NAME")
        return (
            ov
            if ov is not _MISSING
            else self._read_env_alias(
                "TRACEBLOC_DB_NAME", "DB_NAME", "training_test_datasets"
            )
        )

    @property
    def BATCH_SIZE(self) -> int:
        ov = self._override("BATCH_SIZE")
        raw = ov if ov is not _MISSING else os.environ.get("BATCH_SIZE", "4000")
        return self._as_int("BATCH_SIZE", "BATCH_SIZE", raw)

    @property
    def CATEGORICAL_MIN_COUNT(self) -> int:
        """Minimum occurrence count for a categorical value to be emitted in the
        alignment vocab (data-ingestors#360). Values seen fewer than this many
        times are suppressed — a re-identification guard for rare categories
        (a value present for a single record).

        Defaults to **1** (keep every observed value — no suppression), because
        raising it drops rare values from the union vocab, which the edge must
        then handle as out-of-vocabulary at encode time; that trade needs the
        backend privacy sign-off + engine OOV handling (backend#1079/#1053). Set
        it above 1 in the deployment once those are in place.
        """
        ov = self._override("CATEGORICAL_MIN_COUNT")
        raw = ov if ov is not _MISSING else os.environ.get("CATEGORICAL_MIN_COUNT", "1")
        return self._as_int("CATEGORICAL_MIN_COUNT", "CATEGORICAL_MIN_COUNT", raw)

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
    # (cli/run.py:main) sets these as explicit Config(...) overrides from the
    # resolved ingest.yaml, bypassing os.environ entirely (P4c) — the alias
    # below only matters for the legacy env-var path (ingestor-job.yaml /
    # templates/examples reading a module-level ``Config()``). Empty string
    # fails loudly in path operations rather than silently scanning a
    # developer-laptop directory.
    #
    # SRC_PATH/LABEL_FILE are ours (data-ingestors#585): read alias-first as
    # TRACEBLOC_SRC_PATH / TRACEBLOC_LABEL_FILE, falling back to the
    # deprecated SRC_PATH / LABEL_FILE; remove_by 2026-12-31. TABLE_NAME is
    # deliberately NOT renamed here — it's the cross-repo ingest wire
    # contract the launching backend job sets, and its eventual name
    # (dataset_key / physical_table / data_vertical) is a B4 decision, not
    # this ticket's.
    @property
    def SRC_PATH(self) -> str:
        ov = self._override("SRC_PATH")
        return (
            ov
            if ov is not _MISSING
            else self._read_env_alias("TRACEBLOC_SRC_PATH", "SRC_PATH", "")
        )

    @property
    def LABEL_FILE(self) -> str:
        ov = self._override("LABEL_FILE")
        return (
            ov
            if ov is not _MISSING
            else self._read_env_alias("TRACEBLOC_LABEL_FILE", "LABEL_FILE", "")
        )

    @property
    def TABLE_NAME(self) -> str:
        ov = self._override("TABLE_NAME")
        return ov if ov is not _MISSING else os.environ.get("TABLE_NAME", "")

    @property
    def DEST_TABLE(self) -> str:
        """The directory the file tree lands in under STORAGE_PATH. Defaults to
        the dataset label (``TABLE_NAME``); when PER_INGESTION_TABLES is on the
        ingestor injects the physical ``ds_<hex>`` handle via ``set_dest_table``
        so file-bearing assets key on the SAME isolation unit as the row store,
        the SQL grant, and the scoped mount (RFC-0003 D9 phase 2 / D16 —
        client-runtime#203, tracebloc-engine#569). Not a user env field: the
        only writer is ``set_dest_table``."""
        ov = self._override("DEST_TABLE")
        return ov if ov is not _MISSING else self.TABLE_NAME

    @property
    def DEST_PATH(self) -> str:
        return os.path.join(self.STORAGE_PATH, self.DEST_TABLE)

    def set_dest_table(self, physical_table_name: str) -> None:
        """Point the file tree (``DEST_PATH``) at an explicit physical-table
        directory. The ingestor calls this once, after it derives the
        per-ingestion ``ds_<hex>`` handle, so file-bearing assets land under the
        same handle as the row store instead of the shared label tree (#203
        phase 2). Flag-off ingests never call it, so ``DEST_TABLE`` stays the
        label and behavior is byte-for-byte today's."""
        self._overrides["DEST_TABLE"] = physical_table_name

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

    @property
    def PER_INGESTION_TABLES(self) -> bool:
        """RFC-0003 D16/D19 (tracebloc/backend#1205): when true, every ingest
        run creates its own immutable physical table ``ds_<uuid4().hex>``
        (derived from ingestor_id; hex because SQL table grammars forbid
        hyphens) instead of appending to the label-named shared table, and
        reports the handle in the ingest summary (``physical_table``) so the
        backend can persist it (tracebloc/backend#1206).

        Defaults to **off** — today's shared-table behavior, byte for byte.
        Flipping it is coordinated with the backend resolution path and the
        training read path (tracebloc/backend#1208); an edge flipped early
        would ingest datasets the current training path cannot locate.
        """
        ov = self._override("PER_INGESTION_TABLES")
        if ov is not _MISSING:
            return (
                ov
                if isinstance(ov, bool)
                else str(ov).strip().lower() in self._TRUE_STRINGS
            )
        env = os.environ.get("PER_INGESTION_TABLES")
        if env is None:
            return False
        return env.strip().lower() in self._TRUE_STRINGS

    @property
    def EMIT_ENRICHED_SCHEMA(self) -> bool:
        """Whether to emit the per-column *enriched* ``schema`` on the global-
        metadata channel — ``{col: {dtype, role, …}}`` — instead of the legacy
        flat ``{col: SQL_type}`` map (data-ingestors#360 slice 1b).

        Defaults to **on**: the paired backend (backend#1037 — its
        ``_canonical_schema`` preserves the enriched shape and ``_column_dtype``
        reads both) and edge (tracebloc-engine#460 — reads ``role`` from the
        schema to identify the prediction target) now depend on it. Set the
        ``EMIT_ENRICHED_SCHEMA`` env var / override to a falsey value ("0",
        "false", …) to force the legacy flat map back for a deployment whose
        backend predates the cutover. ``feature_stats`` is unaffected (additive).
        """
        ov = self._override("EMIT_ENRICHED_SCHEMA")
        if ov is not _MISSING:
            return (
                ov
                if isinstance(ov, bool)
                else str(ov).strip().lower() in self._TRUE_STRINGS
            )
        env = os.environ.get("EMIT_ENRICHED_SCHEMA")
        if env is not None and env.strip() != "":
            return env.strip().lower() in self._TRUE_STRINGS
        return True

    def validate(self) -> None:
        """Fail fast on missing backend authentication.

        Called explicitly by ``APIClient.__init__`` (the boot moment for a
        real run) rather than from ``__init__`` so that incidental
        module-level ``Config()`` instantiations elsewhere in the package
        don't blow up at import time.

        In any non-local environment, the pod must boot with:
          - ``BACKEND_TOKEN`` (preferred) or ``CLIENT_ID`` +
            ``CLIENT_PASSWORD`` (deprecated fallback) for backend auth, and
          - ``DB_USER`` + ``DB_PASSWORD`` for MySQL.

        Database credentials are required as of backend#1528 (D10): the
        root-equivalent ``edgeuser`` fallback was removed, so the ingestor
        authenticates as the identity jobs-manager injects per-Job
        (``tb_ingest``). They are NOT re-checked here — this method is
        auth-scoped — but ``Config.DB_USER`` / ``DB_PASSWORD`` fail fast on
        first access (``Database()`` init) via ``_require_env`` with a message
        naming the unset variable, so a misconfigured Job still surfaces early.

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
