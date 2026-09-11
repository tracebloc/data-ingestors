"""Lock in ``Config``'s lazy-property contract.

The bug this protects against (see #97): validators import
``config = Config()`` at module top-level. When ``Config`` was a
``@dataclass`` with ``os.getenv`` defaults, those fields were frozen at
class-definition time, long before the declarative entrypoint set
``SRC_PATH`` / ``TABLE_NAME`` / ``LABEL_FILE`` from the resolved YAML.
Customers hit ``Path does not exist`` against a laptop-default path
nobody had set.

These tests verify:
  - env set before ``Config()`` flows through (baseline).
  - env mutated *after* ``Config()`` flows through (the regression).
  - validators that captured ``config = Config()`` at import time observe
    later env mutations (the cluster bug).
  - ``Config(SRC_PATH=...)`` overrides env (test ergonomics).
  - Laptop-path defaults are gone — unset env returns empty string / None.
"""

from __future__ import annotations

import pytest

from tracebloc_ingestor.config import Config


@pytest.fixture
def clean_env(monkeypatch):
    """Strip the env vars these tests read/write."""
    for var in (
        "SRC_PATH",
        "TRACEBLOC_SRC_PATH",
        "LABEL_FILE",
        "TRACEBLOC_LABEL_FILE",
        "TABLE_NAME",
        "TITLE",
        "BACKEND_TOKEN",
        "CLIENT_ID",
        "CLIENT_PASSWORD",
        "CLIENT_ENV",
        "LOG_LEVEL",
        "BATCH_SIZE",
        "MYSQL_HOST",
        "MYSQL_PORT",
        "DB_USER",
        "TRACEBLOC_DB_USER",
        "DB_PASSWORD",
        "TRACEBLOC_DB_PASSWORD",
        "DB_NAME",
        "TRACEBLOC_DB_NAME",
    ):
        monkeypatch.delenv(var, raising=False)


def test_env_set_before_construction_flows_through(clean_env, monkeypatch):
    monkeypatch.setenv("SRC_PATH", "/data/cats-dogs/images")
    monkeypatch.setenv("TABLE_NAME", "cats_dogs_train")
    monkeypatch.setenv("LABEL_FILE", "/data/cats-dogs/labels.csv")

    config = Config()

    assert config.SRC_PATH == "/data/cats-dogs/images"
    assert config.TABLE_NAME == "cats_dogs_train"
    assert config.LABEL_FILE == "/data/cats-dogs/labels.csv"
    assert config.DEST_PATH == "/data/shared/cats_dogs_train"


def test_env_mutation_after_construction_flows_through(clean_env, monkeypatch):
    """The exact bug from the cluster: a validator imports ``config = Config()``,
    the entrypoint sets ``SRC_PATH`` later, and the validator must see the
    new value."""
    config = Config()
    assert config.SRC_PATH == ""  # no env, no laptop default

    monkeypatch.setenv("SRC_PATH", "/data/customer/images")
    assert config.SRC_PATH == "/data/customer/images"

    monkeypatch.setenv("SRC_PATH", "/data/customer/v2/images")
    assert config.SRC_PATH == "/data/customer/v2/images"


def test_module_level_validator_config_picks_up_env_change(clean_env, monkeypatch):
    """Validators capture ``config = Config()`` at module import. After the
    entrypoint sets env, ``file_validator.config.SRC_PATH`` must reflect
    the new value — that's the cluster bug repro."""
    from tracebloc_ingestor.validators import file_validator

    monkeypatch.setenv("SRC_PATH", "/data/shared/sample-image-classification")
    assert file_validator.config.SRC_PATH == "/data/shared/sample-image-classification"

    monkeypatch.setenv("SRC_PATH", "/data/shared/other-customer")
    assert file_validator.config.SRC_PATH == "/data/shared/other-customer"


def test_instance_override_wins_over_env(clean_env, monkeypatch):
    monkeypatch.setenv("BACKEND_TOKEN", "from-env")
    monkeypatch.setenv("SRC_PATH", "/from/env")

    config = Config(BACKEND_TOKEN="from-override", SRC_PATH="/from/override")

    assert config.BACKEND_TOKEN == "from-override"
    assert config.SRC_PATH == "/from/override"


def test_explicit_none_override_suppresses_env(clean_env, monkeypatch):
    """Tests pass ``BACKEND_TOKEN=None`` to *disable* the token path even
    when env has one set. Sentinel-based override-lookup distinguishes
    'absent' from 'explicit None'."""
    monkeypatch.setenv("BACKEND_TOKEN", "leaked-from-env")

    config = Config(BACKEND_TOKEN=None)
    assert config.BACKEND_TOKEN is None


def test_unknown_override_kwarg_raises(clean_env):
    with pytest.raises(TypeError) as exc:
        Config(NOT_A_REAL_FIELD="x")
    assert "NOT_A_REAL_FIELD" in str(exc.value)


@pytest.mark.parametrize("field", ["DB_PORT", "BATCH_SIZE", "CATEGORICAL_MIN_COUNT"])
def test_numeric_override_rejects_none_at_construction(clean_env, field):
    """``None`` is a valid suppression for nullable fields (BACKEND_TOKEN
    etc.) but is nonsensical for numeric ones whose properties do
    ``int(...)``. The constructor must reject this with a helpful message
    rather than deferring to a ``TypeError: int() argument must be a
    string ...`` on first property access."""
    with pytest.raises(TypeError) as exc:
        Config(**{field: None})
    msg = str(exc.value)
    assert field in msg
    assert "None" in msg


@pytest.mark.parametrize(
    "field,value",
    [("DB_PORT", 3307), ("BATCH_SIZE", "8000"), ("CATEGORICAL_MIN_COUNT", "5")],
)
def test_numeric_override_accepts_ints_and_str_ints(clean_env, field, value):
    config = Config(**{field: value})
    assert getattr(config, field) == int(value)


def test_categorical_min_count_defaults_to_one(clean_env, monkeypatch):
    """Default is 1 — no rare-category suppression until a deployment opts in
    (the privacy/OOV trade needs backend sign-off)."""
    monkeypatch.delenv("CATEGORICAL_MIN_COUNT", raising=False)
    assert Config().CATEGORICAL_MIN_COUNT == 1


def test_laptop_path_defaults_are_gone(clean_env):
    """The pre-refactor defaults pointed at ``~/Downloads/data-ingestors/...``,
    which silently scanned a developer laptop dir on customer clusters."""
    config = Config()
    assert config.SRC_PATH == ""
    assert config.LABEL_FILE == ""
    assert config.TABLE_NAME == ""
    assert config.TITLE is None

    # And no stray "Downloads" anywhere — be paranoid about regressions
    # where someone reintroduces a laptop path.
    assert "Downloads" not in (config.SRC_PATH or "")
    assert "Downloads" not in (config.LABEL_FILE or "")


def test_dest_path_follows_table_name(clean_env, monkeypatch):
    monkeypatch.setenv("TABLE_NAME", "first_table")
    config = Config()
    assert config.DEST_PATH == "/data/shared/first_table"

    monkeypatch.setenv("TABLE_NAME", "second_table")
    assert config.DEST_PATH == "/data/shared/second_table"


def test_api_endpoint_follows_edge_env(clean_env, monkeypatch):
    config = Config()
    assert config.EDGE_ENV == "prod"
    assert config.API_ENDPOINT == "https://api.tracebloc.io"

    monkeypatch.setenv("CLIENT_ENV", "stg")
    assert config.EDGE_ENV == "stg"
    assert config.API_ENDPOINT == "https://stg-api.tracebloc.io"

    monkeypatch.setenv("CLIENT_ENV", "local")
    assert config.API_ENDPOINT == "http://localhost:8000"


@pytest.mark.parametrize(
    "env,attr", [("MYSQL_PORT", "DB_PORT"), ("BATCH_SIZE", "BATCH_SIZE")]
)
def test_non_numeric_int_field_raises_clear_error(clean_env, monkeypatch, env, attr):
    # A non-numeric MYSQL_PORT / BATCH_SIZE must surface a clear config error
    # naming the field, not a raw "invalid literal for int()" (#238).
    monkeypatch.setenv(env, "abc")
    with pytest.raises(ValueError, match="must be an integer"):
        getattr(Config(), attr)


def test_numeric_int_field_still_coerces(clean_env, monkeypatch):
    monkeypatch.setenv("MYSQL_PORT", "3307")
    monkeypatch.setenv("BATCH_SIZE", "500")
    config = Config()
    assert config.DB_PORT == 3307
    assert config.BATCH_SIZE == 500


@pytest.mark.parametrize(
    "attr,new_env",
    [("DB_USER", "TRACEBLOC_DB_USER"), ("DB_PASSWORD", "TRACEBLOC_DB_PASSWORD")],
)
def test_db_credentials_required_no_edgeuser_fallback(clean_env, attr, new_env):
    """backend#1528: the root-equivalent 'edgeuser' fallback is gone. With
    neither the new (TRACEBLOC_-prefixed) nor deprecated env var set,
    accessing DB_USER/DB_PASSWORD must fail fast with a message naming the
    canonical variable — never silently return the legacy edgeuser default."""
    with pytest.raises(ValueError, match=f"{new_env} is not set"):
        getattr(Config(), attr)


@pytest.mark.parametrize(
    "attr,env,value",
    [
        ("DB_USER", "DB_USER", "tb_ingest"),
        ("DB_USER", "TRACEBLOC_DB_USER", "tb_ingest"),
        ("DB_PASSWORD", "DB_PASSWORD", "s3cret"),
        ("DB_PASSWORD", "TRACEBLOC_DB_PASSWORD", "s3cret"),
    ],
)
def test_db_credentials_flow_from_env(clean_env, monkeypatch, attr, env, value):
    """data-ingestors#585: DB_USER/DB_PASSWORD are read alias-first — either
    the deprecated bare name or the new TRACEBLOC_-prefixed one works."""
    monkeypatch.setenv(env, value)
    assert getattr(Config(), attr) == value


@pytest.mark.parametrize(
    "attr,new_env,old_env",
    [
        ("DB_USER", "TRACEBLOC_DB_USER", "DB_USER"),
        ("DB_PASSWORD", "TRACEBLOC_DB_PASSWORD", "DB_PASSWORD"),
    ],
)
def test_db_credentials_new_name_wins_when_both_set(
    clean_env, monkeypatch, attr, new_env, old_env
):
    """Alias-first means the new name is authoritative — the deprecated one
    is a fallback for callers who haven't migrated yet, not a second vote."""
    monkeypatch.setenv(new_env, "from-new")
    monkeypatch.setenv(old_env, "from-old")
    assert getattr(Config(), attr) == "from-new"


@pytest.mark.parametrize(
    "attr,new_env,old_env",
    [
        ("DB_USER", "TRACEBLOC_DB_USER", "DB_USER"),
        ("DB_PASSWORD", "TRACEBLOC_DB_PASSWORD", "DB_PASSWORD"),
    ],
)
def test_db_credentials_blank_new_name_wins_and_fails_fast(
    clean_env, monkeypatch, attr, new_env, old_env
):
    """A present-but-blank canonical is a *present* value: it wins over a real
    legacy secret (alias-first, is-not-None), and because the credential is
    REQUIRED the blank then fails fast — it must never silently authenticate
    with the stale legacy value. This is the point of preserving present-but-
    blank at the reader (backend/client-runtime settled on the same rule)."""
    monkeypatch.setenv(new_env, "")
    monkeypatch.setenv(old_env, "stale-legacy-secret")
    with pytest.raises(ValueError, match=f"{new_env} is not set"):
        getattr(Config(), attr)


def test_db_credentials_override_wins(clean_env):
    config = Config(DB_USER="tb_ingest", DB_PASSWORD="s3cret")
    assert config.DB_USER == "tb_ingest"
    assert config.DB_PASSWORD == "s3cret"


# Optional aliased fields (data-ingestors#585): read alias-first, both
# spellings work, a present-but-empty value is returned as-is, and an unset
# field falls to its default. Same parametrized shape as the DB-cred group
# above, minus the fail-fast — these have defaults. DB_NAME / SRC_PATH /
# LABEL_FILE are all ours (set by ingestor-job.yaml / the legacy templates
# path, not a third-party convention).
_OPTIONAL_ALIAS_FIELDS = [
    # attr, new_env, old_env, default
    ("DB_NAME", "TRACEBLOC_DB_NAME", "DB_NAME", "training_test_datasets"),
    ("SRC_PATH", "TRACEBLOC_SRC_PATH", "SRC_PATH", ""),
    ("LABEL_FILE", "TRACEBLOC_LABEL_FILE", "LABEL_FILE", ""),
]


@pytest.mark.parametrize("attr,new_env,old_env,default", _OPTIONAL_ALIAS_FIELDS)
def test_optional_alias_reads_new_name(
    clean_env, monkeypatch, attr, new_env, old_env, default
):
    monkeypatch.setenv(new_env, "from-new")
    assert getattr(Config(), attr) == "from-new"


@pytest.mark.parametrize("attr,new_env,old_env,default", _OPTIONAL_ALIAS_FIELDS)
def test_optional_alias_falls_back_to_deprecated_name(
    clean_env, monkeypatch, attr, new_env, old_env, default
):
    monkeypatch.setenv(old_env, "from-old")
    assert getattr(Config(), attr) == "from-old"


@pytest.mark.parametrize("attr,new_env,old_env,default", _OPTIONAL_ALIAS_FIELDS)
def test_optional_alias_new_name_wins_when_both_set(
    clean_env, monkeypatch, attr, new_env, old_env, default
):
    monkeypatch.setenv(new_env, "from-new")
    monkeypatch.setenv(old_env, "from-old")
    assert getattr(Config(), attr) == "from-new"


@pytest.mark.parametrize("attr,new_env,old_env,default", _OPTIONAL_ALIAS_FIELDS)
def test_optional_alias_blank_new_name_wins_over_real_old(
    clean_env, monkeypatch, attr, new_env, old_env, default
):
    """A present-but-empty canonical wins over a real legacy value and is
    returned as-is — restores the pre-alias os.environ.get behaviour for these
    optional fields (the caller, not the reader, decides whether blank is a
    problem)."""
    monkeypatch.setenv(new_env, "")
    monkeypatch.setenv(old_env, "real-legacy-value")
    assert getattr(Config(), attr) == ""


@pytest.mark.parametrize("attr,new_env,old_env,default", _OPTIONAL_ALIAS_FIELDS)
def test_optional_alias_default_when_unset(clean_env, attr, new_env, old_env, default):
    assert getattr(Config(), attr) == default
