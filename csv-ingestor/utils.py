import os

DS_BACKEND = "https://dev-api.tracebloc.io/"
DEV_BACKEND = "https://dev-api.tracebloc.io/"
STG_BACKEND = "https://stg-api.tracebloc.io/"
LOCAL_BACKEND = "http://127.0.0.1:8000/"
# prduction url
PROD_BACKEND = "https://api.tracebloc.io/"

backend_urls = {
    "dev": DEV_BACKEND,
    "stg": STG_BACKEND,
    "test": DS_BACKEND,
    "local": LOCAL_BACKEND,
    "prod": PROD_BACKEND,
}


def get_config(default_config):
    """
    Retrieves configuration values from environment variables or uses defaults.

    Args:
        default_config: A dictionary containing default configuration values.
    Returns:
        A dictionary with configuration values.
    """
    config = dict(default_config)
    for key, value in config.items():
        env_var = key.upper()
        if env_var in os.environ:
            config[key] = os.environ[env_var]
    return config
