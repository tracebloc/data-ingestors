import os

DS_BACKEND = "https://xray-backend.azurewebsites.net/"
DEV_BACKEND = "https://xray-backend-develop.azurewebsites.net/"
STG_BACKEND = "https://xray-backend-staging.azurewebsites.net/"
LOCAL_BACKEND = "http://127.0.0.1:8000/"
# prduction url
PROD_BACKEND = "https://tracebloc.azurewebsites.net/"

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
