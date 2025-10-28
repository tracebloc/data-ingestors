import logging
import sys
from typing import Optional
from ..config import Config


def setup_logging(
    config: Optional[Config] = None,
) -> None:
    """
    Configure logging with a unified format across the application

    Args:
        config: Optional Config instance for custom logging settings
    """
    # Use config settings if provided, otherwise use defaults
    if config:
        level = config.LOG_LEVEL
        log_format = None
        date_format = None
    else:
        level = logging.INFO
        log_format = None
        date_format = None

    if not log_format:
        log_format = (
            "%(asctime)s | %(levelname)-8s | %(name)s | "
            "%(filename)s:%(lineno)d | %(message)s"
        )

    if not date_format:
        date_format = "%Y-%m-%d %H:%M:%S"

    # Create console handler with formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(fmt=log_format, datefmt=date_format))

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicate logs
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add our console handler
    root_logger.addHandler(console_handler)

    # Suppress overly verbose logs from certain libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)
    logging.getLogger("tqdm").setLevel(logging.INFO)
