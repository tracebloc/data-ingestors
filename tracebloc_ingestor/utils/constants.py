"""
Constants used throughout the application.
"""

# API Constants
API_TIMEOUT = 1500

# Retry configuration for file transfers
RETRY_MAX_ATTEMPTS = 3
RETRY_WAIT_MULTIPLIER = 1.0
RETRY_WAIT_MIN = 1.0
RETRY_WAIT_MAX = 10.0


# Intent Constants
class Intent:
    """
    Enumeration of supported intents for model training and classification.
    """

    TEST = "test"
    TRAIN = "train"

    @classmethod
    def get_all_intents(cls) -> list[str]:
        """
        Returns a list of all available intent values.
        """
        return [cls.TEST, cls.TRAIN]


# Data Categories
class TaskCategory:
    """
    Enumeration of supported data categories for model training and classification.
    """

    IMAGE_CLASSIFICATION = "image_classification"
    OBJECT_DETECTION = "object_detection"
    KEYPOINT_DETECTION = "keypoint_detection"
    TEXT_CLASSIFICATION = "text_classification"
    TABULAR_CLASSIFICATION = "tabular_classification"
    TABULAR_REGRESSION = "tabular_regression"
    TIME_SERIES_FORECASTING = "time_series_forecasting"
    SEMANTIC_SEGMENTATION = "semantic_segmentation"
    INSTANCE_SEGMENTATION = "instance_segmentation"

    @classmethod
    def get_all_categories(cls) -> list[str]:
        """
        Returns a list of all available category values.

        Returns:
            list[str]: List of all category values
        """
        return [
            cls.IMAGE_CLASSIFICATION,
            cls.OBJECT_DETECTION,
            cls.KEYPOINT_DETECTION,
            cls.TEXT_CLASSIFICATION,
            cls.TABULAR_CLASSIFICATION,
            cls.TABULAR_REGRESSION,
            cls.TIME_SERIES_FORECASTING,
            cls.SEMANTIC_SEGMENTATION,
            cls.INSTANCE_SEGMENTATION,
        ]

    @classmethod
    def is_valid_category(cls, category: str) -> bool:
        """
        Check if a given category is valid.

        Args:
            category: The category string to validate

        Returns:
            bool: True if category is valid, False otherwise
        """
        return category in cls.get_all_categories()


class DataFormat:
    """
    Enumeration of supported data formats for model training and classification.
    """

    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    TEXT = "text"
    TABULAR = "tabular"

    @classmethod
    def get_all_formats(cls) -> list[str]:
        """
        Returns a list of all available format values.
        """
        return [cls.IMAGE, cls.VIDEO, cls.AUDIO, cls.TEXT, cls.TABULAR]

    @classmethod
    def is_valid_format(cls, format: str) -> bool:
        """
        Check if a given format is valid.
        """
        return format in cls.get_all_formats()


# ANSI color codes
RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
CYAN = "\033[96m"


class FileExtension:
    """
    Enumeration of supported file extensions.
    """

    JPEG = ".jpeg"
    JPG = ".jpg"
    PNG = ".png"
    XML = ".xml"
    TXT = ".txt"
    TEXT = ".text"

    @classmethod
    def get_all_extensions(cls) -> list[str]:
        """
        Returns a list of all available extension values.
        """
        return [cls.JPEG, cls.JPG, cls.PNG, cls.XML, cls.TXT, cls.TEXT]

    @classmethod
    def is_valid_extension(cls, extension: str) -> bool:
        """
        Check if a given extension is valid.
        """
        return extension in cls.get_all_extensions()


class LogLevel:
    """
    Enumeration of supported log levels.
    """

    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50

    LEVEL_CODES = {
        "DEBUG": DEBUG,
        "INFO": INFO,
        "WARNING": WARNING,
        "ERROR": ERROR,
        "CRITICAL": CRITICAL,
    }

    @classmethod
    def get_level_code(cls, level: str) -> int:
        """
        Get the level code for a given level.
        """
        return cls.LEVEL_CODES.get(level, cls.WARNING)
